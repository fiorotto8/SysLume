"""Central collection runtime for the SysLume DGX cluster (Phase 19B).

A small, long-lived orchestrator that composes the *existing* read-only
collectors with the SQLite history store.  It does **not** reimplement any
collection or database logic -- it drives:

* :class:`cluster.aggregator.ClusterAggregator`  (node telemetry +
  ``cluster-model`` controller, read the controller **once** per cycle)
* :func:`cluster.service_metrics.collect_service_metrics` (reuses the
  snapshot's authoritative controller state, never re-reads it)
* :class:`cluster.history.ClusterHistoryStore`   (one single-writer SQLite
  database)

Every 15 s (default) cycle on a **single worker thread**:

1. collect one cluster snapshot;
2. collect service metrics from that snapshot's already-read state;
3. record snapshot + service metrics atomically (one writer);
4. run a (rate-limited) retention prune;
5. publish the snapshot to a thread-safe cache.

Design guarantees:

* **One collection at a time** -- exactly one worker thread calls
  ``collect``; cycles never overlap and never accumulate (a slow cycle just
  makes the next tick start right after it finishes).
* **One SQLite writer** -- a single shared :class:`ClusterHistoryStore`.
* **Reused long-lived aggregator** -- one :class:`ClusterAggregator` is
  constructed once and reused for every cycle (its stale/offline state is
  carried across cycles exactly as in the live path).
* **Independent error handling** -- a controller failure, a service-metrics
  failure and a history write failure are isolated; none of them stops the
  loop or the live cache.  The cache keeps the last good snapshot until a
  new one succeeds.
* **Bounded shutdown** -- :meth:`stop` signals an event the sleep waits on,
  so shutdown is prompt; the worker is a daemon thread that performs no work
  after the stop event is set.
* **No new workloads** -- no model/GPU work is started; no unbounded threads
  or subprocesses (the aggregator's own bounded daemon threads and the
  controller subprocess are unchanged).

HTTP endpoints (a later phase) read :meth:`latest` / :meth:`latest_snapshot`
and therefore never trigger a fresh collection.
"""

from __future__ import annotations

import json
import threading
import time

from cluster.aggregator import ClusterAggregator
from cluster.history import ClusterHistoryStore
from cluster.service_metrics import collect_service_metrics

#: Default central collection interval (seconds).
DEFAULT_INTERVAL_SECONDS = 15.0
#: Default maintenance cadence for the history store (once per hour) so that a
#: per-cycle prune() does not checkpoint the WAL every 15 s.
DEFAULT_MAINTENANCE_SECONDS = 3600.0


def _err_str(exc):
    """Compact, log-safe description of an isolated phase failure."""
    return "%s: %s" % (type(exc).__name__, str(exc)[:200])


class ClusterRuntime:
    """Long-lived, single-threaded central collection + history runtime.

    Parameters
    ----------
    interval_seconds:
        Nominal time between cycles (default 15).  The next tick is
        ``cycle_start + interval``; if a cycle overran that tick the next
        starts immediately (no overlap, no backlog).
    store:
        An existing :class:`ClusterHistoryStore`.  If omitted, one is created
        from ``database`` (one of ``store`` / ``database`` is required, so
        the runtime never silently creates a production database).
    database, retention_days, sample_interval_seconds, maintenance_seconds:
        Used only when ``store`` is omitted (default maintenance cadence is
        once per hour).
    aggregator:
        An existing :class:`ClusterAggregator` to reuse.  If omitted, a
        default (live) one is built.
    service_metrics_fn:
        ``callable(snapshot) -> services dict`` used for the service sample.
        Defaults to :func:`cluster.service_metrics.collect_service_metrics`
        bound to the snapshot (so the authoritative mode is not re-read).
        Injectable so tests can simulate service-metrics failures.
    clock:
        Zero-argument callable returning epoch seconds (default
        :func:`time.time`).  Injectable for deterministic tests.
    sleeper:
        ``callable(seconds)`` used to wait between cycles (default the stop
        event's ``wait``, which returns early on shutdown).  Injectable so
        tests can advance a fake clock instead of blocking.
    """

    def __init__(self,
                 interval_seconds=DEFAULT_INTERVAL_SECONDS,
                 store=None,
                 database=None,
                 retention_days=30.0,
                 sample_interval_seconds=None,
                 maintenance_seconds=DEFAULT_MAINTENANCE_SECONDS,
                 aggregator=None,
                 service_metrics_fn=None,
                 clock=None,
                 sleeper=None):
        if not isinstance(interval_seconds, (int, float)) \
                or isinstance(interval_seconds, bool) \
                or interval_seconds <= 0:
            raise ValueError("interval_seconds must be a positive number")
        if store is None and database is None:
            raise ValueError(
                "either an existing store or a database path is required "
                "(the runtime never creates a production database by "
                "default)")
        self._interval = float(interval_seconds)
        self._clock = clock if clock is not None else time.time
        self._stop_event = threading.Event()
        self._sleeper = sleeper if sleeper is not None \
            else self._stop_event.wait

        if store is not None:
            self.store = store
        else:
            self.store = ClusterHistoryStore(
                database,
                retention_days=retention_days,
                sample_interval_seconds=sample_interval_seconds
                if sample_interval_seconds is not None
                else self._interval,
                clock=self._clock,
                maintenance_seconds=maintenance_seconds)

        self.aggregator = aggregator if aggregator is not None \
            else ClusterAggregator()

        if service_metrics_fn is not None:
            self._service_fn = service_metrics_fn
        else:
            def _default_service_fn(snapshot):
                return collect_service_metrics(model_state=snapshot)
            self._service_fn = _default_service_fn

        self._thread = None
        self._start_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._cycles = 0
        self._last_errors = {"collect": None, "service": None,
                             "history": None, "prune": None}
        self._latest = None  # {"snapshot":..., "published_at":..., "cycle":N}

    # -- lifecycle ----------------------------------------------------------
    @property
    def is_running(self):
        return self._thread is not None \
            and self._thread.is_alive() and not self._stop_event.is_set()

    def start(self):
        """Start the single worker thread (idempotent under concurrent calls)."""
        with self._start_lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._run, name="syslume-cluster-runtime", daemon=True)
            self._thread.start()

    def stop(self, timeout=None):
        """Signal shutdown and join the worker (bounded).

        Returns ``True`` if the worker stopped within ``timeout`` seconds
        (``None`` waits up to ``2*interval + 5`` s), else ``False``.  The
        worker is a daemon thread, so it cannot hold process exit hostage.
        """
        self._stop_event.set()
        thread = self._thread
        if thread is None:
            return True
        limit = timeout
        if limit is None:
            limit = 2 * self._interval + 5.0
        thread.join(limit)
        stopped = not thread.is_alive()
        with self._start_lock:
            if self._thread is thread:
                self._thread = None if stopped else thread
        return stopped

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *exc):
        return self.stop()

    # -- worker -------------------------------------------------------------
    def _run(self):
        """Single-cycle-at-a-time loop; exits promptly on the stop event."""
        while not self._stop_event.is_set():
            cycle_start = self._clock()
            self._run_cycle()
            if self._stop_event.is_set():
                break
            delay = (cycle_start + self._interval) - self._clock()
            if delay > 0:
                self._sleeper(delay)

    def _run_cycle(self):
        """One collection cycle with independent per-phase error handling."""
        # 1) cluster snapshot.  The aggregator never raises for a node or
        #    controller failure; the guard keeps the loop alive even if a bug
        #    or a misconfigured collector does raise.
        snapshot, collect_err = self._safe_collect()

        # 2) service metrics from the snapshot's already-read controller
        #    state (only when there is a snapshot to pass).
        services, service_err = (None, None)
        if snapshot is not None:
            services, service_err = self._safe_service(snapshot)

        # 3) atomic history write (one writer).  A history failure must never
        #    stop live monitoring: the snapshot still goes to the cache.
        history_err = None
        if snapshot is not None:
            history_err = self._safe_record(snapshot, services)

        # 4) rate-limited retention maintenance (never fatal).
        prune_err = self._safe_prune()

        # 5) publish + bookkeeping (cache only advances on a good collect).
        now = self._clock()
        with self._state_lock:
            self._cycles += 1
            self._last_errors = {"collect": collect_err,
                                 "service": service_err,
                                 "history": history_err,
                                 "prune": prune_err}
            if snapshot is not None:
                self._latest = {"snapshot": json.loads(
                    json.dumps(snapshot)),  # defensive deep copy
                    "published_at": now, "cycle": self._cycles}
        return self.status()

    # -- isolated phases ----------------------------------------------------
    def _safe_collect(self):
        try:
            return self.aggregator.collect(), None
        except Exception as exc:  # noqa: BLE001 - isolate any collector bug
            return None, _err_str(exc)

    def _safe_service(self, snapshot):
        try:
            return self._service_fn(snapshot), None
        except Exception as exc:  # noqa: BLE001
            return None, _err_str(exc)

    def _safe_record(self, snapshot, services):
        """Record this cycle; return an error string (or ``None``)."""
        try:
            if services is None:
                self.store.record_snapshot(snapshot)
            else:
                self.store.record_snapshot(snapshot, services=services)
            return None
        except Exception as exc:  # noqa: BLE001
            return _err_str(exc)

    def _safe_prune(self):
        try:
            self.store.prune(now=self._clock())
            return None
        except Exception as exc:  # noqa: BLE001
            return _err_str(exc)

    # -- read path (used by later HTTP endpoints; never collects) -----------
    def latest(self):
        """Thread-safe view of the last *successfully collected* snapshot.

        Returns ``{"snapshot", "published_at", "cycle"}`` or ``None`` before
        the first successful collection.  Never triggers a fresh collection.
        """
        with self._state_lock:
            if self._latest is None:
                return None
            return {"snapshot": self._latest["snapshot"],
                    "published_at": self._latest["published_at"],
                    "cycle": self._latest["cycle"]}

    def latest_snapshot(self):
        """Just the snapshot dict (or ``None``); never collects."""
        entry = self.latest()
        return None if entry is None else entry["snapshot"]

    def status(self):
        """Bounded, JSON-serializable runtime summary (never collects)."""
        with self._state_lock:
            cycles = self._cycles
            errors = dict(self._last_errors)
            latest = None if self._latest is None else {
                "published_at": self._latest["published_at"],
                "cycle": self._latest["cycle"],
                "timestamp": self._latest["snapshot"].get("timestamp")}
        return {
            "running": self.is_running,
            "interval_seconds": self._interval,
            "cycles": cycles,
            "last_errors": errors,
            "latest": latest,
            "history": self.store.status(),
        }
