"""Central SQLite historical storage for the SysLume DGX cluster (DGX01).

Persists the read-only cluster snapshots produced by
:mod:`cluster.aggregator` (node telemetry + cluster-model controller) and the
service-level samples produced by :mod:`cluster.service_metrics` into a single
local SQLite database so the dashboard can query ~30 days of history.

Design (Phase 19A):

* Standard library only (``sqlite3``).  No external services, no packages.
* One writer: the store owns a single writer connection; all queries open
  short-lived read-only connections.  WAL mode with ``synchronous=NORMAL``
  makes interrupted writes restart-safe (the WAL is replayed on reopen).
* Separate tables for the three kinds of history:
    ``cluster_samples``   - authoritative cluster mode / health per poll
    ``node_samples``      - per-node freshness markers + validated metrics
    ``service_samples``   - vLLM and API-gateway counters/health/latency
* ``cluster_samples.mode`` is stored **verbatim** as a non-empty string.  The
  Phase 15 controller *observes* one of ``split``, ``heavy-qwen``,
  ``heavy-deepseek``, ``stop``, ``degraded``, ``unreachable`` or
  ``transitioning`` (see :data:`KNOWN_CLUSTER_MODES`); when the controller is
  unavailable the aggregator reports the mode as ``"unknown"`` and the
  healthy/transitioning booleans are stored as NULL -- never guessed.  No
  mode name is invented and none is rejected.
* **Stale-data policy:** a node row is written for *every* central poll, but
  numeric metrics are recorded only for a **fresh** sample
  (``state == "online"``).  Stale, error and never-seen nodes get an explicit
  marker row with NULL metrics and the retained sample's own timestamp in
  ``sample_ts`` -- a cached stale sample is never re-recorded as a new
  measurement, and a gap in the numbers is distinguishable from a real 0.
* The row's ``ts`` is the **central collection timestamp** (history alignment);
  the node's own sample time is preserved separately in ``sample_ts``.
* Retention / bounded growth: rows older than ``retention_days`` (default
  30) are removed by :meth:`ClusterHistoryStore.prune`, which the central
  runtime calls each cycle but which does real work (range deletes + a
  PASSIVE WAL checkpoint) at most once per ``maintenance_seconds`` window
  (default 0 = every call; the runtime uses ~3600 s) so a 15 s cadence does
  not checkpoint every 15 s.  Deletes reuse freelist pages (no VACUUM), so
  the file stays bounded.  Measured with representative data at a 15 s
  interval and 30-day retention (172,800 cycles = 864,000 rows): the steady-
  state database is ~1.8 KB/cycle, ~10.6 MB/day, ~320 MB total -- well under
  any 10 GB budget.
* Query API: bounded, bucketed (min/mean/max + counts) downsampling with
  missing-bucket gaps filled as NULLs; metric names come from an explicit
  allowlist, so user input can never become SQL.
* Nothing sensitive is stored: no credentials, no Telegram identifiers, no
  prompts/responses, no environment variables, no command lines, no HTTP
  headers, no per-model raw counter maps or process inventories (those remain
  in the live aggregator).
"""

from __future__ import annotations

import bisect as _bisect
import importlib.util
import json
import math
import os
import sqlite3
import threading
import time
from contextlib import closing
from pathlib import Path

from collectors import system_metrics as SM

__all__ = [
    "ClusterHistoryStore",
    "HistoryError",
    "SCHEMA_VERSION",
    "NODE_IDS",
    "NODE_METRICS",
    "NODE_METRIC_UNITS",
    "SERVICE_METRICS",
    "SERVICE_METRIC_UNITS",
]


class HistoryError(ValueError):
    """Raised when a snapshot/query argument is invalid for history storage."""


#: Version of *this* database schema (PRAGMA user_version on the db file).
SCHEMA_VERSION = 1

#: Canonical cluster node identities (must match the node collectors).
NODE_IDS = ("dgx01", "dgx02", "dgx03")

#: The cluster-mode vocabulary the Phase 15 ``cluster-model status --json``
#: controller can *observe* (derived in ``lib/probe.sh`` / ``lib/status.sh``),
#: plus the aggregator's sentinel for "controller unavailable".  These are
#: documentation/discovery only -- :class:`ClusterHistoryStore` stores the
#: ``mode`` field **verbatim** (any non-empty string) and never validates it
#: against this set, so a new controller mode is recorded without a schema
#: change.  ``"unknown"`` is emitted by :mod:`cluster.aggregator` when the
#: controller cannot be read; the healthy/transitioning booleans are then
#: stored as NULL (unknown), never guessed.  There is no ``cooperative`` mode.
KNOWN_CLUSTER_MODES = (
    "split",
    "heavy-qwen",
    "heavy-deepseek",
    "stop",
    "degraded",
    "unreachable",
    "transitioning",
    "unknown",
)

#: Validated node-snapshot metric names (column allowlist).  The first block
#: is the upstream SysLume set + GB10 unified memory carried by
#: ``collectors.system_metrics.METRIC_KEYS``; the remainder are the exact
#: flattened keys the node collector emits for the ollama and processes
#: components (see collectors.node_collector._FALLBACK_METRIC_KEYS).
NODE_METRICS = tuple(SM.METRIC_KEYS) + (
    "ollama_up", "ollama_models_installed", "ollama_models_resident",
    "ollama_resident_bytes", "ollama_context_tokens_max",
    "ollama_daemon_count", "ollama_runner_count", "ray_process_count",
    "vllm_process_count", "vllm_engine_core_count", "vllm_rank_worker_count",
    "docker_container_count", "tmux_session_count",
)

#: Units for :data:`NODE_METRICS` (upstream SI-base convention, plus counts).
NODE_METRIC_UNITS = dict(SM.MON.METRICS)
NODE_METRIC_UNITS.update({
    "mem_unified_used_bytes": "bytes",
    "mem_unified_total_bytes": "bytes",
    "ollama_up": "0/1",
    "ollama_models_installed": "count",
    "ollama_models_resident": "count",
    "ollama_resident_bytes": "bytes",
    "ollama_context_tokens_max": "tokens",
    "ollama_daemon_count": "count",
    "ollama_runner_count": "count",
    "ray_process_count": "count",
    "vllm_process_count": "count",
    "vllm_engine_core_count": "count",
    "vllm_rank_worker_count": "count",
    "docker_container_count": "count",
    "tmux_session_count": "count",
})

#: Validated service-level metric names mapped to :class:`ClusterHistoryStore`
#: columns.  vLLM counter names are the exact keys emitted by
#: ``collectors/heavy_metrics.VllmReader`` (see cluster.service_metrics);
#: gateway names are the chartable scalars from the gateway's metadata-only
#: counters (per-model raw counter maps stay in the live aggregator only).
SERVICE_METRICS = (
    # vLLM (DGX02 heavy API)
    "vllm_available", "vllm_stale", "vllm_up", "vllm_healthy",
    "vllm_running_reqs", "vllm_waiting_reqs", "vllm_kv_cache_usage",
    "vllm_prompt_tokens_total", "vllm_generation_tokens_total",
    # API gateway (DGX01 inference endpoint)
    "gw_available", "gw_stale", "gw_requests_total", "gw_errors_total",
    "gw_in_flight", "gw_since_start_seconds",
    "gw_latency_p50_ms", "gw_latency_p95_ms", "gw_latency_max_ms",
)

SERVICE_METRIC_UNITS = {
    "vllm_available": "0/1", "vllm_stale": "0/1", "vllm_up": "0/1",
    "vllm_healthy": "0/1", "vllm_running_reqs": "reqs",
    "vllm_waiting_reqs": "reqs", "vllm_kv_cache_usage": "frac 0-1",
    "vllm_prompt_tokens_total": "tokens",
    "vllm_generation_tokens_total": "tokens",
    "gw_available": "0/1", "gw_stale": "0/1",
    "gw_requests_total": "count", "gw_errors_total": "count",
    "gw_in_flight": "reqs", "gw_since_start_seconds": "s",
    "gw_latency_p50_ms": "ms", "gw_latency_p95_ms": "ms",
    "gw_latency_max_ms": "ms",
}

#: Numeric service metrics that are pure vLLM reader counters (columns
#: ``vllm_<key>`` are filled from the reader's own metric names).
_VLLM_COUNTERS = ("vllm_up", "vllm_running_reqs", "vllm_waiting_reqs",
                  "vllm_kv_cache_usage", "vllm_prompt_tokens_total",
                  "vllm_generation_tokens_total")


def _finite(value):
    return (isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(value))


def _number(value):
    """Finite int/float for storage; anything else (incl. bool) -> None."""
    return value if _finite(value) else None


def _bool01(value):
    """0/1 for booleans; other values (e.g. None -> unknown) -> None."""
    if value is True:
        return 1
    if value is False:
        return 0
    return None


SERVICE_MODEL_MAX_LEN = 128


def _short(text, limit=200):
    if not isinstance(text, str):
        return None
    text = " ".join(text.split())
    return text[:limit] or None


class ClusterHistoryStore:
    """Bounded SQLite history for cluster snapshots (single local writer).

    Parameters
    ----------
    database:
        Path of the SQLite file.  Parent directories are created.
        The production database lives on DGX01 only; tests use temp paths.
    retention_days:
        History retained (default 30).  ``prune`` removes older rows.
    sample_interval_seconds:
        Nominal central collection interval, used to align downsample
        buckets to natural sample boundaries (default 5 s).
    maintenance_seconds:
        Minimum wall-clock gap between actual maintenance passes
        (retention deletes + a PASSIVE WAL checkpoint).  ``0`` (default)
        keeps the original behaviour: every :meth:`prune` call performs the
        pass.  The central runtime sets this to ~3600 so that a
        once-per-cycle prune() call does real work at most once per hour
        instead of checkpointing every 15 s.  Skipped calls return
        ``{"deleted": 0, "now": now, "skipped": True}``.
    clock:
        Zero-argument callable returning the current epoch time
        (inject a fake for deterministic tests).
    """

    def __init__(self, database, retention_days=30.0,
                 sample_interval_seconds=5.0, clock=None,
                 maintenance_seconds=0.0):
        if not isinstance(database, (str, os.PathLike)) or str(database) == "":
            raise HistoryError("database path must be a non-empty path")
        if not _finite(retention_days) or retention_days <= 0:
            raise HistoryError("retention_days must be a positive number")
        if not _finite(sample_interval_seconds) or sample_interval_seconds <= 0:
            raise HistoryError("sample_interval_seconds must be positive")
        if not _finite(maintenance_seconds) or maintenance_seconds < 0:
            raise HistoryError("maintenance_seconds must be a non-negative number")
        self.path = Path(database)
        self.retention = float(retention_days) * 86400.0
        self.interval = float(sample_interval_seconds)
        self.maintenance = float(maintenance_seconds)
        self._last_maintenance = None
        self._clock = clock if clock is not None else time.time
        self._lock = threading.Lock()        # single-writer guard
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = self._connect(path=self.path, readonly=False)
        try:
            self._initialize(conn)
        except Exception:
            conn.close()
            raise
        self._writer = conn

    # -- connection / schema ------------------------------------------------
    def _connect(self, path=None, readonly=False):
        path = self.path if path is None else path
        if readonly:
            conn = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True,
                                   timeout=10)
        else:
            # The store is designed to be shared across threads (the central
            # runtime creates it on one thread and the worker thread writes to
            # it).  ``check_same_thread=False`` lets the single writer
            # connection be used from any thread; the store's ``self._lock``
            # (single-writer guard) already serializes every ``_writer`` use.
            conn = sqlite3.connect(str(path), timeout=10,
                                   check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=-4096")  # 4 MiB page cache
        conn.row_factory = sqlite3.Row
        return conn

    def _initialize(self, conn):
        """Create the schema and pin the version; never clobbers old data."""
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version not in (0, SCHEMA_VERSION):
            raise HistoryError(
                "unsupported history database version %r; refusing to "
                "overwrite existing data" % (version,))
        metric_columns = ",\n  ".join('"%s" REAL' % m for m in NODE_METRICS)
        with conn:  # one schema transaction (restart-safe)
            conn.execute(
                "CREATE TABLE IF NOT EXISTS cluster_samples (\n"
                "  ts REAL PRIMARY KEY,\n"
                "  mode TEXT,\n"
                "  healthy INTEGER,\n"
                "  transitioning INTEGER,\n"
                "  controller_ok INTEGER,\n"
                "  warning_count INTEGER\n"
                ")")
            conn.execute(
                "CREATE TABLE IF NOT EXISTS node_samples (\n"
                "  ts REAL NOT NULL,\n"
                "  node_id TEXT NOT NULL,\n"
                "  sample_ts REAL,\n"
                "  state TEXT,\n"
                "  reachable INTEGER,\n"
                "  sample_stale INTEGER,\n"
                "  quality TEXT,\n"
                "  %s,\n"
                "  PRIMARY KEY (ts, node_id)\n"
                ")" % metric_columns)
            conn.execute(
                "CREATE TABLE IF NOT EXISTS service_samples (\n"
                "  ts REAL PRIMARY KEY,\n"
                "  vllm_status TEXT,\n"
                "  vllm_served_model TEXT,\n"
                "  gw_ts REAL,\n"
                "  gw_age_seconds REAL,\n"
                "  %s\n"
                ")" % ",\n  ".join('"%s" REAL' % m for m in SERVICE_METRICS))
            conn.execute("PRAGMA user_version=%d" % SCHEMA_VERSION)

    # -- write path ---------------------------------------------------------
    def record_snapshot(self, snapshot, services=None):
        """Persist one central collection cycle atomically.

        ``snapshot`` is a :meth:`cluster.aggregator.ClusterAggregator.collect`
        result; ``services`` an optional
        :func:`cluster.service_metrics.collect_service_metrics` result.
        Everything for one ``snapshot["timestamp"]`` is written in a single
        transaction; any invalid input raises :class:`HistoryError` without
        partial writes.  Re-recording the same timestamp replaces those rows
        (idempotent re-delivery), it never duplicates them.
        Returns ``{"cluster": 1, "nodes": n, "services": 1-or-zero, "ts": ts}``.
        """
        cluster_row, ts = self._cluster_row(snapshot)
        node_rows = self._node_rows(snapshot, ts)
        service_row = self._service_row(ts, services)
        sql_cluster = ("INSERT OR REPLACE INTO cluster_samples "
                       "(ts, mode, healthy, transitioning, controller_ok, "
                       "warning_count) VALUES (?,?,?,?,?,?)")
        node_keys = ("ts", "node_id", "sample_ts", "state", "reachable",
                     "sample_stale", "quality") + NODE_METRICS
        sql_node = ("INSERT OR REPLACE INTO node_samples (%s) VALUES (%s)"
                    % (",".join('"%s"' % k for k in node_keys),
                       ",".join("?" for _ in node_keys)))
        svc_keys = ("ts", "vllm_status", "vllm_served_model", "gw_ts",
                    "gw_age_seconds") + SERVICE_METRICS
        sql_service = ("INSERT OR REPLACE INTO service_samples (%s) VALUES (%s)"
                       % (",".join('"%s"' % k for k in svc_keys),
                          ",".join("?" for _ in svc_keys)))
        with self._lock:
            with self._writer:  # single transaction; auto-rollback on error
                self._writer.execute(sql_cluster, cluster_row)
                for row in node_rows:
                    self._writer.execute(sql_node, row)
                if service_row is not None:
                    self._writer.execute(sql_service, service_row)
        return {"cluster": 1, "nodes": len(node_rows),
                "services": 1 if service_row is not None else 0, "ts": ts}

    @staticmethod
    def _require(condition, message):
        if not condition:
            raise HistoryError(message)

    def _cluster_row(self, snapshot):
        """Validate the snapshot and build the cluster_samples row."""
        self._require(isinstance(snapshot, dict),
                      "snapshot must be a dict")
        version = snapshot.get("schema_version")
        self._require(version == 1,
                      "unsupported snapshot schema_version %r" % (version,))
        ts = _number(snapshot.get("timestamp"))
        self._require(ts is not None and ts > 0,
                      "snapshot has no usable central timestamp")
        cluster = snapshot.get("cluster")
        self._require(isinstance(cluster, dict), "snapshot missing cluster block")
        mode = cluster.get("mode")
        self._require(isinstance(mode, str) and mode,
                      "cluster mode must be a non-empty string (verbatim, "
                      "never guessed)")
        nodes = snapshot.get("nodes")
        self._require(isinstance(nodes, dict) and nodes,
                      "snapshot must contain a non-empty nodes map")
        for node_id in nodes:
            self._require(node_id in NODE_IDS,
                          "unknown node_id %r in snapshot" % (node_id,))
        warnings = snapshot.get("warnings")
        warning_count = (len(warnings) if isinstance(warnings, list) else 0)
        cluster_row = (ts,
                       mode,
                       _bool01(cluster.get("healthy")),
                       _bool01(cluster.get("transitioning")),
                       _bool01(cluster.get("controller") == "ok"),
                       warning_count)
        return cluster_row, ts

    def _node_rows(self, snapshot, ts):
        """Build one node_samples row per node; stale -> NULL metrics."""
        rows = []
        for node_id in NODE_IDS:
            entry = snapshot["nodes"].get(node_id)
            if entry is None:
                continue
            self._require(isinstance(entry, dict),
                          "node entry for %r must be a dict" % (node_id,))
            state = entry.get("state")
            self._require(isinstance(state, str) and state,
                          "node %r has no valid state" % (node_id,))
            sample = entry.get("sample")
            sample_ts = None
            quality = None
            metrics = None
            if isinstance(sample, dict):
                sample_ts = _number(sample.get("timestamp"))
                quality = _short(sample.get("status"), 32)
                # Metrics are recorded ONLY for a fresh accepted sample.
                # A cached stale sample or a re-served old reading is never
                # stored as a new measurement: the row is an explicit
                # marker with NULL numbers (sample_ts keeps the last real
                # measurement time where one exists).
                if state == "online":
                    raw = sample.get("metrics")
                    if isinstance(raw, dict):
                        metrics = raw
            values = (ts, node_id, sample_ts, state,
                      _bool01(entry.get("reachable")),
                      _bool01(entry.get("sample_stale")), quality)
            for key in NODE_METRICS:
                values = values + ((_number(metrics.get(key))
                                    if metrics is not None else None),)
            rows.append(values)
        return rows

    def _service_row(self, ts, services):
        """Build the service_samples row (or None when no service sample)."""
        # ``services=None`` (no service sample this cycle) yields no row; a
        # dict always records an explicit row (all-NULL metrics are a valid
        # "unavailable" marker, consistent with the node policy).
        if not isinstance(services, dict):
            return None
        vllm = {key: None for key in _VLLM_COUNTERS}
        gateway = {key: None for key in SERVICE_METRICS
                   if key.startswith("gw_") and key not in
                   ("gw_ts", "gw_age_seconds")}
        vllm_extra = (None, None)
        avail = services.get("availability")
        stale = services.get("staleness")
        if isinstance(avail, dict):
            vllm["vllm_available"] = _bool01(avail.get("vllm"))
            gateway["gw_available"] = _bool01(avail.get("gateway"))
        if isinstance(stale, dict):
            vllm["vllm_stale"] = _bool01(stale.get("vllm"))
            gateway["gw_stale"] = _bool01(stale.get("gateway"))
        block = services.get("vllm")
        if isinstance(block, dict):
            vllm_extra = (
                _short(block.get("status"), 32),
                _short(block.get("served_model"), SERVICE_MODEL_MAX_LEN),
            )
            vllm["vllm_healthy"] = _bool01(block.get("health"))
            comp = block.get("metrics")
            if isinstance(comp, dict):
                for key in _VLLM_COUNTERS:
                    vllm[key] = _number(comp.get(key))
        gblock = services.get("gateway")
        if isinstance(gblock, dict):
            gateway["gw_ts"] = _number(gblock.get("ts"))
            gateway["gw_age_seconds"] = _number(gblock.get("age_seconds"))
            gm = gblock.get("metrics")
            if isinstance(gm, dict):
                gateway["gw_requests_total"] = _number(
                    gm.get("requests_total"))
                gateway["gw_errors_total"] = _number(gm.get("errors_total"))
                gateway["gw_in_flight"] = _number(gm.get("in_flight"))
                gateway["gw_since_start_seconds"] = _number(
                    gm.get("since_start_seconds"))
                latency = gm.get("latency_ms")
                if isinstance(latency, dict):
                    gateway["gw_latency_p50_ms"] = _number(latency.get("p50"))
                    gateway["gw_latency_p95_ms"] = _number(latency.get("p95"))
                    gateway["gw_latency_max_ms"] = _number(latency.get("max"))
        values = [ts] + list(vllm_extra) + [
            gateway.get("gw_ts"), gateway.get("gw_age_seconds")]
        for key in SERVICE_METRICS:
            values.append(vllm.get(key, gateway.get(key)))
        return tuple(values)

    # -- retention ----------------------------------------------------------
    def prune(self, now=None):
        """Delete rows older than the retention window (restart-safe).

        Each pass is plain range deletes (freelist pages are reused by later
        inserts, so the file stays bounded; no per-pass VACUUM) followed by a
        PASSIVE checkpoint so the WAL does not grow without bound between
        deletions.  When ``maintenance_seconds > 0`` a pass is real work at
        most once per that window -- a call earlier than that returns without
        touching the database (``skipped: True``).  Returns
        ``{"deleted": N, "now": now, "skipped": bool}``.
        """
        now = self._clock() if now is None else float(now)
        cutoff = now - self.retention
        with self._lock:
            if (self.maintenance > 0
                    and self._last_maintenance is not None
                    and now - self._last_maintenance < self.maintenance):
                return {"deleted": 0, "now": now, "skipped": True}
            with self._writer:
                deleted = 0
                for table in ("cluster_samples", "node_samples",
                              "service_samples"):
                    cursor = self._writer.execute(
                        "DELETE FROM %s WHERE ts < ?" % table, (cutoff,))
                    deleted += cursor.rowcount or 0
            self._writer.execute("PRAGMA wal_checkpoint(PASSIVE)")
            self._last_maintenance = now
        return {"deleted": deleted, "now": now, "skipped": False}

    def status(self):
        """Bounded summary of the history database (for operators/tests)."""
        with closing(self._connect(readonly=True)) as conn:
            rows = {}
            for table in ("cluster_samples", "node_samples",
                          "service_samples"):
                n = conn.execute("SELECT COUNT(*) FROM %s" % table).fetchone()[0]
                lo, hi = conn.execute(
                    "SELECT MIN(ts), MAX(ts) FROM %s" % table).fetchone()
                rows[table] = {"rows": n, "first_ts": lo, "last_ts": hi}
        size = self.path.stat().st_size if self.path.exists() else 0
        return {"path": str(self.path), "size_bytes": size,
                "retention_days": self.retention / 86400.0,
                "schema_version": SCHEMA_VERSION, "tables": rows}

    # -- read helpers -------------------------------------------------------
    def _validate_range(self, start, end):
        now = self._clock()
        if end is None:
            end = now
        if start is None:
            start = max(self._earliest_ts(), end - self.retention)
            if start >= end:
                # empty database or all history within a single tick: fall
                # back to a bounded recent lookback instead of erroring
                start = end - min(3600.0, self.retention)
        if not _finite(start) or not _finite(end) or end <= start:
            raise HistoryError("use a valid start/end time range")
        if end - start > self.retention + 86400:
            raise HistoryError("requested range exceeds configured retention")
        return float(start), float(end)

    def _earliest_ts(self):
        try:
            with closing(self._connect(readonly=True)) as conn:
                row = conn.execute(
                    "SELECT MIN(ts) FROM cluster_samples").fetchone()
        except sqlite3.OperationalError:
            return self._clock() - self.retention
        return row[0] if row and row[0] is not None else (
            self._clock() - self.retention)

    @staticmethod
    def _check_metrics(metrics, allowlist, label):
        """Return an ordered, de-duplicated list confined to the allowlist.

        Anything else -- including any SQL-shaped string -- raises
        :class:`HistoryError`, so a caller can never inject a column.
        """
        if not isinstance(metrics, (list, tuple)) or not metrics:
            raise HistoryError("%s metrics must be a non-empty list" % label)
        out = []
        for name in metrics:
            if not isinstance(name, str) or name not in allowlist:
                raise HistoryError(
                    "unknown %s metric %r (must be an allowlisted name)"
                    % (label, name))
            if name not in out:
                out.append(name)
        return out

    @staticmethod
    def _check_nodes(nodes):
        if isinstance(nodes, str):
            nodes = [nodes]
        if not isinstance(nodes, (list, tuple)) or not nodes:
            raise HistoryError("nodes must be a non-empty node selection")
        out = []
        for node_id in nodes:
            if node_id not in NODE_IDS:
                raise HistoryError("unknown node_id %r" % (node_id,))
            if node_id not in out:
                out.append(node_id)
        return out

    @staticmethod
    def _check_points(points):
        if not isinstance(points, int) or isinstance(points, bool):
            raise HistoryError("points must be an integer")
        # Bounded work on both the query and the reply.
        return max(10, min(5000, points))

    def _bucket_width(self, start, end, points):
        # Snap bucket edges to natural sample boundaries (>= one nominal
        # sample per bucket), never wider than needed for the point budget.
        width = max(self.interval,
                    math.ceil((end - start) / points / self.interval)
                    * self.interval)
        anchor = math.floor(start / width) * width
        return width, anchor

    def _read(self, start, end, work):
        """Run a bounded read with a wall-clock progress deadline."""
        with closing(self._connect(readonly=True)) as conn:
            deadline = time.monotonic() + 12
            conn.set_progress_handler(
                lambda: int(time.monotonic() > deadline), 10000)
            return work(conn)

    # -- query API ----------------------------------------------------------
    def node_history(self, metrics, start=None, end=None, points=300,
                     nodes=None):
        """Bounded min/mean/max history for allowlisted node metrics.

        ``metrics``: list of names from :data:`NODE_METRICS` (anything else
        raises).  ``nodes``: list (or single id) from :data:`NODE_IDS`.
        Returns bucketed series per node+metric with per-bucket sample counts
        and gaps (missing buckets / unavailable metric values) as NULLs and
        zero counts -- the reply size is bounded by ``points`` regardless of
        how many raw samples exist in the window.
        """
        start, end = self._validate_range(start, end)
        points = self._check_points(points)
        names = self._check_metrics(metrics, NODE_METRICS, "node")
        node_ids = self._check_nodes(
            NODE_IDS if nodes is None else nodes)
        width, anchor = self._bucket_width(start, end, points)
        positions = _bucket_positions(width, anchor, start, end)
        per_node = {}
        for node_id in node_ids:
            selects = []
            for name in names:
                selects.append('AVG("%s") AS "%s_avg"' % (name, name))
                selects.append('MIN("%s") AS "%s_min"' % (name, name))
                selects.append('MAX("%s") AS "%s_max"' % (name, name))
                selects.append('COUNT("%s") AS "%s_n"' % (name, name))
            sql = ("SELECT CAST((ts - ?) / ? AS INTEGER) AS bucket, "
                   "AVG(ts) AS t, COUNT(*) AS n, " + ", ".join(selects) +
                   " FROM node_samples WHERE node_id = ? AND ts >= ? AND ts <= ?"
                   " GROUP BY bucket ORDER BY bucket")
            rows = self._read(start, end, lambda conn, sql=sql, node_id=node_id:
                              conn.execute(sql,
                                           (anchor, width, node_id, start,
                                            end)).fetchall())
            series, counts, timestamps = {}, [], []
            for pos in positions:
                row = _row_at(rows, pos)
                series_row = {name: {
                    "mean": row["%s_avg" % name] if row else None,
                    "min": row["%s_min" % name] if row else None,
                    "max": row["%s_max" % name] if row else None,
                    "count": row["%s_n" % name] if row else 0,
                } for name in names}
                for name in names:
                    series.setdefault(name,
                                       {"mean": [], "min": [], "max": [],
                                        "count": []})
                    for label, arr in (("mean", "mean"), ("min", "min"),
                                       ("max", "max"), ("count", "count")):
                        series[name][label].append(series_row[name][label])
                counts.append(row["n"] if row else 0)
                timestamps.append(row["t"] if row else
                                  anchor + (pos + 0.5) * width)
            per_node[node_id] = {"timestamps": timestamps,
                                 "sample_counts": counts, "series": series}
        total = sum(sum(entry["sample_counts"])
                    for entry in per_node.values())
        return {"start": start, "end": end,
                "bucket_seconds": width,
                "nodes": node_ids, "metrics": names,
                "units": {name: NODE_METRIC_UNITS.get(name) for name in names},
                "per_node": per_node,
                "sample_counts": {nid: per_node[nid]["sample_counts"]
                                   for nid in node_ids},
                "sample_count": total}

    def service_history(self, metrics, start=None, end=None, points=300):
        """Bounded min/mean/max history for allowlisted service metrics.

        Covers vLLM counters/health and gateway request/error/latency
        metrics from :data:`SERVICE_METRICS`; availability and staleness
        are stored as 0/1 series (NULL while unknown).
        """
        start, end = self._validate_range(start, end)
        points = self._check_points(points)
        names = self._check_metrics(metrics, SERVICE_METRICS, "service")
        width, anchor = self._bucket_width(start, end, points)
        positions = _bucket_positions(width, anchor, start, end)
        selects = []
        for name in names:
            selects.append('AVG("%s") AS "%s_avg"' % (name, name))
            selects.append('MIN("%s") AS "%s_min"' % (name, name))
            selects.append('MAX("%s") AS "%s_max"' % (name, name))
            selects.append('COUNT("%s") AS "%s_n"' % (name, name))
        sql = ("SELECT CAST((ts - ?) / ? AS INTEGER) AS bucket, AVG(ts) AS t, "
               "COUNT(*) AS n, " + ", ".join(selects) +
               " FROM service_samples WHERE ts >= ? AND ts <= ? "
               "GROUP BY bucket ORDER BY bucket")
        rows = self._read(start, end, lambda conn: conn.execute(
            sql, (anchor, width, start, end)).fetchall())
        series = {name: {"mean": [], "min": [], "max": [], "count": []}
                  for name in names}
        timestamps, counts = [], []
        for pos in positions:
            row = _row_at(rows, pos)
            counts.append(row["n"] if row else 0)
            timestamps.append(row["t"] if row else anchor + (pos + 0.5) * width)
            for name in names:
                series[name]["mean"].append(
                    row["%s_avg" % name] if row else None)
                series[name]["min"].append(row["%s_min" % name] if row else None)
                series[name]["max"].append(row["%s_max" % name] if row else None)
                series[name]["count"].append(row["%s_n" % name] if row else 0)
        return {"start": start, "end": end,
                "bucket_seconds": width,
                "metrics": names,
                "units": {name: SERVICE_METRIC_UNITS.get(name)
                          for name in names},
                "timestamps": timestamps,
                "sample_counts": counts,
                "sample_count": sum(counts),
                "series": series}

    def cluster_history(self, start=None, end=None, points=300):
        """Bounded cluster-mode history.

        One row per bucket keeps the latest observed (mode, healthy,
        transitioning, controller_ok) -- non-numeric state is not averaged.
        Missing buckets are gaps (NULLs), never invented states.
        """
        start, end = self._validate_range(start, end)
        points = self._check_points(points)
        width, anchor = self._bucket_width(start, end, points)
        positions = _bucket_positions(width, anchor, start, end)
        sql = ("SELECT bucket, ts, mode, healthy, transitioning, "
               "controller_ok FROM ("
               " SELECT CAST((ts - ?) / ? AS INTEGER) AS bucket,"
               "        ROW_NUMBER() OVER (PARTITION BY"
               "            CAST((ts - ?) / ? AS INTEGER) ORDER BY ts DESC)"
               "        AS rn, ts, mode, healthy, transitioning,"
               "        controller_ok"
               " FROM cluster_samples WHERE ts >= ? AND ts <= ?)"
               " WHERE rn = 1 ORDER BY bucket")
        rows = self._read(start, end, lambda conn: conn.execute(
            sql, (anchor, width, anchor, width, start, end)).fetchall())
        timestamps = []
        series = {"mode": [], "healthy": [], "transitioning": [],
                  "controller_ok": []}
        for pos in positions:
            row = _row_at(rows, pos)
            timestamps.append(row["ts"] if row else
                              anchor + (pos + 0.5) * width)
            series["mode"].append(row["mode"] if row else None)
            series["healthy"].append(bool(row["healthy"])
                                     if row and row["healthy"] is not None
                                     else None)
            series["transitioning"].append(bool(row["transitioning"])
                                           if row and row["transitioning"]
                                           is not None else None)
            series["controller_ok"].append(
                bool(row["controller_ok"])
                if row and row["controller_ok"] is not None else None)
        return {"start": start, "end": end,
                "bucket_seconds": width,
                "timestamps": timestamps, "series": series}

    def close(self):
        with self._lock:
            if self._writer is not None:
                self._writer.close()
                self._writer = None


def _bucket_positions(width, anchor, start, end):
    """Bucket indices (relative to ``anchor``) covering [start, end]."""
    first = math.floor((start - anchor) / width)
    last = math.floor((end - anchor) / width)
    return range(first, last + 1)


def _row_at(rows, bucket):
    """Find the row for a bucket; rows must be ordered by bucket."""
    buckets = [row["bucket"] for row in rows]
    index = _bisect.bisect_right(buckets, bucket)
    if index and rows[index - 1]["bucket"] == bucket:
        return rows[index - 1]
    return None
