"""Tests for cluster.runtime (central collection + history runtime).

Deterministic by construction: most tests drive one cycle synchronously
via ``_run_cycle()`` with a fake aggregator, an injectable fake clock and
a real SQLite history store in a tempdir.  A few tests start the actual
worker thread with a *bounded* fake sleeper (the sleeper advances a fake
clock and/or trips the stop event after a fixed number of cycles), so no
test can spin an unbounded fake-clock loop.

No services are installed, no production database is created and the
inference cluster is never touched.
"""

import copy
import json
import os
import sqlite3
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from cluster.history import ClusterHistoryStore
from cluster.runtime import (
    DEFAULT_INTERVAL_SECONDS,
    DEFAULT_MAINTENANCE_SECONDS,
    ClusterRuntime,
)

NOW = 1_700_000_000.0
NODE_IDS = ("dgx01", "dgx02", "dgx03")


class FakeClock:
    """Injectable clock; tests advance it explicitly, never implicitly."""

    def __init__(self, start=NOW):
        self.t = start

    def __call__(self):
        return self.t

    def advance(self, seconds):
        self.t += seconds


def online_sample(ts, **overrides):
    metrics = {"cpu_percent": 12.5, "gpu_percent": 40.0,
               "ram_used_bytes": 4096.0, "vllm_process_count": 2.0}
    metrics.update(overrides)
    return {"schema_version": 1, "timestamp": ts, "status": "ok",
            "metrics": metrics}


def make_snapshot(ts, mode="split", controller_ok=True, nodes=None,
                  warnings=()):
    """A structurally valid aggregator snapshot (matches record_snapshot)."""
    if controller_ok:
        cluster = {"mode": mode, "healthy": True, "transitioning": False,
                   "controller": "ok", "source": "test", "raw": None,
                   "error": None}
    else:  # mirrors the aggregator's controller-unavailable output
        cluster = {"mode": "unknown", "healthy": None,
                   "transitioning": None, "controller": "error",
                   "source": "test", "raw": None,
                   "error": "test controller failure"}
    if nodes is None:
        nodes = {nid: {"state": "online", "reachable": True,
                       "sample_stale": False,
                       "sample": online_sample(ts - 1.0)}
                 for nid in NODE_IDS}
    return {"schema_version": 1, "timestamp": ts, "cluster": cluster,
            "nodes": nodes, "warnings": list(warnings),
            "meta": {"total_ms": 5}}


def good_services():
    return {"availability": {"vllm": False, "gateway": True},
            "staleness": {"vllm": False, "gateway": False},
            "vllm": {"status": "inactive", "health": None,
                     "served_model": None, "metrics": {}},
            "gateway": {"ts": NOW, "age_seconds": 1.0,
                        "metrics": {"requests_total": 10.0,
                                    "errors_total": 0.0, "in_flight": 0.0,
                                    "since_start_seconds": 100.0,
                                    "latency_ms": {"p50": 1.0, "p95": 2.0,
                                                   "max": 3.0}}}}


class FakeAggregator:
    """collect() returns a fixed snapshot (or calls snapshot_fn) or raises.

    Optionally trips the runtime stop event after *stop_after* collects so
    background-loop tests are always bounded."""

    def __init__(self, snapshot=None, exc=None, snapshot_fn=None,
                 stop_after=None):
        self.snapshot = snapshot
        self.exc = exc
        self.snapshot_fn = snapshot_fn
        self.stop_after = stop_after
        self.collect_calls = 0
        self._depth = 0
        self.max_depth = 0
        self.rt = None                # bound by tests needing loop control

    def collect(self):
        self.collect_calls += 1
        self._depth += 1
        self.max_depth = max(self.max_depth, self._depth)
        try:
            if self.stop_after is not None \
                    and self.collect_calls >= self.stop_after \
                    and self.rt is not None:
                self.rt._stop_event.set()
            if self.exc is not None:
                raise self.exc
            if self.snapshot_fn is not None:
                return self.snapshot_fn()
            return self.snapshot
        finally:
            self._depth -= 1


class LoopDriver:
    """Deterministic sleeper for background-loop tests.

    Each cycle that would block calls the sleeper, which advances the fake
    clock and - once the aggregator has collected *after_cycles* times -
    trips the runtime stop event.  The worker therefore cannot spin without
    blocking; every loop iteration either advances fake time or exits."""

    def __init__(self, clock, aggregator, after_cycles):
        self.clock = clock
        self.aggregator = aggregator
        self.after_cycles = after_cycles
        self.delays = []
        self.runtime = None

    def __call__(self, seconds):
        self.delays.append(seconds)
        self.clock.advance(seconds)
        if self.runtime is not None \
                and self.aggregator.collect_calls >= self.after_cycles:
            self.runtime._stop_event.set()


class RuntimeTest(unittest.TestCase):
    """Base: tempdir SQLite store + fake clock per test."""

    interval = DEFAULT_INTERVAL_SECONDS

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db_path = os.path.join(self.tmp.name, "history.db")
        self.clock = FakeClock()

    def new_store(self, maintenance_seconds=DEFAULT_MAINTENANCE_SECONDS,
                  **kw):
        return ClusterHistoryStore(self.db_path, clock=self.clock,
                                   sample_interval_seconds=self.interval,
                                   maintenance_seconds=maintenance_seconds,
                                   **kw)

    def make_runtime(self, store=None, aggregator=None, service=None,
                     sleeper=None, **kw):
        return ClusterRuntime(
            interval_seconds=kw.pop("interval", self.interval),
            store=store, database=None if store is not None
            else kw.pop("database", self.db_path),
            aggregator=aggregator,
            service_metrics_fn=service if service is not None
            else (lambda snap: good_services()),
            clock=kw.pop("clock", self.clock), sleeper=sleeper, **kw)

    def wait_worker_idle(self, rt, minimum_cycles, timeout=10.0):
        """Wait (real time) for the worker to do *minimum_cycles* and exit.

        For self-terminating loop tests: the stop event must NOT be set by
        the test before the worker finishes its cycles, so stop() is only
        called afterwards (it then just joins the dead thread).
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            cycles = rt.status()["cycles"]
            alive = rt._thread is not None and rt._thread.is_alive()
            if cycles >= minimum_cycles and not alive:
                return True
            time.sleep(0.005)
        return False

    def rows(self, table, **columns):
        conn = sqlite3.connect(self.db_path)
        try:
            query = "SELECT %s FROM %s" % (
                ", ".join(columns.keys()) if columns else "*", table)
            return conn.execute(query).fetchall()
        finally:
            conn.close()

    def query(self, sql, path=None, args=(), readonly=True):
        """Arbitrary read query against the tempdir database."""
        conn = sqlite3.connect(path or self.db_path)
        try:
            return conn.execute(sql, args).fetchall()
        finally:
            conn.close()


class TestConstruction(RuntimeTest):
    def test_store_or_database_required(self):
        with self.assertRaises(ValueError):
            ClusterRuntime(store=None, database=None)

    def test_interval_must_be_positive(self):
        for bad in (0, -1, "15", True):
            with self.assertRaises(ValueError):
                ClusterRuntime(database=self.db_path, interval_seconds=bad)

    def test_default_store_uses_hourly_maintenance(self):
        # Checked against ClusterHistoryStore's real default (0.0):
        # the runtime deliberately passes once-per-hour maintenance.
        rt = self.make_runtime(aggregator=FakeAggregator())
        self.assertEqual(rt.store.maintenance,
                         DEFAULT_MAINTENANCE_SECONDS)
        self.assertEqual(rt.store.maintenance, 3600.0)
        self.assertIsNone(rt.store._last_maintenance)
        self.assertEqual(rt.store.interval, DEFAULT_INTERVAL_SECONDS)
        self.assertEqual(rt.status()["interval_seconds"], 15.0)


class TestSingleCycle(RuntimeTest):
    def test_successful_cycle_persists_and_publishes(self):
        store = self.new_store()
        snap = make_snapshot(self.clock())
        agg = FakeAggregator(snapshot=snap)
        rt = self.make_runtime(store=store, aggregator=agg)

        status = rt._run_cycle()

        self.assertEqual(agg.collect_calls, 1)
        self.assertEqual(agg.max_depth, 1)
        entry = rt.latest()
        self.assertEqual(entry["cycle"], 1)
        self.assertEqual(entry["snapshot"]["timestamp"], snap["timestamp"])
        self.assertEqual(
            rt.latest_snapshot()["cluster"]["mode"], "split")
        self.assertEqual(self.rows("cluster_samples", ts=1, mode=1),
                         [(snap["timestamp"], "split")])
        self.assertEqual(
            [r[0] for r in self.rows("node_samples", node_id=1, state=1)],
            list(NODE_IDS))
        self.assertEqual(
            self.rows("service_samples", gw_requests_total=1),
            [(10.0,)])  # one service row with the fake gateway counter
        self.assertEqual(status["cycles"], 1)
        self.assertEqual(status["last_errors"],
                         {"collect": None, "service": None,
                          "history": None, "prune": None})
        self.assertIsNotNone(store._last_maintenance)
        self.assertEqual(status["history"]["tables"]["cluster_samples"]
                         ["rows"], 1)


class TestPersistence(RuntimeTest):
    def test_unavailable_node_is_persisted_as_explicit_state(self):
        store = self.new_store()
        self.clock.advance(0.5)
        nodes = {nid: {"state": "online", "reachable": True,
                       "sample_stale": False,
                       "sample": online_sample(self.clock())}
                 for nid in NODE_IDS}
        nodes["dgx02"] = {"state": "unreachable", "reachable": False,
                          "sample_stale": None, "sample": None,
                          "error": "connection refused"}
        snap = make_snapshot(self.clock(), nodes=nodes)
        self.make_runtime(
            store=store,
            aggregator=FakeAggregator(snapshot=snap))._run_cycle()

        plain = self.rows("node_samples", node_id=1, state=1, reachable=1,
                          cpu_percent=1, sample_ts=1)
        by_id = {row[0]: row for row in plain}
        self.assertEqual(set(by_id), set(NODE_IDS))
        online = by_id["dgx01"]
        self.assertEqual(online[1:],
                         ("online", 1, 12.5,
                          snap["nodes"]["dgx01"]["sample"]["timestamp"]))
        dead = by_id["dgx02"]
        self.assertEqual(dead[1:], ("unreachable", 0, None, None))
        self.assertEqual(self.rows("cluster_samples", mode=1, healthy=1),
                         [("split", 1)])

    def test_controller_unavailable_is_stored_verbatim_unknown(self):
        store = self.new_store()
        self.clock.advance(1.0)
        snap = make_snapshot(self.clock(), controller_ok=False)
        rt = self.make_runtime(store=store,
                               aggregator=FakeAggregator(snapshot=snap))
        status = rt._run_cycle()
        # The aggregator never raises on a controller failure: the snapshot
        # is still collected and persisted with mode ``unknown``.
        self.assertEqual(status["last_errors"]["collect"], None)
        self.assertEqual(self.rows(
            "cluster_samples", mode=1, healthy=1, transitioning=1,
            controller_ok=1),
            [("unknown", None, None, 0)])
        self.assertEqual(rt.latest_snapshot()["cluster"]["mode"],
                         "unknown")

    def test_modes_are_stored_verbatim_in_order(self):
        store = self.new_store()
        expected = ["split", "heavy-qwen", "heavy-deepseek", "stop",
                    "transitioning"]
        for mode in expected:
            self.clock.advance(self.interval)
            snap = make_snapshot(self.clock(), mode=mode)
            self.make_runtime(
                store=store, aggregator=FakeAggregator(snapshot=snap))\
                ._run_cycle()
        conn = sqlite3.connect(self.db_path)
        try:
            modes = [r[0] for r in conn.execute(
                "SELECT mode FROM cluster_samples ORDER BY ts")]
        finally:
            conn.close()
        self.assertEqual(modes, expected)


class TestStaleSamples(RuntimeTest):
    def test_stale_state_stores_null_metrics_but_keeps_marker(self):
        store = self.new_store()
        self.clock.advance(1.0)
        nodes = {nid: {"state": "online", "reachable": True,
                       "sample_stale": False,
                       "sample": online_sample(self.clock())}
                 for nid in NODE_IDS}
        # A node whose telemetry is stale: the sample is present (with
        # metrics) but state is not "online", so numbers must be NULL.
        nodes["dgx03"] = {"state": "stale", "reachable": True,
                          "sample_stale": True,
                          "sample": online_sample(self.clock() - 900.0,
                                                  cpu_percent=77.7),
                          "error": "last sample 912s old"}
        snap = make_snapshot(self.clock(), nodes=nodes)
        self.make_runtime(
            store=store, aggregator=FakeAggregator(snapshot=snap))\
            ._run_cycle()

        row = self.query(
            "SELECT node_id, state, reachable, sample_stale, quality, "
            "cpu_percent, sample_ts FROM node_samples WHERE node_id = ?",
            args=("dgx03",))[0]
        node_id, state, reachable, sample_stale, quality, cpu, sample_ts \
            = row
        self.assertEqual(node_id, "dgx03")
        self.assertEqual(state, "stale")
        self.assertTrue(reachable)
        self.assertTrue(sample_stale)
        self.assertEqual(quality, "ok")
        self.assertIsNone(cpu)  # stale sample: numbers are NULL
        self.assertEqual(sample_ts,
                         snap["nodes"]["dgx03"]["sample"]["timestamp"])


class TestCollectionErrors(RuntimeTest):
    def test_collect_failure_is_isolated_and_cache_keeps_last_good(self):
        store = self.new_store()
        good = make_snapshot(self.clock())
        self.clock.advance(self.interval)
        rt = self.make_runtime(store=store, aggregator=FakeAggregator())
        rt.aggregator.snapshot = good
        rt._run_cycle()
        self.assertIsNotNone(rt.latest())

        # Next cycle: the collector itself raises (e.g. a bug).
        self.clock.advance(self.interval)
        rt.aggregator.exc = RuntimeError("collector exploded")
        status = rt._run_cycle()

        self.assertIn("collector exploded", status["last_errors"]["collect"])
        self.assertEqual(status["last_errors"]["collect"].split(": ")[0],
                         "RuntimeError")
        self.assertEqual(status["cycles"], 2)
        # cache keeps the last good snapshot; nothing new persisted
        self.assertEqual(rt.latest()["snapshot"], good)
        self.assertEqual(len(self.rows("cluster_samples", ts=1)), 1)
        self.assertEqual(len(self.rows("node_samples", ts=1)), 3)

    def test_collect_failure_records_no_service_sample(self):
        store = self.new_store()
        service_calls = []
        rt = self.make_runtime(store=store, aggregator=FakeAggregator(
            exc=RuntimeError("down")),
            service=lambda snap: service_calls.append(1) or good_services())
        self.clock.advance(1.0)
        status = rt._run_cycle()
        # No snapshot -> no service metrics attempt, no history write.
        self.assertEqual(service_calls, [])
        self.assertEqual(status["last_errors"]["service"], None)
        self.assertEqual(status["last_errors"]["history"], None)
        self.assertEqual(len(self.rows("cluster_samples", ts=1)), 0)
        self.assertIsNone(rt.latest())


class TestServiceMetrics(RuntimeTest):
    def test_service_error_is_isolated_and_snapshot_still_recorded(self):
        store = self.new_store()
        self.clock.advance(1.0)
        snap = make_snapshot(self.clock())
        rt = self.make_runtime(
            store=store, aggregator=FakeAggregator(snapshot=snap),
            service=lambda s: (_ for _ in ()).throw(
                RuntimeError("service probe broken")))
        status = rt._run_cycle()

        self.assertIn("service probe broken",
                      status["last_errors"]["service"])
        self.assertIsNone(status["last_errors"]["history"])
        # snapshot still persisted; *no* service row for this ts
        self.assertEqual(len(self.rows("cluster_samples", ts=1)), 1)
        self.assertEqual(
            self.query("SELECT COUNT(*) FROM service_samples WHERE ts = ?",
                       args=(snap["timestamp"],))[0][0], 0)
        self.assertEqual(rt.latest()["snapshot"], snap)


class TestHistoryFailures(RuntimeTest):
    def test_history_write_failure_is_isolated_then_recovers(self):
        store = self.new_store()
        self.clock.advance(1.0)
        snap = make_snapshot(self.clock())
        rt = self.make_runtime(store=store, aggregator=FakeAggregator(
            snapshot=snap))

        original = store.record_snapshot
        failures = {"remaining": 2, "last_error": None}

        persisted_ts = []

        def flaky(snapshot, services=None):
            if failures["remaining"] > 0:
                failures["remaining"] -= 1
                raise sqlite3.OperationalError("database is locked")
            result = original(snapshot, services=services)
            persisted_ts.append(snapshot["timestamp"])
            return result

        store.record_snapshot = flaky
        # two consecutive cycles fail the write; the third recovers
        statuses = []
        for _ in range(3):
            self.clock.advance(self.interval)
            snap = make_snapshot(self.clock())
            rt.aggregator.snapshot = snap
            statuses.append(rt._run_cycle())
        for cycle_status in statuses[:2]:
            self.assertEqual(cycle_status["last_errors"]["history"]
                             .split(": ")[0], "OperationalError")
            self.assertIn("database is locked",
                          cycle_status["last_errors"]["history"])
        status = statuses[2]
        self.assertIsNone(status["last_errors"]["history"])
        self.assertEqual(status["cycles"], 3)
        # live cache holds the latest snapshot despite earlier write errors
        self.assertEqual(rt.latest()["snapshot"], snap)
        # only the successful (third) cycle was persisted
        self.assertEqual(persisted_ts, [snap["timestamp"]])
        self.assertEqual(len(self.rows("cluster_samples", ts=1)), 1)
        self.assertEqual(
            self.query("SELECT ts FROM cluster_samples ORDER BY ts"),
            [(snap["timestamp"],)])


class TestBackgroundLoop(RuntimeTest):
    """Runs the real worker thread with a bounded, deterministic sleeper.

    The worker is a daemon; every loop iteration either advances the fake
    clock (sleeper) or exits via the stop event, so no test can spin an
    unbounded fake-clock loop.
    """

    def test_loop_collects_at_interval_without_overlap(self):
        clock = self.clock
        agg = FakeAggregator(snapshot_fn=lambda: make_snapshot(clock()))
        driver = LoopDriver(clock, agg, after_cycles=3)
        rt = self.make_runtime(aggregator=agg, sleeper=driver,
                               interval=15.0)
        driver.runtime = rt
        rt.start()
        self.assertTrue(
            self.wait_worker_idle(rt, 3), "worker did not finish 3 cycles")
        self.assertTrue(rt.stop(timeout=10))

        # exactly one collection per cycle, never overlapping
        self.assertEqual(agg.collect_calls, 3)
        self.assertEqual(agg.max_depth, 1)
        # a full nominal gap between cycles (slow cycle => no backlog)
        self.assertEqual(driver.delays, [15.0, 15.0, 15.0])
        # strict +15 s spacing in persisted samples
        ts = [r[0] for r in self.query(
            "SELECT ts FROM cluster_samples ORDER BY ts")]
        base = clock.t - 45.0
        self.assertEqual(ts, [base, base + 15.0, base + 30.0])

    def test_slow_cycle_never_backlogs_or_overlaps(self):
        clock = self.clock

        def slow_snap():
            clock.advance(40.0)  # cycle takes longer than the 15 s interval
            return make_snapshot(clock())

        agg = FakeAggregator(snapshot_fn=slow_snap, stop_after=3)
        delays = []
        rt = self.make_runtime(
            aggregator=agg, sleeper=delays.append, interval=15.0)
        agg.rt = rt
        rt.start()
        self.assertTrue(
            self.wait_worker_idle(rt, 3), "worker did not finish 3 cycles")
        self.assertTrue(rt.stop(timeout=10))

        self.assertEqual(agg.collect_calls, 3)
        self.assertEqual(agg.max_depth, 1)
        self.assertEqual(delays, [])  # never waited; next tick ran at once
        ts = [r[0] for r in self.query(
            "SELECT ts FROM cluster_samples ORDER BY ts")]
        self.assertEqual(len(ts), 3)
        self.assertEqual(
            [round(b - a, 6) for a, b in zip(ts, ts[1:])], [40.0, 40.0])

    def test_bounded_shutdown_is_prompt_and_stops_the_thread(self):
        rt = self.make_runtime(
            aggregator=FakeAggregator(
                snapshot_fn=lambda: make_snapshot(time.time())),
            clock=time.time)  # real clock + real (default) stop-wait sleeper
        rt.start()
        deadline = time.monotonic() + 5.0
        while rt.status()["cycles"] < 1 \
                and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertGreaterEqual(rt.status()["cycles"], 1)

        started = time.monotonic()
        self.assertTrue(rt.stop(timeout=5.0))
        self.assertLess(time.monotonic() - started, 4.0)
        self.assertFalse(rt.is_running)


class TestCrossThreadWriter(RuntimeTest):
    def test_writer_connection_is_usable_from_another_thread(self):
        # Regression: the store's single writer connection is created on the
        # thread that builds the store, but the runtime's worker thread does
        # all the writing.  It must work cross-thread (safe via the store's
        # single-writer lock).
        store = self.new_store()
        result = []

        def writer():
            try:
                store.record_snapshot(make_snapshot(1_700_000_000.0))
                result.append("ok")
            except Exception as exc:  # noqa: BLE001 - surface any error
                result.append(exc)

        thread = threading.Thread(target=writer, name="cross-thread-writer")
        thread.start()
        thread.join(5)
        self.assertEqual(result, ["ok"])
        self.assertEqual(
            self.query("SELECT COUNT(*) FROM cluster_samples"), [(1,)])


class TestRestartWithExistingHistory(RuntimeTest):
    def test_reopen_existing_history_preserves_prior_samples(self):
        # First "process": build a store on the DB and persist two cycles.
        store1 = self.new_store()
        rt1 = self.make_runtime(store=store1,
                                aggregator=FakeAggregator(
                                    snapshot=make_snapshot(self.clock())))
        rt1._run_cycle()
        self.clock.advance(self.interval)
        rt1.aggregator.snapshot = make_snapshot(self.clock())
        rt1._run_cycle()
        first_ts, second_ts = [r[0] for r in self.query(
            "SELECT ts FROM cluster_samples ORDER BY ts")]

        # "Restart": a brand-new store object opens the SAME file.  It must
        # not clobber existing data (schema pinned, tables are IF NOT EXISTS).
        store2 = self.new_store()
        rt2 = self.make_runtime(store=store2,
                                aggregator=FakeAggregator(
                                    snapshot=make_snapshot(self.clock() +
                                                           self.interval)))
        rt2._run_cycle()

        rows = [r[0] for r in self.query(
            "SELECT ts FROM cluster_samples ORDER BY ts")]
        self.assertEqual(rows, [first_ts, second_ts,
                                first_ts + 2 * self.interval])
        # node samples for all three cycles preserved too
        self.assertEqual(
            self.query("SELECT COUNT(*) FROM node_samples"), [(9,)])
        self.assertEqual(
            store2.status()["tables"]["cluster_samples"]["rows"], 3)


class TestJsonSerialization(RuntimeTest):
    def test_status_and_latest_are_json_serializable(self):
        store = self.new_store()
        self.clock.advance(1.0)
        snap = make_snapshot(self.clock())
        rt = self.make_runtime(store=store, aggregator=FakeAggregator())
        # one failed cycle (error strings present) then a good one
        rt.aggregator.exc = RuntimeError("boom")
        failed_status = rt._run_cycle()
        self.assertIn("RuntimeError", failed_status["last_errors"]["collect"])
        self.assertIn("boom", failed_status["last_errors"]["collect"])
        json.dumps(failed_status)  # error strings are JSON-safe
        self.clock.advance(self.interval)
        rt.aggregator.exc = None
        rt.aggregator.snapshot = snap
        rt._run_cycle()

        status = rt.status()
        encoded = json.dumps(status)
        self.assertEqual(json.loads(encoded)["cycles"], 2)
        # last_errors describes the most recent (good) cycle
        self.assertEqual(status["last_errors"]["collect"], None)
        self.assertIsInstance(status["history"]["size_bytes"], int)

        latest = json.loads(json.dumps(rt.latest()))
        self.assertEqual(latest["snapshot"]["cluster"]["mode"], "split")
        self.assertEqual(rt.latest_snapshot()["cluster"]["mode"], "split")

    def test_latest_before_first_success_is_none(self):
        store = self.new_store()
        rt = self.make_runtime(store=store, aggregator=FakeAggregator())
        self.assertIsNone(rt.latest())
        self.assertIsNone(rt.latest_snapshot())
        # a failed cycle must not publish anything
        rt.aggregator.exc = RuntimeError("no snapshot")
        rt._run_cycle()
        self.assertIsNone(rt.latest())
        self.assertEqual(rt.status()["cycles"], 1)
        self.assertEqual(rt.status()["latest"], None)


class TestNoClusterMutations(RuntimeTest):
    def test_runtime_does_not_mutate_the_collected_snapshot(self):
        store = self.new_store()
        self.clock.advance(1.0)
        snap = make_snapshot(self.clock())
        original = copy.deepcopy(snap)
        rt = self.make_runtime(store=store,
                               aggregator=FakeAggregator(snapshot=snap))
        rt._run_cycle()
        # the aggregator's object is left exactly as produced
        self.assertEqual(snap, original)
        # the cache holds an independent deep copy (not the same object)
        cached = rt.latest()["snapshot"]
        self.assertIsNot(cached, snap)
        self.assertEqual(cached, snap)
        # mutating the aggregator's snapshot afterwards has no effect...
        snap["cluster"]["mode"] = "CORRUPTED"
        self.assertEqual(rt.latest_snapshot()["cluster"]["mode"], "split")
        # ...so a later successful cycle of the *same* object republishes
        # pristine data from the aggregator's perspective
        self.assertEqual(snap["cluster"]["mode"], "CORRUPTED")
