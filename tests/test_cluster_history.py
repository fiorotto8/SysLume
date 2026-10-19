"""Tests for cluster.history (central SQLite historical storage, Phase 19A).

Fixtures are produced by driving the *real* :class:`cluster.aggregator.
ClusterAggregator` with injected request_fn / controller / clock (the same
dependency-injection technique as tests/test_cluster_aggregator.py), so the
store is always exercised against the actual aggregator output schema.  The
service fixtures use the exact :func:`cluster.service_metrics.collect_service_
metrics` result shape.

All databases live in temporary directories; no production database is ever
created and no live cluster endpoint is touched.
"""

import json
import math
import os
import re
import sqlite3
import sys
import tempfile
import unittest
from contextlib import closing
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from cluster.aggregator import (ClusterAggregator, ControllerError,
                                NodeFetchError)
from cluster.history import (
    ClusterHistoryStore,
    HistoryError,
    NODE_IDS,
    NODE_METRICS,
    NODE_METRIC_UNITS,
    SCHEMA_VERSION,
    SERVICE_METRICS,
    SERVICE_METRIC_UNITS,
)
from collectors import node_collector as NC
from collectors import system_metrics as SM

TABLES = ("cluster_samples", "node_samples", "service_samples")


# --------------------------------------------------------------------------
# Fixtures (real ClusterAggregator with injected fakes)
# --------------------------------------------------------------------------

def split_metrics(**over):
    m = {"cpu_percent": 12.0, "ram_used_bytes": 100, "ram_total_bytes": 200,
         "gpu_temp_c": None, "ollama_up": 1, "ollama_daemon_count": 1,
         "ollama_runner_count": 1, "ollama_models_installed": 4,
         "ollama_models_resident": 2, "ray_process_count": 0,
         "vllm_process_count": 0, "vllm_engine_core_count": 0,
         "vllm_rank_worker_count": 0, "docker_container_count": 2,
         "tmux_session_count": 1}
    m.update(over)
    return m


def node_sample(nid, metrics, status="ok", schema_version=1,
                timestamp=9000.0, extra=None):
    sample = {"schema_version": schema_version, "node_id": nid,
              "timestamp": timestamp, "status": status, "components": {},
              "metrics": metrics, "warnings": []}
    if extra:
        sample.update(extra)
    return sample


def telemetry(nid, metrics, status="ok", remote_stale=False,
              timestamp=9000.0):
    return {"node": node_sample(nid, metrics, status=status,
                                timestamp=timestamp),
            "server_time": timestamp + 1.0, "age_seconds": 1.0,
            "stale": remote_stale, "collector_error": None}


def controller_status(mode="split", healthy=True, transitioning=False):
    return {"schema": 1, "ts": "2026-01-01T00:00:00Z", "mode": mode,
            "healthy": healthy, "transitioning": transitioning, "notes": [],
            "transition": None}


class FakeClock:
    def __init__(self, start=9000.0):
        self.t = start

    def __call__(self):
        return self.t

    def advance(self, seconds):
        self.t += seconds


class FakeController:
    def __init__(self, status=None, exc=None):
        self._status = status
        self._exc = exc
        self.calls = 0

    def status(self):
        self.calls += 1
        if self._exc is not None:
            raise self._exc
        return self._status


HOST_URLS = {
    "127.0.0.1": "dgx01",          # DGX01 control node (loopback)
    "192.168.0.2": "dgx02",        # DGX02
    "192.168.2.2": "dgx03",        # DGX03
}


class AggregatorSession:

    """One long-lived real ClusterAggregator driven cycle by cycle.

    Unlike a throwaway aggregator, the session keeps its rolling per-node
    state (last accepted sample, freshness windows), which is what makes the
    online / stale / error / never distinctions observable across cycles.
    """

    def __init__(self):
        self._clock = FakeClock(0.0)
        self._controller = FakeController(controller_status())
        self._payloads = {}
        self._agg = ClusterAggregator(
            controller=self._controller, clock=self._clock,
            request_fn=self._request_fn)

    def _request_fn(self, url, token, timeout):
        nid = next((node for host, node in HOST_URLS.items() if host in url),
                   None)
        if nid is None or nid not in self._payloads:
            raise NodeFetchError("connection error to %s" % url)
        payload = self._payloads[nid]
        if isinstance(payload, Exception):
            raise payload
        return payload

    def cycle(self, payloads, now, mode="split", healthy=True,
              transitioning=False, controller_available=True):
        """Feed one set of node payloads at central time ``now``."""
        self._payloads = payloads
        self._clock.t = now
        if controller_available:
            self._controller._status = controller_status(mode, healthy,
                                                         transitioning)
            self._controller._exc = None
        else:
            self._controller._exc = ControllerError("controller unavailable")
        return self._agg.collect()


def all_online_payloads(ts):
    return {nid: telemetry(nid, split_metrics(cpu_percent=5.0 + i),
                           timestamp=ts - 1.0)
            for i, nid in enumerate(NODE_IDS)}


def services_result(availability, staleness, vllm_status, vllm_metrics,
                    vllm_health, served_model, gw_metrics, gw_ts, gw_age):
    """One collect_service_metrics() result (actual schema)."""
    return {
        "schema_version": 1,
        "timestamp": 9500.0,
        "cluster_mode": "heavy-qwen",
        "model_state_source": "aggregator-snapshot",
        "vllm": {"status": vllm_status, "expected_mode": "heavy-qwen",
                 "expected_model_id": "heavy-qwen",
                 "served_model": served_model,
                 "model_identity": ("verified" if served_model == "heavy-qwen"
                                    else None),
                 "health": vllm_health, "metrics": vllm_metrics,
                 "available": availability["vllm"],
                 "stale": staleness["vllm"], "sample_ts": 9499.0,
                 "error": None, "details": None},
        "gateway": {"available": availability["gateway"],
                    "stale": staleness["gateway"], "ts": gw_ts,
                    "age_seconds": gw_age, "source_path": "/tmp/metrics.json",
                    "metrics": gw_metrics, "error": None},
        "metrics": {"vllm": vllm_metrics, "gateway": gw_metrics},
        "availability": availability,
        "staleness": staleness,
        "warnings": [],
    }


def sample_services(vllm_ok=False, gw_ok=True, **over):
    v_metrics = None
    served = None
    health = None
    if vllm_ok:
        v_metrics = {"vllm_up": 1, "vllm_running_reqs": 3,
                     "vllm_waiting_reqs": 0, "vllm_kv_cache_usage": 0.42,
                     "vllm_prompt_tokens_total": 1000,
                     "vllm_generation_tokens_total": 250}
        served, health = "heavy-qwen", True
    gw_metrics = None
    if gw_ok:
        gw_metrics = {"requests_total": 1200, "errors_total": 2,
                      "in_flight": 1, "since_start_seconds": 3600.0,
                      "latency_ms": {"p50": 8.5, "p95": 90.0,
                                     "max": 410.0}, "counters": {}}
    return services_result(
        {"vllm": vllm_ok, "gateway": gw_ok},
        {"vllm": not vllm_ok, "gateway": not gw_ok},
        "ok" if vllm_ok else "unreachable", v_metrics, health, served,
        gw_metrics, 9499.0 if gw_ok else None, 1.0 if gw_ok else None)


class StoreCaseBase(unittest.TestCase):
    """Base: temp database per test + fake central clock."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        clock = FakeClock(9000.0)
        self.store = ClusterHistoryStore(
            os.path.join(self._tmp.name, "history.db"),
            retention_days=30.0, sample_interval_seconds=5.0,
            clock=clock)
        self.addCleanup(self.store.close)
        self.session = AggregatorSession()

    def record(self, payloads, now, **kw):
        """Record one real-session snapshot + default services sample."""
        snap = self.session.cycle(payloads, now, **kw)
        return self.store.record_snapshot(snap, sample_services())

    def rows(self, table):
        with closing(sqlite3.connect(str(self.store.path))) as conn:
            return conn.execute("SELECT * FROM %s" % table).fetchall()

    def count(self, table):
        return len(self.rows(table))


# --------------------------------------------------------------------------
# 1-2: initialization, schema and version
# --------------------------------------------------------------------------

class InitTests(StoreCaseBase):
    def test_fresh_database_initialization(self):
        st = self.store.status()
        self.assertEqual(st["schema_version"], SCHEMA_VERSION)
        for table in TABLES:
            self.assertEqual(st["tables"][table]["rows"], 0)
            self.assertIsNone(st["tables"][table]["first_ts"])
        with closing(sqlite3.connect(str(self.store.path))) as conn:
            self.assertEqual(
                conn.execute("PRAGMA journal_mode").fetchone()[0], "wal")
            self.assertEqual(conn.execute("PRAGMA user_version")
                             .fetchone()[0], SCHEMA_VERSION)
        # WAL side files created alongside the database file
        self.assertTrue(os.path.exists(str(self.store.path) + "-wal"))

    def test_schema_and_version_verification(self):
        with closing(sqlite3.connect(str(self.store.path))) as conn:
            tables = {row[0] for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertTrue(set(TABLES) <= tables)
            node_cols = {row[1] for row in conn.execute(
                "PRAGMA table_info(node_samples)")}
            self.assertTrue(set(NODE_METRICS) <= node_cols)
            svc_cols = {row[1] for row in conn.execute(
                "PRAGMA table_info(service_samples)")}
            self.assertTrue(set(SERVICE_METRICS) <= svc_cols)
            # timestamp-indexed (PK on ts / (ts, node_id))
            node_idx = list(conn.execute(
                "PRAGMA index_list(node_samples)"))
            self.assertTrue(any(row[2] == 1 for row in node_idx),
                            "node_samples should have a primary-key index")
            for metric in ("cpu_percent", "gpu_temp_c", "ollama_up",
                           "vllm_process_count"):
                self.assertIn(metric, NODE_METRIC_UNITS)
            for metric in SERVICE_METRICS:
                self.assertIn(metric, SERVICE_METRIC_UNITS)



# --------------------------------------------------------------------------
# 3-4: snapshot insertion
# --------------------------------------------------------------------------

class InsertTests(StoreCaseBase):
    def test_single_cluster_snapshot_insert(self):
        """One poll: one cluster row, one node row per node, one service row."""
        result = self.record(all_online_payloads(9000.0), now=9000.0)
        self.assertEqual(result, {"cluster": 1, "nodes": 3,
                                  "services": 1, "ts": 9000.0})
        self.assertEqual(self.count("cluster_samples"), 1)
        self.assertEqual(self.count("node_samples"), 3)
        self.assertEqual(self.count("service_samples"), 1)
        with closing(sqlite3.connect(str(self.store.path))) as conn:
            row = conn.execute(
                "SELECT ts, mode, healthy, transitioning, controller_ok, "
                "warning_count FROM cluster_samples").fetchone()
        self.assertEqual(row, (9000.0, "split", 1, 0, 1, 0))
        # services omitted entirely -> no service row
        result = self.store.record_snapshot(
            self.session.cycle(all_online_payloads(9050.0), 9050.0))
        self.assertEqual(result["services"], 0)
        self.assertEqual(self.count("service_samples"), 1)

    def test_three_node_snapshot_insert(self):
        """Three nodes: per-node metrics from the real sample are stored."""
        self.record(
            {"dgx01": telemetry("dgx01", split_metrics(cpu_percent=11.0,
                                                       ram_used_bytes=111),
                                timestamp=8999.0),
             "dgx02": telemetry("dgx02", split_metrics(cpu_percent=22.0,
                                                       ram_used_bytes=222),
                                timestamp=8999.0),
             "dgx03": telemetry("dgx03", split_metrics(cpu_percent=33.0,
                                                       ram_used_bytes=333),
                                timestamp=8999.0)},
            now=9000.0)
        with closing(sqlite3.connect(str(self.store.path))) as conn:
            got = {row[0]: row[1] for row in conn.execute(
                "SELECT node_id, cpu_percent FROM node_samples")}
            rams = {row[0]: row[1] for row in conn.execute(
                "SELECT node_id, ram_used_bytes FROM node_samples")}
            sample_ts = {row[0]: row[1] for row in conn.execute(
                "SELECT node_id, sample_ts FROM node_samples")}
        self.assertEqual(got, {"dgx01": 11.0, "dgx02": 22.0, "dgx03": 33.0})
        self.assertEqual(rams, {"dgx01": 111.0, "dgx02": 222.0,
                                "dgx03": 333.0})
        # central ts used for alignment; the node's own sample time kept
        for nid in NODE_IDS:
            with closing(sqlite3.connect(str(self.store.path))) as conn:
                ts = conn.execute(
                    "SELECT ts FROM node_samples WHERE node_id=?",
                    (nid,)).fetchone()[0]
            self.assertEqual(ts, 9000.0)
            self.assertEqual(sample_ts[nid], 8999.0)

    def test_partial_node_failure(self):
        """Online + error + stale + never nodes: distinct markers."""
        # First the unseen-node distinction, then a mixed snapshot after the
        # nodes have a known history (offline-with-retained-sample = "error").
        self.store.record_snapshot(
            self.session.cycle({"dgx01": telemetry(
                "dgx01", split_metrics(cpu_percent=1.0)),
                "dgx02": NodeFetchError("connection error to "
                                        "192.168.0.2:8165")},
                8995.0))
        self.record(
            all_online_payloads(8999.0), now=9000.0)
        self.store.record_snapshot(
            self.session.cycle({"dgx01": telemetry(
                "dgx01", split_metrics(cpu_percent=10.0)),
                "dgx02": NodeFetchError("connection error to "
                                        "192.168.0.2:8165"),
                "dgx03": telemetry("dgx03",
                                   split_metrics(cpu_percent=30.0),
                                   remote_stale=True)},
                9050.0))
        with closing(sqlite3.connect(str(self.store.path))) as conn:
            first = {row[0]: row[1] for row in conn.execute(
                "SELECT node_id, state FROM node_samples WHERE ts=8995.0")}
            states = {row[0]: (row[1], row[2]) for row in conn.execute(
                "SELECT node_id, state, reachable FROM node_samples "
                "WHERE ts=9050.0")}
            cpus = {row[0]: row[1] for row in conn.execute(
                "SELECT node_id, cpu_percent FROM node_samples "
                "WHERE ts=9050.0")}
        self.assertEqual(first["dgx01"], "online")
        self.assertEqual(first["dgx02"], "never")   # missing-node distinction
        self.assertEqual(states["dgx01"], ("online", 1))
        self.assertEqual(states["dgx02"][0], "error")
        self.assertEqual(states["dgx02"][1], 0)  # unreachable
        self.assertEqual(states["dgx03"][0], "stale")
        self.assertEqual(states["dgx03"][1], 1)  # endpoint still answers
        # only the fresh online node has numbers at ts=9050
        self.assertEqual(cpus["dgx01"], 10.0)
        self.assertIsNone(cpus["dgx02"])
        self.assertIsNone(cpus["dgx03"])

    def test_all_nodes_unavailable(self):
        payload = {nid: NodeFetchError("connection error to %s" % nid)
                   for nid in NODE_IDS}
        result = self.store.record_snapshot(
            self.session.cycle(payload, 9000.0), sample_services(gw_ok=False))
        self.assertEqual(result["nodes"], 3)
        with closing(sqlite3.connect(str(self.store.path))) as conn:
            rows = conn.execute(
                "SELECT node_id, state, reachable FROM node_samples"
            ).fetchall()
            nonnull = conn.execute(
                "SELECT COUNT(*) FROM node_samples WHERE cpu_percent IS NOT "
                "NULL").fetchone()[0]
        self.assertEqual({row[0]: row[2] for row in rows},
                         {nid: 0 for nid in NODE_IDS})
        self.assertTrue(all(row[1] in ("error", "stale", "never")
                            for row in rows))
        self.assertEqual(nonnull, 0)
        # missing nodes vs failed nodes stay distinguishable on replay:
        # after a later fresh cycle, the never-seen node differs from the
        # previously-seen one (stale marker vs online).
        self.store.record_snapshot(
            self.session.cycle(all_online_payloads(9050.0), 9050.0))
        with closing(sqlite3.connect(str(self.store.path))) as conn:
            s01 = conn.execute(
                "SELECT state FROM node_samples WHERE node_id='dgx01' AND "
                "ts=9050.0").fetchone()[0]
        self.assertEqual(s01, "online")

    def test_explicit_null_preservation(self):
        """A fresh sample whose metric is None stays NULL (never 0-filled)."""
        metrics = split_metrics()
        metrics["cpu_percent"] = None     # deliberately unavailable
        metrics.pop("gpu_temp_c", None)   # absent entirely
        self.record({"dgx01": telemetry(
            "dgx01", metrics,
            status="partial")}, now=9000.0)
        with closing(sqlite3.connect(str(self.store.path))) as conn:
            row = conn.execute(
                "SELECT cpu_percent, gpu_temp_c, ram_used_bytes, quality "
                "FROM node_samples").fetchone()
        self.assertIsNone(row[0])
        self.assertIsNone(row[1])
        self.assertEqual(row[2], 100.0)   # real values still stored
        self.assertEqual(row[3], "partial")  # collection quality preserved
        hist = self.store.node_history(
            ["cpu_percent", "ram_used_bytes", "gpu_temp_c"],
            9000.0, 9000.5, 10)
        series = hist["per_node"]["dgx01"]["series"]
        # last bucket holds the sample; the unavailable metrics count 0
        self.assertEqual(series["cpu_percent"]["count"][-1], 0)
        self.assertEqual(series["ram_used_bytes"]["count"][-1], 1)
        self.assertEqual(series["gpu_temp_c"]["count"][-1], 0)



# --------------------------------------------------------------------------
# 8: stale data must not become fresh history
# --------------------------------------------------------------------------

class StalePolicyTests(StoreCaseBase):
    def test_stale_sample_not_recorded_as_fresh(self):
        """A cached stale sample is a marker row, never new numbers."""
        # Baseline: dgx02 served a real reading, then goes stale and keeps
        # re-serving the same cached sample on every poll.
        base = split_metrics(cpu_percent=42.0)
        self.record({"dgx01": telemetry("dgx01",
                                        split_metrics(cpu_percent=1.0)),
                     "dgx02": telemetry("dgx02", dict(base),
                                        timestamp=8990.0),
                     "dgx03": telemetry("dgx03",
                                        split_metrics(cpu_percent=3.0),
                                        timestamp=8990.0)},
                    now=9000.0)
        for i in range(1, 5):
            t = 9000.0 + 5.0 * i
            self.record({"dgx01": telemetry("dgx01",
                                            split_metrics(cpu_percent=1.0)),
                         "dgx02": telemetry("dgx02", dict(base),
                                            remote_stale=True,
                                            timestamp=8990.0),
                         "dgx03": telemetry("dgx03",
                                            split_metrics(cpu_percent=3.0),
                                            timestamp=t - 1.0)},
                        now=t)
        with closing(sqlite3.connect(str(self.store.path))) as conn:
            fresh = conn.execute(
                "SELECT COUNT(*) FROM node_samples WHERE node_id='dgx02' "
                "AND cpu_percent IS NOT NULL").fetchone()[0]
            markers = conn.execute(
                "SELECT COUNT(*) FROM node_samples WHERE node_id='dgx02' "
                "AND state='stale' AND cpu_percent IS NULL").fetchone()[0]
            sample_ts = conn.execute(
                "SELECT DISTINCT sample_ts FROM node_samples "
                "WHERE node_id='dgx02' ORDER BY sample_ts").fetchall()
        # dgx02 has 5 polls but exactly ONE real measurement: 1 row keeps
        # numbers, the 4 stale polls are explicit NULL markers.
        self.assertEqual(fresh, 1)
        self.assertEqual(markers, 4)
        self.assertEqual([row[0] for row in sample_ts], [8990.0])
        # The stale markers preserve the last real measurement time, so the
        # reader can show "last seen 15 s ago" without a new reading.
        hist = self.store.node_history(["cpu_percent"], 9040.0, 9060.0, 10)
        self.assertEqual(hist["per_node"]["dgx02"]["series"]
                         ["cpu_percent"]["count"], [0, 0, 0, 0, 0])


# --------------------------------------------------------------------------
# 9: cluster-mode transitions
# --------------------------------------------------------------------------

class ModeTransitionTests(StoreCaseBase):
    def test_mode_transitions_recorded_verbatim(self):
        cycles = ((9000.0, "split", True, False),
                  (9005.0, "transitioning", False, True),
                  (9010.0, "transitioning", False, True),
                  (9015.0, "heavy-qwen", True, False),
                  (9020.0, "stop", True, True))
        for t, mode, healthy, transition in cycles:
            self.record(all_online_payloads(t - 1.0), now=t, mode=mode,
                        healthy=healthy, transitioning=transition)
        with closing(sqlite3.connect(str(self.store.path))) as conn:
            rows = conn.execute(
                "SELECT mode, healthy, transitioning, controller_ok FROM "
                "cluster_samples ORDER BY ts").fetchall()
        self.assertEqual([row[0] for row in rows],
                         ["split", "transitioning", "transitioning",
                          "heavy-qwen", "stop"])
        self.assertEqual([row[1] for row in rows], [1, 0, 0, 1, 1])
        self.assertEqual([row[2] for row in rows], [0, 1, 1, 0, 1])
        self.assertEqual([row[3] for row in rows], [1, 1, 1, 1, 1])
        # bucketed view: one entry per bucket, mode never averaged
        hist = self.store.cluster_history(8990.0, 9030.0, 10)
        self.assertIn("split", hist["series"]["mode"])
        self.assertIn("heavy-qwen", hist["series"]["mode"])
        # controller unavailable -> verbatim "unknown" mode, never guessed
        self.store.record_snapshot(
            self.session.cycle(all_online_payloads(9024.5), 9025.0,
                               controller_available=False),
            sample_services())
        with closing(sqlite3.connect(str(self.store.path))) as conn:
            row = conn.execute(
                "SELECT mode, controller_ok FROM cluster_samples "
                "WHERE ts=9025.0").fetchone()
        self.assertEqual(row, ("unknown", 0))

    def test_all_real_controller_modes_stored_and_queryable(self):
        """Every mode the Phase 15 controller can observe is stored verbatim
        and round-trips through the query API -- including the "unknown"
        controller-unavailable sentinel, which keeps healthy/transitioning as
        NULL (unknown), never a guessed value."""
        from cluster.history import KNOWN_CLUSTER_MODES
        # (mode, healthy, transitioning, controller available?)
        real_modes = (
            ("split",          True,  False, True),
            ("heavy-qwen",     True,  False, True),
            ("heavy-deepseek", True,  False, True),
            ("stop",           True,  False, True),
            ("degraded",       False, False, True),
            ("unreachable",    False, False, True),
            ("transitioning",  False, True,  True),
            ("unknown",        None,  None,  False),
        )
        start = 9000.0
        for i, (mode, healthy, transitioning, controller_ok) in enumerate(
                real_modes):
            t = start + 5.0 * i
            self.record(all_online_payloads(t - 1.0), now=t, mode=mode,
                        healthy=healthy, transitioning=transitioning,
                        controller_available=controller_ok)
        # raw rows: every recorded mode is present verbatim, in order
        with closing(sqlite3.connect(str(self.store.path))) as conn:
            rows = conn.execute(
                "SELECT mode, healthy, transitioning, controller_ok FROM "
                "cluster_samples ORDER BY ts").fetchall()
        self.assertEqual([r[0] for r in rows],
                         [m for m, *_ in real_modes])
        for r, (mode, healthy, transitioning, controller_ok) in zip(
                rows, real_modes):
            self.assertEqual(r[0], mode)
            self.assertEqual(r[1], 1 if healthy is True
                             else 0 if healthy is False else None)
            self.assertEqual(r[2], 1 if transitioning is True
                             else 0 if transitioning is False else None)
            self.assertEqual(r[3], 1 if controller_ok else 0)
        # the "unknown" row specifically carries NULL health booleans
        with closing(sqlite3.connect(str(self.store.path))) as conn:
            unk = conn.execute(
                "SELECT mode, healthy, transitioning, controller_ok FROM "
                "cluster_samples WHERE mode='unknown'").fetchone()
        self.assertEqual(unk, ("unknown", None, None, 0))
        # every mode is discoverable through the bucketed query API too
        hist = self.store.cluster_history(start, start + 5.0 * len(real_modes),
                                          len(real_modes))
        seen = [m for m in hist["series"]["mode"] if m is not None]
        for mode, *_ in real_modes:
            self.assertIn(mode, seen)
        # the documented vocabulary matches the controller's real set
        self.assertEqual(set(m for m, *_ in real_modes),
                         set(KNOWN_CLUSTER_MODES))


# --------------------------------------------------------------------------
# 10: vLLM / gateway service metrics
# --------------------------------------------------------------------------

class ServiceMetricsTests(StoreCaseBase):
    def test_vllm_and_gateway_metrics_stored(self):
        self.record(all_online_payloads(9000.0), now=9000.0)
        # vLLM online with counters; gateway healthy
        self.store.record_snapshot(
            self.session.cycle(all_online_payloads(9049.0), 9050.0),
            sample_services(vllm_ok=True, gw_ok=True))
        with closing(sqlite3.connect(str(self.store.path))) as conn:
            first = conn.execute(
                "SELECT vllm_available, vllm_stale, vllm_status, "
                "gw_available, gw_stale, gw_requests_total FROM "
                "service_samples WHERE ts=9000.0").fetchone()
            second = conn.execute(
                "SELECT vllm_available, vllm_up, vllm_healthy, "
                "vllm_running_reqs, vllm_waiting_reqs, vllm_kv_cache_usage, "
                "vllm_prompt_tokens_total, vllm_generation_tokens_total, "
                "vllm_served_model, gw_requests_total, gw_errors_total, "
                "gw_in_flight, gw_since_start_seconds, gw_latency_p50_ms, "
                "gw_latency_p95_ms, gw_latency_max_ms, gw_ts, gw_age_seconds "
                "FROM service_samples WHERE ts=9050.0").fetchone()
        # unavailable vLLM (non-heavy expectation) -> explicit 0/1 markers +
        # NULL counters, never zero-filled
        self.assertEqual(first, (0, 1, "unreachable", 1, 0, 1200.0))
        self.assertEqual(second, (1, 1, 1, 3.0, 0.0, 0.42, 1000.0, 250.0,
                                  "heavy-qwen", 1200.0, 2.0, 1.0, 3600.0,
                                  8.5, 90.0, 410.0, 9499.0, 1.0))
        hist = self.store.service_history(
            ["vllm_running_reqs", "gw_requests_total",
             "vllm_available", "gw_latency_p95_ms"], 9050.0, 9050.5, 10)
        self.assertEqual(hist["series"]["vllm_running_reqs"]["count"][-1], 1)
        self.assertEqual(hist["series"]["vllm_running_reqs"]["mean"][-1], 3.0)
        self.assertEqual(hist["series"]["gw_requests_total"]["mean"][-1],
                         1200.0)
        self.assertEqual(hist["series"]["gw_latency_p95_ms"]["max"][-1],
                         90.0)
        # availability flag: 0 while vLLM absent, 1 once up
        self.assertEqual(hist["series"]["vllm_available"]["mean"][-1], 1.0)
        prev = self.store.service_history(
            ["vllm_available"], 9000.0, 9000.5, 10)
        self.assertEqual(prev["series"]["vllm_available"]["mean"][-1], 0.0)
        self.assertEqual(hist["units"]["gw_latency_p95_ms"], "ms")


# --------------------------------------------------------------------------
# 11: duplicate samples
# --------------------------------------------------------------------------

class DuplicateTests(StoreCaseBase):
    def test_duplicate_sample_replacement(self):
        """Re-delivered same-timestamp snapshots replace, never stack."""
        snap = self.store.record_snapshot(
            self.session.cycle(all_online_payloads(8999.0), 9000.0),
            sample_services())
        self.assertEqual(snap, {"cluster": 1, "nodes": 3, "services": 1,
                                "ts": 9000.0})
        # identical re-delivery
        self.session.cycle(all_online_payloads(8999.0), 9000.0)
        self.store.record_snapshot(
            self.session.cycle(all_online_payloads(8999.0), 9000.0),
            sample_services())
        self.assertEqual(self.count("cluster_samples"), 1)
        self.assertEqual(self.count("node_samples"), 3)
        self.assertEqual(self.count("service_samples"), 1)
        # corrected re-submission of the same cycle replaces the values
        self.session.cycle({"dgx01": telemetry("dgx01",
                                               split_metrics(cpu_percent=9.0),
                                               timestamp=8999.0),
                            "dgx02": telemetry("dgx02",
                                               split_metrics(cpu_percent=2.0),
                                               timestamp=8999.0),
                            "dgx03": telemetry("dgx03",
                                               split_metrics(cpu_percent=3.0),
                                               timestamp=8999.0)}, 9000.0)
        self.store.record_snapshot(
            self.session.cycle({"dgx01": telemetry("dgx01",
                                                   split_metrics(
                                                       cpu_percent=9.0),
                                                   timestamp=8999.0),
                                "dgx02": telemetry("dgx02",
                                                   split_metrics(
                                                       cpu_percent=2.0),
                                                   timestamp=8999.0),
                                "dgx03": telemetry("dgx03",
                                                   split_metrics(
                                                       cpu_percent=3.0),
                                                   timestamp=8999.0)}, 9000.0))
        with closing(sqlite3.connect(str(self.store.path))) as conn:
            rows = conn.execute(
                "SELECT node_id, cpu_percent FROM node_samples ORDER BY "
                "node_id").fetchall()
        self.assertEqual(rows, [("dgx01", 9.0), ("dgx02", 2.0),
                                ("dgx03", 3.0)])
        self.assertEqual(self.count("node_samples"), 3)


# --------------------------------------------------------------------------
# 12: transaction rollback
# --------------------------------------------------------------------------

class RollbackTests(StoreCaseBase):
    def test_rollback_on_write_failure(self):
        """A failure mid-cycle leaves the database untouched (atomicity)."""
        before = {t: self.count(t) for t in TABLES}
        original = self.store._writer

        class FailingWriter:
            """Proxy: forwards to the real connection, fails mid-cycle."""

            def __init__(self, conn):
                self._conn = conn

            def execute(self, sql, params=()):
                if sql.startswith("INSERT OR REPLACE INTO node_samples"):
                    raise sqlite3.OperationalError(
                        "simulated disk failure")
                return self._conn.execute(sql, params)

            def __enter__(self):
                return self._conn.__enter__()

            def __exit__(self, *exc):
                return self._conn.__exit__(*exc)

            def close(self):
                return self._conn.close()

            def __getattr__(self, name):
                return getattr(self._conn, name)

        self.store._writer = FailingWriter(original)
        try:
            with self.assertRaises(sqlite3.OperationalError):
                self.record(all_online_payloads(8999.0), now=9000.0)
        finally:
            self.store._writer = original
        after = {t: self.count(t) for t in TABLES}
        self.assertEqual(before, after,
                         "a failed cycle must not leave partial rows")
        # the store remains usable after the failure
        self.record(all_online_payloads(8999.0), now=9000.0)
        self.assertEqual(self.count("node_samples"), 3)

    def test_invalid_snapshot_rejected_before_any_write(self):
        bad = {
            "schema_version": 1, "timestamp": 9000.0,
            "cluster": {"mode": "split", "healthy": True,
                        "transitioning": False, "controller": "ok"},
            "nodes": {"dgx99": {"state": "online"}}, "warnings": [],
        }
        with self.assertRaises(HistoryError):
            self.store.record_snapshot(bad)
        for table in TABLES:
            self.assertEqual(self.count(table), 0)
        bad_ts = {"schema_version": 1, "timestamp": "soon",
                  "cluster": {"mode": "split"}, "nodes": {"dgx01": {}},
                  "warnings": []}
        with self.assertRaises(HistoryError):
            self.store.record_snapshot(bad_ts)
        with self.assertRaises(HistoryError):
            self.store.record_snapshot({"schema_version": 9, "timestamp": 1.0,
                                        "cluster": {"mode": "stop"},
                                        "nodes": {"dgx01": {}},
                                        "warnings": []})
        for table in TABLES:
            self.assertEqual(self.count(table), 0)


# --------------------------------------------------------------------------
# 13: persistence across reopen
# --------------------------------------------------------------------------

class ReopenTests(StoreCaseBase):
    def test_persistence_across_reopen(self):
        self.record(all_online_payloads(8999.0), now=9000.0)
        self.store.close()
        reopened = ClusterHistoryStore(
            str(self.store.path), retention_days=30.0,
            clock=lambda: 9000.0)
        try:
            self.assertEqual(self.count("cluster_samples"), 1)
            self.assertEqual(self.count("node_samples"), 3)
            self.assertEqual(self.count("service_samples"), 1)
            hist = reopened.node_history(["cpu_percent"], 9000.0, 9000.5, 10)
            self.assertEqual(hist["per_node"]["dgx01"]
                             ["series"]["cpu_percent"]["mean"][-1], 5.0)
            # and it can keep recording after the reopen
            with closing(sqlite3.connect(str(self.store.path))) as conn:
                self.assertEqual(
                    conn.execute("PRAGMA user_version").fetchone()[0],
                    SCHEMA_VERSION)
        finally:
            reopened.close()


# --------------------------------------------------------------------------
# 14: retention
# --------------------------------------------------------------------------

def make_cycle_payload(ts, mode="split"):
    """One full real-snapshot at central time ``ts`` (fresh session)."""
    return AggregatorSession().cycle(all_online_payloads(ts - 1.0), ts,
                                     mode=mode)


class RetentionTests(StoreCaseBase):
    def _new_store(self, retention_days):
        return ClusterHistoryStore(
            os.path.join(self._tmp.name, "ret.db"),
            retention_days=retention_days, sample_interval_seconds=5.0,
            clock=lambda: self._now)

    def test_retention_at_boundary(self):
        """Rows strictly older than the window are pruned; boundary kept."""
        start = 1_000_000.0
        store = self._new_store(1.0)  # 1-day window is easier to reason about
        self.addCleanup(store.close)
        day = 86400.0
        store.record_snapshot(make_cycle_payload(start))             # old
        store.record_snapshot(make_cycle_payload(start + day - 1.0)) # old
        now = start + day + day
        store.record_snapshot(make_cycle_payload(now))               # new
        deleted = store.prune(now=now)
        self.assertEqual(deleted["deleted"], 8)  # 2 cycles x (1 cluster + 3 nodes)
        rows = store.status()["tables"]
        self.assertEqual(rows["cluster_samples"]["rows"], 1)
        self.assertEqual(rows["node_samples"]["rows"], 3)
        self.assertEqual(rows["cluster_samples"]["first_ts"], now)
        # a row exactly at the cutoff is NOT pruned (ts < cutoff only)
        now = start + day + 86400.0 + 1.0
        store.record_snapshot(make_cycle_payload(now))
        store.prune(now=now)
        self.assertEqual(store.status()["tables"]["cluster_samples"]["rows"],
                         2)

    def test_default_retention_is_30_days_and_prune_is_bounded(self):
        st = self.store.status()
        self.assertEqual(st["retention_days"], 30.0)
        self.record(all_online_payloads(8990.0), now=8995.0)
        self.record(all_online_payloads(8991.0), now=9000.0)
        deleted = self.store.prune(now=9000.0)  # nothing old enough
        self.assertEqual(
            deleted, {"deleted": 0, "now": 9000.0, "skipped": False})
        self.assertEqual(self.count("node_samples"), 6)

    def test_retention_days_configurable_and_enforced(self):
        store = self._new_store(2.0)
        self.addCleanup(store.close)
        now = 2_000_000.0
        old_ts = now - 50 * 3600.0        # ~2.08 days back
        new_ts = now - 5 * 3600.0
        store.record_snapshot(make_cycle_payload(old_ts))
        store.record_snapshot(make_cycle_payload(new_ts))
        store.record_snapshot(make_cycle_payload(new_ts))  # duplicate ts
        store.prune(now=now)
        rows = store.status()["tables"]["cluster_samples"]
        self.assertEqual(rows["rows"], 1)
        self.assertEqual(rows["first_ts"], new_ts)


# --------------------------------------------------------------------------
# Maintenance cadence (rate-limited prune/checkpoint)
# --------------------------------------------------------------------------

class MaintenanceCadenceTests(StoreCaseBase):
    """With maintenance_seconds set, per-cycle prune() calls do real work
    (delete + checkpoint) at most once per window, not every 15 s."""

    def _maint_store(self, seconds):
        store = ClusterHistoryStore(
            os.path.join(self._tmp.name, "maint.db"),
            retention_days=30.0, sample_interval_seconds=5.0,
            clock=FakeClock(1_900_000_000.0), maintenance_seconds=seconds)
        self.addCleanup(store.close)
        return store

    def _seed(self, store):
        """One old row (beyond retention) + one fresh row."""
        store.record_snapshot(make_cycle_payload(1_800_000_000.0))  # 111 d old
        store.record_snapshot(make_cycle_payload(1_900_000_000.0))  # fresh

    def test_maintenance_rate_limited_within_window(self):
        store = self._maint_store(3600.0)
        self._seed(store)
        now = 1_900_000_000.0
        first = store.prune(now=now)                 # first pass: real work
        self.assertEqual(first, {"deleted": 4, "now": now, "skipped": False})
        within = store.prune(now=now + 30.0)         # 30 s later: skipped
        self.assertEqual(within, {"deleted": 0,
                                  "now": now + 30.0, "skipped": True})
        # a skipped pass must not advance the maintenance timestamp
        late = store.prune(now=now + 3599.0)
        self.assertTrue(late["skipped"])
        # once the full window has elapsed, a pass runs again
        due = store.prune(now=now + 3600.0)
        self.assertEqual(due["skipped"], False)

    def test_maintenance_zero_means_every_call_runs(self):
        store = self._maint_store(0.0)               # legacy behaviour
        self._seed(store)
        now = 1_900_000_000.0
        for offset in (0.0, 1.0, 2.0):
            r = store.prune(now=now + offset)
            self.assertEqual(r["skipped"], False)

    def test_maintenance_validation(self):
        for bad in (-1.0, -0.5, "x", None, float("inf"), float("nan")):
            with self.assertRaises(HistoryError):
                ClusterHistoryStore(os.path.join(self._tmp.name, "bad.db"),
                                    maintenance_seconds=bad)


# --------------------------------------------------------------------------
# 15: min/mean/max downsampling
# --------------------------------------------------------------------------

class DownsampleTests(StoreCaseBase):
    def test_deterministic_values_per_bucket(self):
        """One sample per bucket: mean == min == max == the value."""
        for i in range(10):
            t = 9000.0 + 5.0 * i
            self.record({"dgx01": telemetry(
                "dgx01", split_metrics(cpu_percent=float(i)),
                timestamp=t - 1.0),
                "dgx02": telemetry("dgx02",
                                   split_metrics(cpu_percent=float(i) + 0.5),
                                   timestamp=t - 1.0)},
                now=t)
        hist = self.store.node_history(["cpu_percent"], 8995.0,
                                       9051.0, 100)
        for node, offset in (("dgx01", 0.0), ("dgx02", 0.5)):
            series = hist["per_node"][node]["series"]["cpu_percent"]
            values = [v for v in series["mean"] if v is not None]
            self.assertEqual(len(values), 10)
            self.assertEqual(values, [i + offset for i in range(10)])
            self.assertEqual(series["min"], series["mean"])
            self.assertEqual(series["max"], series["mean"])

    def test_multi_sample_bucket_stats(self):
        """Two samples in one bucket: exact min/mean/max."""
        self.record({"dgx01": telemetry(
            "dgx01", split_metrics(ram_used_bytes=10.0), timestamp=8998.0),
            "dgx02": telemetry("dgx02", split_metrics(ram_used_bytes=20.0),
                                timestamp=8998.0),
            "dgx03": telemetry("dgx03", split_metrics(ram_used_bytes=20.0),
                                timestamp=8998.0)}, now=9000.0)
        self.record({"dgx01": telemetry(
            "dgx01", split_metrics(ram_used_bytes=30.0), timestamp=9001.0),
            "dgx02": telemetry("dgx02", split_metrics(ram_used_bytes=40.0),
                                timestamp=9001.0),
            "dgx03": telemetry("dgx03", split_metrics(ram_used_bytes=40.0),
                                timestamp=9001.0)}, now=9005.0)
        # A 10-wide bucket (points=10 over a 100 s span) so both 9000 and
        # 9005 land in the same bucket: anchor 8990, bucket 1 = [9000,9010).
        hist = self.store.node_history(["ram_used_bytes"], 8995.0, 9095.0,
                                       10)
        series = hist["per_node"]["dgx01"]["series"]["ram_used_bytes"]
        self.assertEqual(hist["bucket_seconds"], 10.0)
        self.assertEqual(hist["bucket_seconds"] and series["count"][1], 2)
        self.assertEqual(series["mean"][1], 20.0)
        self.assertEqual(series["min"][1], 10.0)
        self.assertEqual(series["max"][1], 30.0)
        other = [c for i, c in enumerate(series["count"]) if i != 1]
        self.assertTrue(all(c == 0 for c in other))


# --------------------------------------------------------------------------
# 16: missing measurement gaps
# --------------------------------------------------------------------------

class GapTests(StoreCaseBase):
    def test_missing_measurements_are_gaps(self):
        """A node outage shows as zero-count NULL gaps, not zeros."""
        for t in (9000.0, 9005.0, 9010.0):
            self.record({"dgx01": telemetry(
                "dgx01", split_metrics(cpu_percent=7.0)),
                "dgx02": telemetry("dgx02",
                                   split_metrics(cpu_percent=7.0))},
                now=t, mode="split")
        # dgx01 goes down for the next three cycles
        for t in (9015.0, 9020.0, 9025.0):
            self.store.record_snapshot(
                self.session.cycle({"dgx01": NodeFetchError(
                    "connection error to 127.0.0.1:8165"),
                    "dgx02": telemetry("dgx02",
                                       split_metrics(cpu_percent=7.0))},
                    t))
        hist = self.store.node_history(["cpu_percent"], 8995.0,
                                       9030.0, 100)
        s01 = hist["per_node"]["dgx01"]["series"]["cpu_percent"]
        s02 = hist["per_node"]["dgx02"]["series"]["cpu_percent"]
        got01 = [(round(v, 4) if v is not None else None, c)
                 for v, c in zip(s01["mean"], s01["count"])]
        got02 = [(round(v, 4) if v is not None else None, c)
                 for v, c in zip(s02["mean"], s02["count"])]
        online = (7.0, 1)
        gap = (None, 0)
        # buckets (width 5, anchor 8995): pos0=[8995,9000) is a leading gap;
        # samples sit at pos1..pos6
        self.assertEqual(got01[1:4], [online, online, online])
        self.assertEqual(got01[4:7], [gap, gap, gap])
        self.assertEqual(got02[1:7], [online] * 6)
        # measured-value gaps (not row gaps): marker rows continue to exist
        # for the down node (row count stays 1) while measurements are null
        self.assertEqual(hist["sample_counts"]["dgx01"][4:7], [1, 1, 1])
        self.assertEqual(hist["sample_counts"]["dgx02"][4:7], [1, 1, 1])
        self.assertEqual(s01["count"][4:7], [0, 0, 0])
        self.assertEqual(s02["count"][4:7], [1, 1, 1])
        # the row-level marker explains the gap (downed node, not zero CPU)
        with closing(sqlite3.connect(str(self.store.path))) as conn:
            states = {row[0]: row[1] for row in conn.execute(
                "SELECT node_id, state FROM node_samples WHERE ts=9020.0")}
        self.assertEqual(states["dgx01"] in ("error", "stale"), True)
        self.assertEqual(states["dgx02"], "online")

# --------------------------------------------------------------------------
# 17: invalid metric names
# --------------------------------------------------------------------------

class InvalidMetricTests(StoreCaseBase):
    def test_invalid_metric_name_rejection(self):
        self.record(all_online_payloads(8999.0), now=9000.0)
        bad_names = [
            "cpu_percent; DROP TABLE node_samples",   # SQL injection
            "cpu_percent OR 1=1",                     # boolean injection
            "NODE_METRICS[0]",                        # arbitrary expression
            "CPU_PERCENT",                            # wrong case
            "gateway_requests_total",                 # looks plausible
            "gw_requests_total",                      # service, not node
            "vllm_up",                                # service, not node
            "metrics; --",
            "",
            5,
        ]
        for name in bad_names:
            with self.assertRaises(HistoryError, msg=repr(name)):
                self.store.node_history([name], 9000.0, 9000.5, 10)
        with self.assertRaises(HistoryError):
            self.store.service_history(["cpu_percent"], 9000.0, 9000.5, 10)
        with self.assertRaises(HistoryError):
            self.store.node_history([], 9000.0, 9000.5, 10)
        with self.assertRaises(HistoryError):
            self.store.node_history(["cpu_percent"], 9000.0, 9000.5, 10,
                                    nodes=["dgx99"])
        # nothing was touched by the rejected queries
        self.assertEqual(self.count("node_samples"), 3)
        # a valid name still works next to the rejected ones
        hist = self.store.node_history(["cpu_percent"], 9000.0, 9000.5, 10)
        self.assertEqual(hist["per_node"]["dgx01"]["series"]
                         ["cpu_percent"]["count"][-1], 1)

    def test_all_metric_names_are_queryable(self):
        """Every allowlisted name accepts and round-trips (allowlist is real)."""
        self.store.record_snapshot(
            self.session.cycle(all_online_payloads(8999.0), 9000.0),
            sample_services())
        hist = self.store.node_history(list(NODE_METRICS), 9000.0, 9000.5,
                                       10)
        self.assertEqual(set(hist["units"]), set(NODE_METRICS))
        svc = self.store.service_history(list(SERVICE_METRICS), 9000.0,
                                         9000.5, 10)
        self.assertEqual(set(svc["units"]), set(SERVICE_METRICS))


# --------------------------------------------------------------------------
# 18: bounded queries
# --------------------------------------------------------------------------

class BoundedQueryTests(StoreCaseBase):
    def test_reply_size_bounded_by_points(self):
        """Thousands of raw samples -> at most ~points reply entries."""
        n_samples = 240  # 20 minutes of 5 s samples
        for i in range(n_samples):
            t = 9000.0 + 5.0 * i
            self.store.record_snapshot(
                self.session.cycle(all_online_payloads(t - 1.0), t))
        self.assertEqual(self.count("node_samples"), n_samples * 3)
        for points in (10, 100, 1000):
            hist = self.store.node_history(["cpu_percent"], 9000.0,
                                           9000.0 + 5.0 * n_samples, points)
            for node in NODE_IDS:
                length = len(hist["per_node"][node]["series"]
                             ["cpu_percent"]["mean"])
                self.assertLessEqual(length, points + 2)
        svc = self.store.service_history(
            ["gw_requests_total"], 9000.0, 9000.0 + 5.0 * n_samples, 10)
        self.assertLessEqual(len(svc["series"]["gw_requests_total"]["mean"]),
                             12)
        cluster = self.store.cluster_history(9000.0,
                                             9000.0 + 5.0 * n_samples, 10)
        self.assertLessEqual(len(cluster["series"]["mode"]), 12)

    def test_points_clamped_to_bounds(self):
        for bad in (0.5, "100", [5]):
            with self.assertRaises(HistoryError):
                self.store.node_history(["cpu_percent"], 9000.0, 9000.5,
                                        points=bad)
        low = self.store.node_history(["cpu_percent"], 9000.0, 9000.5,
                                      points=1)
        self.assertEqual(len(low["per_node"]["dgx01"]
                            ["series"]["cpu_percent"]["mean"]), 1)
        high = self.store.node_history(["cpu_percent"], 9000.0, 9000.5,
                                       points=10 ** 9)
        self.assertTrue(high["per_node"]["dgx01"]
                        ["series"]["cpu_percent"]["mean"])

    def test_range_exceeding_retention_rejected(self):
        self.record(all_online_payloads(8999.0), now=9000.0)
        with self.assertRaises(HistoryError):
            # 32 days > 30-day retention + 1-day grace
            self.store.node_history(
                ["cpu_percent"], 0.0, 32 * 86400.0, 100)
        with self.assertRaises(HistoryError):
            self.store.cluster_history(9000.5, 9000.0, 10)
        # default range (no args) is always within retention
        hist = self.store.node_history(["cpu_percent"])
        self.assertLessEqual(hist["end"] - hist["start"],
                             30 * 86400.0 + 86400.0)


# --------------------------------------------------------------------------
# 19: schema-version incompatibility
# --------------------------------------------------------------------------

class SchemaVersionTests(StoreCaseBase):
    def test_incompatible_schema_version_refused(self):
        with closing(sqlite3.connect(str(self.store.path))) as conn:
            conn.execute("PRAGMA user_version=99")
        self.store.close()
        with self.assertRaises(HistoryError):
            ClusterHistoryStore(str(self.store.path))
        # a pristine (v0) database is still accepted as "new"
        fresh = os.path.join(self._tmp.name, "fresh.db")
        ok = ClusterHistoryStore(fresh, clock=lambda: 9000.0)
        self.assertEqual(ok.status()["schema_version"], SCHEMA_VERSION)
        ok.close()


# --------------------------------------------------------------------------
# 20: no sensitive fields stored
# --------------------------------------------------------------------------

class NoSensitiveDataTests(StoreCaseBase):
    def test_no_sensitive_fields_stored(self):
        """Credentials, Telegram ids, prompts, env vars, command lines and
        raw headers must never reach the database -- only validated numbers,
        states and small labels do."""
        secret_token = "SECRET-BEARER-TOKEN-abc123XYZ"
        telegram_id = 987654321
        prompt = "THIS_IS_A_SECRET_PROMPT do not store"
        response = "THIS_IS_A_SECRET_RESPONSE do not store"
        cmd_line = "./vllm --api-key " + secret_token + " --serve"
        header = "Authorization: Bearer " + secret_token
        # Stuff the sample details / components (the rich per-node state the
        # aggregator shows live) with everything sensitive.
        rich_details = {"ollama": {"daemon_pids": [42],
                                   "runners": [{"cmdline": cmd_line,
                                                "env": {"OPENAI_API_KEY":
                                                        secret_token},
                                                "prompt": prompt,
                                                "response": response}]},
                        "http": {"request_headers": {"Authorization":
                                                     header}},
                        "telegram": {"chat_id": telegram_id}}
        payload = {}
        for nid in NODE_IDS:
            sample = node_sample(nid, split_metrics(),
                                 timestamp=8999.0,
                                 extra={"details": rich_details,
                                        "components": {"processes":
                                                       {"details":
                                                        rich_details}}})
            payload[nid] = {"node": sample,
                            "server_time": 9000.0, "age_seconds": 1.0,
                            "stale": False, "collector_error": None}
        snap = self.session.cycle(payload, 9000.0, mode="split",
                                  healthy=True, transitioning=False)
        # the snapshot itself may carry sensitive content (it is live state);
        # the history must not.
        snap["warnings"] = ["node token " + secret_token + " rejected",
                            "prompt: " + prompt]
        snap["cluster"]["notes_telegram"] = "chat " + str(telegram_id)
        self.store.record_snapshot(snap)
        stored = ""
        for table in TABLES:
            with closing(sqlite3.connect(str(self.store.path))) as conn:
                for row in conn.execute("SELECT * FROM %s" % table):
                    for value in row:
                        if value is not None:
                            stored += str(value) + "|"
        blob = stored
        for needle in (secret_token, prompt, response, cmd_line, header,
                       str(telegram_id)):
            self.assertNotIn(needle, blob,
                             "sensitive content %r leaked into history"
                             % needle[:40])
        # schema-level: no column could ever carry a sensitive payload
        with closing(sqlite3.connect(str(self.store.path))) as conn:
            columns = set()
            for table in TABLES:
                columns.update(
                    row[1] for row in conn.execute(
                        "PRAGMA table_info(%s)" % table))
        sensitive_patterns = (r"\btoken\b", r"\bsecret\b", r"\bcredential\b",
                              r"\bprompt\b", r"\bresponse\b",
                              r"\bheader\b", r"\bpassword\b",
                              r"\btelegram\b", r"\bchat_id\b", r"\bcmdline\b",
                              r"\binventory\b")
        for column in columns:
            lowered = column.lower()
            for pattern in sensitive_patterns:
                self.assertIsNone(re.search(pattern, lowered),
                                  "column %r looks sensitive" % column)
        # byte-level scan of the database file as a final check
        with open(str(self.store.path), "rb") as fh:
            file_blob = fh.read()
        for needle in (secret_token, prompt, response):
            self.assertNotIn(needle.encode("utf-8"), file_blob)


if __name__ == "__main__":
    unittest.main()
