"""Tests for cluster.http_api (central read-only HTTP API, Phase 19B.1).

Deterministic by construction: a *fake* runtime exposes only the cached read
surface ``latest()`` / ``status()`` (it never collects), backed by a *temporary*
SQLite :class:`~cluster.history.ClusterHistoryStore`.  A real
:class:`~cluster.http_api.ClusterHttpApi` server runs on a loopback ephemeral
port.  No production database, no services, no collection, no controller.

Covers the 20 required scenarios: liveness (running/stopped), current snapshot
(present/absent), cluster status, node/service/cluster history, invalid metric
and node selection, invalid time ranges, excessive points, stale samples,
missing-data gaps, method/Host/Origin enforcement, concurrency, temporary
SQLite unavailability, "no collection" and "no mutation" guarantees, and JSON
serialization / sanitization (no NaN/Infinity, correct headers).
"""

import json
import math
import os
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from cluster.history import ClusterHistoryStore, NODE_IDS, NODE_METRICS, SERVICE_METRICS
from cluster.http_api import ApiError, ClusterHttpApi, MAX_POINTS

NOW = 1_700_000_000.0
ALL_NODES = list(NODE_IDS)


# --- snapshot / service builders (structurally valid for record_snapshot) --

def _cluster_block(mode, controller_ok=True):
    if controller_ok:
        return {"mode": mode, "healthy": True, "transitioning": False,
                "controller": "ok", "source": "test", "raw": None, "error": None}
    return {"mode": "unknown", "healthy": None, "transitioning": None,
            "controller": "error", "source": "test", "raw": None,
            "error": "test controller failure"}


def online_node(sample_ts, cpu=10.0, gpu=50.0, vllm=2.0):
    return {"state": "online", "reachable": True, "fresh": True,
            "sample_fresh": True, "sample_stale": False, "quality": "ok",
            "sample": {"timestamp": sample_ts, "status": "ok",
                       "metrics": {"cpu_percent": cpu, "gpu_percent": gpu,
                                   "vllm_process_count": vllm}}}


def stale_node(sample_ts):
    # Retained, explicitly-stale sample: metrics must NOT be re-recorded.
    return {"state": "stale", "reachable": True, "fresh": False,
            "sample_fresh": False, "sample_stale": True, "quality": "ok",
            "sample": {"timestamp": sample_ts, "status": "ok",
                       "metrics": {"cpu_percent": 99.0, "gpu_percent": 1.0}}}


def never_node():
    return {"state": "never", "reachable": False, "fresh": False,
            "sample_fresh": False, "sample_stale": False, "quality": None,
            "sample": None, "error": "no sample yet"}


def make_snapshot(ts, mode="split", online=True, controller_ok=True):
    nodes = {}
    for nid in ALL_NODES:
        if online:
            nodes[nid] = online_node(ts - 1.0)
        else:
            nodes[nid] = never_node()
    return {"schema_version": 1, "timestamp": ts,
            "cluster": _cluster_block(mode, controller_ok),
            "nodes": nodes, "warnings": [], "meta": {"total_ms": 3}}


def make_services(ts, gateway_requests=10.0):
    return {"availability": {"vllm": False, "gateway": True},
            "staleness": {"vllm": False, "gateway": False},
            "vllm": {"status": "inactive", "health": None,
                     "served_model": None, "metrics": {}},
            "gateway": {"ts": ts, "age_seconds": 1.0,
                        "metrics": {"requests_total": gateway_requests,
                                    "errors_total": 0.0, "in_flight": 0.0,
                                    "since_start_seconds": 100.0,
                                    "latency_ms": {"p50": 1.0, "p95": 2.0,
                                                   "max": 3.0}}}}


# --- fakes -----------------------------------------------------------------

class FakeRuntime:
    """Fake :class:`ClusterRuntime` exposing only the cached read surface.

    ``collect`` and ``_run_cycle`` are spy methods that record (and, with
    ``fail_on_collect``, raise on) any accidental call, so a test proves the
    API never triggers a collection or a cluster mutation.
    """

    def __init__(self, store, snapshot=None, published_at=None, running=False,
                 interval=15.0, fail_on_collect=True,
                 last_errors=None):
        self.store = store
        self._snapshot = snapshot
        self._published_at = published_at
        self._running = running
        self._interval = interval
        self._fail_on_collect = fail_on_collect
        self._last_errors = last_errors if last_errors is not None else {
            "collect": None, "service": None, "history": None, "prune": None}
        self.collect_calls = 0

    @property
    def is_running(self):
        return self._running

    def set_latest(self, snapshot, published_at):
        self._snapshot = snapshot
        self._published_at = published_at

    def set_running(self, value):
        self._running = bool(value)

    def latest(self):
        if self._snapshot is None:
            return None
        return {"snapshot": self._snapshot,
                "published_at": self._published_at, "cycle": 7}

    def status(self):
        return {"running": self._running, "interval_seconds": self._interval,
                "cycles": 7, "last_errors": dict(self._last_errors),
                "latest": None if self._snapshot is None
                          else {"published_at": self._published_at, "cycle": 7},
                "history": self.store.status()}

    # -- spies: the API must never call these -----------------------------
    def collect(self):
        self.collect_calls += 1
        if self._fail_on_collect:
            raise AssertionError("API triggered collection")

    _run_cycle = collect


class SpyStore:
    """Wraps a real store; any write method call is recorded + failed."""

    def __init__(self, real):
        self._real = real
        self.path = real.path
        self.write_calls = 0

    def _fail_write(self, *args, **kwargs):
        self.write_calls += 1
        raise AssertionError("store was written (mutation)")

    record_snapshot = _fail_write
    prune = _fail_write

    def node_history(self, *a, **k):
        return self._real.node_history(*a, **k)

    def service_history(self, *a, **k):
        return self._real.service_history(*a, **k)

    def cluster_history(self, *a, **k):
        return self._real.cluster_history(*a, **k)

    def status(self):
        return self._real.status()


class BrokenStore:
    """Simulates a temporarily-unavailable SQLite database."""

    def __init__(self, tmpdir):
        # A path that does not exist -> "unable to open database file".
        self.path = os.path.join(tmpdir, "missing.sqlite")

    def node_history(self, *a, **k):
        raise sqlite3.OperationalError("database is locked")

    def service_history(self, *a, **k):
        raise sqlite3.OperationalError("database is locked")

    def cluster_history(self, *a, **k):
        raise sqlite3.OperationalError("database is locked")

    def status(self):
        return {"available": False, "rows": 0}


class ApiTestCase(unittest.TestCase):
    """Base: temp SQLite store + fake runtime + real loopback HTTP API."""

    def setUp(self):
        self.now = NOW
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmpdir = self._tmp.name
        self.store = ClusterHistoryStore(
            os.path.join(self.tmpdir, "history.sqlite"), clock=lambda: self.now)
        self.rt = FakeRuntime(self.store)
        self.api = ClusterHttpApi(self.rt, clock=lambda: self.now)
        self.port = self.api.start()
        self.addCleanup(self.api.stop)
        self.headers_log = []

    # -- test helpers ------------------------------------------------------
    def get(self, path, headers=None, method="GET"):
        url = "http://127.0.0.1:%d%s" % (self.port, path)
        req = urllib.request.Request(url, method=method, headers=headers or {})
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status, resp.headers, resp.read()
        except urllib.error.HTTPError as exc:
            return exc.code, exc.headers, exc.read()

    def jget(self, path, headers=None, method="GET"):
        code, hdr, body = self.get(path, headers=headers, method=method)
        return code, hdr, json.loads(body.decode("utf-8"))

    def seed(self, entries, services_by_ts=None):
        """entries: list of (ts, snapshot); optional map ts -> services."""
        svc = services_by_ts or {}
        for ts, snapshot in entries:
            self.store.record_snapshot(snapshot, services=svc.get(ts))

    def row_counts(self, tables=("cluster_samples", "node_samples",
                                 "service_samples")):
        counts = {}
        for table in tables:
            conn = sqlite3.connect(
                self.store.path.as_uri() + "?mode=ro", uri=True)
            try:
                counts[table] = conn.execute(
                    "SELECT COUNT(*) FROM %s" % table).fetchone()[0]
            finally:
                conn.close()
        return counts


class TestHealth(ApiTestCase):
    def test_healthz_running(self):
        self.rt.set_running(True)
        self.rt.set_latest(make_snapshot(NOW), NOW - 3.0)   # fresh (age 3 <= 45)
        code, hdr, body = self.jget("/healthz")
        self.assertEqual(code, 200)
        self.assertEqual(body["service"], "up")
        self.assertTrue(body["running"])
        self.assertEqual(body["cache"]["has_snapshot"], True)
        self.assertEqual(body["cache"]["fresh"], True)
        self.assertIsInstance(body["cache"]["age_seconds"], float)
        self.assertEqual(body["last_cycle"]["cycle"], 7)
        self.assertIn("collect", body["collection_errors"])

    def test_healthz_stopped(self):
        self.rt.set_running(False)
        code, hdr, body = self.jget("/healthz")
        self.assertEqual(code, 200)
        self.assertEqual(body["running"], False)
        # Stopped and no snapshot yet: last_cycle is absent but graceful.
        self.assertIsNone(body["last_cycle"])
        self.assertEqual(body["cache"]["has_snapshot"], False)
        self.assertEqual(body["cache"]["fresh"], False)

    def test_healthz_stopped_with_snapshot(self):
        self.rt.set_running(False)
        self.rt.set_latest(make_snapshot(NOW), NOW - 3.0)
        code, hdr, body = self.jget("/healthz")
        self.assertEqual(code, 200)
        self.assertEqual(body["running"], False)
        self.assertEqual(body["last_cycle"]["cycle"], 7)  # still reports last
        self.assertEqual(body["cache"]["has_snapshot"], True)


class TestCurrent(ApiTestCase):
    def test_current_snapshot(self):
        snap = make_snapshot(NOW)
        services = make_services(NOW)
        self.seed([(NOW, snap)], {NOW: services})
        self.rt.set_latest(snap, NOW - 2.0)
        code, hdr, body = self.jget("/api/cluster/current")
        self.assertEqual(code, 200)
        self.assertTrue(body["available"])
        self.assertEqual(body["reason"], None)
        # Controller state + node status from the cached snapshot...
        self.assertEqual(body["snapshot"]["cluster"]["mode"], "split")
        self.assertEqual(body["snapshot"]["cluster"]["controller"], "ok")
        self.assertIn("dgx01", body["snapshot"]["nodes"])
        self.assertEqual(
            body["snapshot"]["nodes"]["dgx01"]["state"], "online")
        # ...and the latest service sample read from the history database.
        self.assertIsNotNone(body["service"])
        self.assertEqual(body["service"]["ts"], NOW)
        self.assertEqual(body["service"]["gw_requests_total"], 10.0)
        self.assertEqual(body["cycle"], 7)
        self.assertEqual(body["snapshot_age_seconds"], 2.0)

    def test_current_no_snapshot(self):
        code, hdr, body = self.jget("/api/cluster/current")
        self.assertEqual(code, 200)
        self.assertEqual(body["available"], False)
        self.assertEqual(body["reason"], "no_sample_yet")
        self.assertIsNone(body["snapshot"])
        self.assertIsNone(body["service"])


class TestStatusView(ApiTestCase):
    def test_cluster_status(self):
        self.rt.set_running(True)
        self.rt.set_latest(make_snapshot(NOW), NOW - 1.0)
        code, hdr, body = self.jget("/api/cluster/status")
        self.assertEqual(code, 200)
        # Mirrors ClusterRuntime.status() (sanitized), bounded and JSON-safe.
        self.assertTrue(body["running"])
        self.assertEqual(body["interval_seconds"], 15.0)
        self.assertIn("cycles", body)
        self.assertIn("last_errors", body)
        self.assertIn("history", body)
        self.assertIn("path", body["history"])
        # No node collector tokens / raw secrets leak into the status view.
        self.assertNotIn("token", json.dumps(body).lower())


class TestHistory(ApiTestCase):
    def _find_bucket(self, counts, value):
        for i, c in enumerate(counts):
            if c == value:
                return i
        return None

    def test_node_history(self):
        base = NOW - 1.0
        snaps = [(base, make_snapshot(base)),
                 (base + 0.1, make_snapshot(base + 0.1)),
                 (base + 0.2, make_snapshot(base + 0.2))]
        # Give the three samples distinct cpu values so min < mean < max.
        for (ts, snap), cpu in zip(snaps, (10.0, 20.0, 30.0)):
            snap["nodes"]["dgx01"] = online_node(ts, cpu=cpu)
        self.seed(snaps)
        code, hdr, body = self.jget(
            "/api/history/node?metrics=cpu_percent&nodes=dgx01"
            "&start=%.1f&end=%.1f&points=10" % (NOW - 3.0, NOW))
        self.assertEqual(code, 200)
        self.assertEqual(body["metrics"], ["cpu_percent"])
        self.assertEqual(body["nodes"], ["dgx01"])
        per = body["per_node"]["dgx01"]
        self.assertEqual(body["sample_count"], 3)
        idx = self._find_bucket(per["sample_counts"], 3)
        self.assertIsNotNone(idx)
        means = per["series"]["cpu_percent"]
        self.assertEqual(means["min"][idx], 10.0)
        self.assertEqual(means["max"][idx], 30.0)
        self.assertEqual(means["mean"][idx], 20.0)
        self.assertEqual(means["count"][idx], 3)
        self.assertEqual(body["units"], {"cpu_percent": "%"})

    def test_service_history(self):
        base = NOW - 2.0
        self.seed([(base, make_snapshot(base))],
                  {base: make_services(base, gateway_requests=42.0)})
        code, hdr, body = self.jget(
            "/api/history/service?metrics=gw_requests_total,gw_latency_p95_ms"
            "&start=%.1f&end=%.1f" % (NOW - 5.0, NOW))
        self.assertEqual(code, 200)
        self.assertEqual(body["metrics"], ["gw_requests_total",
                                           "gw_latency_p95_ms"])
        self.assertGreaterEqual(body["sample_count"], 1)
        counts = body["sample_counts"]
        idx = max(range(len(counts)), key=counts.__getitem__)
        self.assertEqual(body["sample_counts"][idx], 1)
        self.assertEqual(body["series"]["gw_requests_total"]["mean"][idx], 42.0)

    def test_cluster_mode_history(self):
        snaps = [(NOW - 20.0, make_snapshot(NOW - 20.0, mode="split")),
                 (NOW - 10.0, make_snapshot(NOW - 10.0, mode="heavy-qwen")),
                 (NOW - 5.0, make_snapshot(NOW - 5.0, mode="heavy-qwen"))]
        self.seed(snaps)
        code, hdr, body = self.jget(
            "/api/history/cluster?start=%.1f&end=%.1f" % (NOW - 25.0, NOW))
        self.assertEqual(code, 200)
        modes = set(m for m in body["series"]["mode"] if m is not None)
        self.assertEqual(modes, {"split", "heavy-qwen"})
        # Booleans are preserved (latest-observed per bucket), never averaged.
        self.assertTrue(all(v in (True, False, None)
                            for v in body["series"]["healthy"]))


class TestValidation(ApiTestCase):
    def test_invalid_metric_node(self):
        self.seed([(NOW, make_snapshot(NOW))])
        code, hdr, body = self.jget(
            "/api/history/node?metrics=bogus_metric&nodes=dgx01")
        self.assertEqual(code, 400)
        self.assertEqual(body["code"], "unknown_node_metric")

    def test_invalid_metric_service(self):
        self.seed([(NOW, make_snapshot(NOW))])
        code, hdr, body = self.jget(
            "/api/history/service?metrics=bogus_metric")
        self.assertEqual(code, 400)
        self.assertEqual(body["code"], "unknown_service_metric")

    def test_invalid_node_ids(self):
        self.seed([(NOW, make_snapshot(NOW))])
        code, hdr, body = self.jget(
            "/api/history/node?metrics=cpu_percent&nodes=dgx99")
        self.assertEqual(code, 400)
        self.assertEqual(body["code"], "unknown_node")
        self.assertEqual(body["details"]["value"], "dgx99")

    def test_missing_metric_param(self):
        code, hdr, body = self.jget("/api/history/node?nodes=dgx01")
        self.assertEqual(code, 400)
        self.assertEqual(body["code"], "missing_parameter")

    def test_invalid_time_range_end_before_start(self):
        self.seed([(NOW, make_snapshot(NOW))])
        code, hdr, body = self.jget(
            "/api/history/cluster?start=%.1f&end=%.1f" % (NOW, NOW - 60.0))
        self.assertEqual(code, 400)
        self.assertEqual(body["code"], "invalid_range")

    def test_invalid_time_range_non_numeric(self):
        code, hdr, body = self.jget(
            "/api/history/cluster?start=abc&end=%.1f" % NOW)
        self.assertEqual(code, 400)
        self.assertEqual(body["code"], "bad_parameter")

    def test_range_exceeds_retention(self):
        code, hdr, body = self.jget(
            "/api/history/cluster?start=%.1f&end=%.1f"
            % (NOW - 40 * 86400.0, NOW))   # beyond 30-day retention
        self.assertEqual(code, 400)
        self.assertEqual(body["code"], "invalid_range")

    def test_excessive_points_clamped(self):
        self.seed([(NOW - 10.0, make_snapshot(NOW - 10.0))])
        code, hdr, body = self.jget(
            "/api/history/cluster?start=%.1f&end=%.1f&points=1000000000000"
            % (NOW - 3600.0, NOW))
        self.assertEqual(code, 200)
        # Bounded reply regardless of the absurd request (store clamps to 5000).
        self.assertGreaterEqual(len(body["timestamps"]), 1)
        self.assertLessEqual(len(body["timestamps"]), MAX_POINTS)

    def test_excessive_points_invalid(self):
        code, hdr, body = self.jget(
            "/api/history/cluster?start=%.1f&end=%.1f&points=abc"
            % (NOW - 60.0, NOW))
        self.assertEqual(code, 400)
        self.assertEqual(body["code"], "bad_parameter")
        code, hdr, body = self.jget(
            "/api/history/cluster?start=%.1f&end=%.1f&points=0"
            % (NOW - 60.0, NOW))
        self.assertEqual(code, 400)
        self.assertEqual(body["code"], "bad_parameter")

    def test_sql_injection_metric_is_rejected(self):
        # An SQL-shaped metric name is just an unknown metric (never a query).
        self.seed([(NOW, make_snapshot(NOW))])
        before = self.row_counts()["node_samples"]
        self.assertGreater(before, 0)
        from urllib.parse import quote
        evil = "1); DROP TABLE node_samples; --"
        code, hdr, body = self.jget(
            "/api/history/node?metrics=%s\u0026nodes=dgx01" % quote(evil, safe=""))
        self.assertEqual(code, 400)
        self.assertEqual(body["code"], "unknown_node_metric")
        # Nothing was executed against the database: state unchanged.
        self.assertEqual(self.row_counts()["node_samples"], before)

class TestStalenessGaps(ApiTestCase):
    def mixed_snapshot(self, ts):
        snap = make_snapshot(ts)
        snap["nodes"]["dgx02"] = stale_node(ts - 1.0)
        snap["nodes"]["dgx03"] = never_node()
        return snap

    def test_stale_sample_current_representation(self):
        snap = self.mixed_snapshot(NOW - 2.0)
        self.rt.set_latest(snap, NOW - 2.0)
        code, hdr, body = self.jget("/api/cluster/current")
        self.assertEqual(code, 200)
        nodes = body["snapshot"]["nodes"]
        # Each node state is preserved verbatim; a dead node is never zero-filled.
        self.assertEqual(nodes["dgx01"]["state"], "online")
        self.assertEqual(nodes["dgx02"]["state"], "stale")
        self.assertEqual(nodes["dgx03"]["state"], "never")
        self.assertEqual(nodes["dgx03"]["reachable"], False)

    def test_stale_and_never_yields_null_history(self):
        snap = self.mixed_snapshot(NOW - 5.0)
        self.seed([(NOW - 5.0, snap)])
        code, hdr, body = self.jget(
            "/api/history/node?metrics=cpu_percent"
            "&nodes=dgx01,dgx02,dgx03&start=%.1f&end=%.1f"
            % (NOW - 12.0, NOW))
        self.assertEqual(code, 200)
        per = body["per_node"]
        # Fresh node has real measurements (a fresh sample is recorded).
        self.assertGreater(sum(per["dgx01"]["series"]["cpu_percent"]["count"]), 0)
        # Stale and never nodes: a marker row exists, but the measurement is a
        # gap -- per-metric count is 0 and every mean is NULL (no re-recording).
        for nid in ("dgx02", "dgx03"):
            series = per[nid]["series"]["cpu_percent"]
            self.assertEqual(series["count"], [0] * len(series["count"]))
            for value in series["mean"]:
                self.assertIsNone(value)
            # The marker row itself is still present in the row counter.
            self.assertGreaterEqual(sum(per[nid]["sample_counts"]), 1)

    def test_gaps_preserved_as_nulls(self):
        self.seed([(NOW - 20.0, make_snapshot(NOW - 20.0, mode="split")),
                   (NOW - 2.0, make_snapshot(NOW - 2.0, mode="heavy-qwen"))])
        code, hdr, body = self.jget(
            "/api/history/cluster?start=%.1f&end=%.1f" % (NOW - 30.0, NOW))
        self.assertEqual(code, 200)
        modes = body["series"]["mode"]
        self.assertIn("split", modes)
        self.assertIn("heavy-qwen", modes)
        # The gap between the two samples is an explicit None bucket, not a guess.
        self.assertIn(None, modes)
        self.assertEqual(len(body["timestamps"]), len(modes))


class TestMethodAndOrigin(ApiTestCase):
    def test_unsupported_methods_rejected(self):
        for method in ("POST", "PUT", "DELETE", "PATCH", "OPTIONS"):
            code, hdr, raw = self.get("/api/cluster/current", method=method)
            self.assertEqual(code, 405, method)
            self.assertEqual(hdr.get("Allow"), "GET, HEAD")
            body = json.loads(raw.decode("utf-8"))
            self.assertEqual(body["code"], "method_not_allowed")

    def test_unknown_path_404(self):
        code, hdr, body = self.jget("/does/not/exist")
        self.assertEqual(code, 404)
        self.assertEqual(body["code"], "not_found")

    def test_host_allowed_loopback_names(self):
        for host in ("127.0.0.1:%d" % self.port, "127.0.0.1", "localhost",
                     "localhost:%d" % self.port):
            code, hdr, raw = self.get("/healthz", headers={"Host": host})
            self.assertEqual(code, 200, host)

    def test_host_rejected_when_not_allowed(self):
        code, hdr, body = self.jget("/healthz", headers={"Host": "evil.example"})
        self.assertEqual(code, 403)
        self.assertEqual(body["code"], "host_not_allowed")

    def test_origin_rejected_when_cross_origin(self):
        code, hdr, body = self.jget(
            "/healthz", headers={"Host": "127.0.0.1:%d" % self.port,
                                 "Origin": "https://evil.example"})
        self.assertEqual(code, 403)
        self.assertEqual(body["code"], "origin_not_allowed")
        # No permissive CORS: we never advertise any origin.
        self.assertIsNone(hdr.get("Access-Control-Allow-Origin"))

    def test_custom_allowed_hostname(self):
        # A future Tailscale Serve hostname can be whitelisted without code change.
        api = ClusterHttpApi(self.rt,
                             allowed_hostnames=frozenset(
                                 {"127.0.0.1", "serve.example"}), port=0)
        try:
            port2 = api.start()
            req = urllib.request.Request(
                "http://127.0.0.1:%d/healthz" % port2,
                headers={"Host": "serve.example"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                self.assertEqual(resp.status, 200)
        finally:
            api.stop()


class TestConcurrency(ApiTestCase):
    def test_concurrent_requests(self):
        self.rt.set_running(True)
        self.seed([(NOW - 10.0, make_snapshot(NOW - 10.0)),
                   (NOW - 5.0, make_snapshot(NOW - 5.0))],
                  {NOW - 10.0: make_services(NOW - 10.0)})
        self.rt.set_latest(make_snapshot(NOW - 1.0), NOW - 1.0)
        routes = ["/healthz", "/api/cluster/current", "/api/cluster/status",
                  "/api/history/node?metrics=cpu_percent&nodes=dgx01",
                  "/api/history/service?metrics=gw_requests_total",
                  "/api/history/cluster"]
        errors = []
        barrier = threading.Barrier(12)

        def worker():
            try:
                barrier.wait(timeout=10)
                for _ in range(8):
                    for path in routes:
                        code, hdr, raw = self.get(path)
                        if code != 200:
                            errors.append((code, path, raw[:120]))
                        else:
                            json.loads(raw.decode("utf-8"))
            except Exception as exc:  # noqa: BLE001
                errors.append(("exception", repr(exc)))

        threads = [threading.Thread(target=worker) for _ in range(12)]
        for t in threads:
            t.start()
        deadline = time.time() + 40
        for t in threads:
            t.join(max(0.1, deadline - time.time()))
            self.assertFalse(t.is_alive(), "a worker did not finish (hang?)")
        self.assertEqual(errors, [], "concurrent errors: %r" % errors[:5])


class TestSqliteUnavailable(ApiTestCase):
    def test_history_queries_503_and_current_still_serves_cache(self):
        self.rt.set_running(True)
        self.seed([(NOW - 3.0, make_snapshot(NOW - 3.0))],
                  {NOW - 3.0: make_services(NOW - 3.0)})
        self.rt.set_latest(make_snapshot(NOW - 3.0), NOW - 3.0)
        # Now simulate the whole DB becoming temporarily unavailable.
        self.api._store = BrokenStore(self.tmpdir)

        code, hdr, body = self.jget(
            "/api/history/node?metrics=cpu_percent&nodes=dgx01")
        self.assertEqual(code, 503)
        self.assertEqual(body["code"], "history_unavailable")
        code, hdr, body = self.jget(
            "/api/history/cluster?start=%.1f&end=%.1f" % (NOW - 10.0, NOW))
        self.assertEqual(code, 503)
        self.assertEqual(body["code"], "history_unavailable")

        # The cached snapshot (from the runtime, not the DB) still serves.
        code, hdr, body = self.jget("/api/cluster/current")
        self.assertEqual(code, 200)
        self.assertTrue(body["available"])
        self.assertEqual(body["snapshot"]["cluster"]["controller"], "ok")
        self.assertIsNone(body["service"])
        self.assertIn("service_error", body)


class TestNoSideEffects(ApiTestCase):
    def setUp(self):
        super().setUp()
        self.spy = SpyStore(self.store)
        self.api._store = self.spy
        self.rt.set_running(True)

    def _prepare(self):
        snap = make_snapshot(NOW - 2.0)
        self.seed([(NOW - 2.0, snap)], {NOW - 2.0: make_services(NOW - 2.0)})
        self.rt.set_latest(snap, NOW - 2.0)

    def _run_requests(self):
        for path in ["/healthz", "/api/cluster/current", "/api/cluster/status",
                     "/api/history/node?metrics=cpu_percent",
                     "/api/history/service?metrics=gw_requests_total",
                     "/api/history/cluster"]:
            code, hdr, raw = self.get(path)
            self.assertEqual(code, 200, path)

    def test_no_collection_caused_by_http(self):
        self._prepare()
        self._run_requests()
        # The read surface is the only thing touched: collect/_run_cycle never ran.
        self.assertEqual(self.rt.collect_calls, 0)

    def test_no_cluster_mutations(self):
        self._prepare()
        before = self.row_counts()
        self._run_requests()
        after = self.row_counts()
        self.assertEqual(before, after)
        self.assertEqual(self.spy.write_calls, 0)


class TestSerialization(ApiTestCase):
    def test_no_nan_or_inf_and_correct_headers(self):
        snap = make_snapshot(NOW)
        snap["nodes"]["dgx01"]["sample"]["metrics"]["cpu_percent"] = float("nan")
        snap["nodes"]["dgx01"]["sample"]["metrics"]["weird"] = float("inf")
        self.rt.set_latest(snap, NOW)

        code, hdr, raw = self.get("/api/cluster/current")
        self.assertEqual(code, 200)
        self.assertEqual(hdr.get("Content-Type"),
                         "application/json; charset=utf-8")
        self.assertEqual(hdr.get("Cache-Control"), "no-store")
        self.assertEqual(int(hdr.get("Content-Length", "0")), len(raw))
        self.assertEqual(hdr.get("X-Content-Type-Options"), "nosniff")
        self.assertNotIn(b"NaN", raw)
        self.assertNotIn(b"Infinity", raw)

        # Strict parse: any non-finite constant would raise.
        def _reject_constant(token):
            raise ValueError("non-finite JSON token leaked: %s" % token)
        parsed = json.loads(raw.decode("utf-8"), parse_constant=_reject_constant)
        self.assertEqual(
            parsed["snapshot"]["nodes"]["dgx01"]["sample"]
            ["metrics"]["cpu_percent"], None)
        # Sanitization must not mutate the cached snapshot in place.
        self.assertTrue(math.isnan(
            snap["nodes"]["dgx01"]["sample"]["metrics"]["cpu_percent"]))

    def test_structured_error_shape(self):
        code, hdr, body = self.jget("/api/history/node?metrics=bogus")
        self.assertEqual(code, 400)
        self.assertEqual(hdr.get("Content-Type"),
                         "application/json; charset=utf-8")
        for key in ("status", "code", "message"):
            self.assertIn(key, body)
        self.assertEqual(body["status"], "error")


# --- optional static dashboard serving (static_root) -----------------------

class StaticServingTestCase(ApiTestCase):
    """Base for tests of the opt-in explicit static-file routes."""

    def setUp(self):
        self.now = NOW
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmpdir = self._tmp.name
        self.store = ClusterHistoryStore(
            os.path.join(self.tmpdir, "history.sqlite"), clock=lambda: self.now)
        self.rt = FakeRuntime(self.store)
        self.static_root = os.path.join(self.tmpdir, "static")
        os.makedirs(self.static_root)
        with open(os.path.join(self.static_root, "cluster.html"), "w") as fh:
            fh.write("<!doctype html><title>SysLume Cluster</title>MARKER_PAGE")
        for name, content in (
                ("cluster.js", "window.Marker = 1;"),
                ("style.css", ".x{color:red}"),
                (".env", "SECRET=1"),
                ("secret.db", "bytes\0bytes"),
        ):
            with open(os.path.join(self.static_root, name), "w") as fh:
                fh.write(content)
        os.makedirs(os.path.join(self.static_root, "sub"))
        with open(os.path.join(self.static_root, "sub", "deep.css"), "w") as fh:
            fh.write(".deep{color:blue}")
        self.api = ClusterHttpApi(self.rt, static_root=self.static_root,
                                  clock=lambda: self.now)
        self.port = self.api.start()
        self.addCleanup(self.api.stop)


class TestStaticServing(StaticServingTestCase):
    def test_root_serves_cluster_html(self):
        code, hdr, body = self.get("/")
        self.assertEqual(code, 200)
        self.assertEqual(hdr.get("Content-Type"), "text/html; charset=utf-8")
        self.assertEqual(hdr.get("Cache-Control"), "no-store")
        self.assertIn(b"SysLume Cluster", body)

    def test_head_root(self):
        code, hdr, body = self.get("/", method="HEAD")
        self.assertEqual(code, 200)
        self.assertEqual(body, b"")
        self.assertTrue(int(hdr.get("Content-Length", "0")) > 0)

    def test_static_file_types(self):
        code, hdr, body = self.get("/static/cluster.js")
        self.assertEqual(code, 200)
        self.assertEqual(hdr.get("Content-Type"),
                         "text/javascript; charset=utf-8")
        self.assertEqual(body, b"window.Marker = 1;")
        code, hdr, _ = self.get("/static/style.css")
        self.assertEqual(code, 200)
        self.assertEqual(hdr.get("Content-Type"), "text/css; charset=utf-8")

    def test_missing_and_disallowed_paths(self):
        code, _, raw = self.get("/static/missing.js")
        self.assertEqual(code, 404)
        self.assertEqual(json.loads(raw)["code"], "not_found")
        # hidden file, disallowed extension, sub-path: all JSON 404
        for path in ("/static/.env", "/static/secret.db", "/static/sub/deep.css"):
            code, _, raw = self.get(path)
            self.assertEqual(code, 404, path)
            self.assertEqual(json.loads(raw)["status"], "error", path)
        # traversal attempts
        for path in ("/static/../agg.py", "/static/%2e%2e/cluster.json"):
            code, _, _ = self.get(path)
            self.assertIn(code, (404, 400), path)

    def test_unknown_path_still_json_404(self):
        code, hdr, raw = self.get("/nope.html")
        self.assertEqual(code, 404)
        self.assertEqual(hdr.get("Content-Type"),
                         "application/json; charset=utf-8")
        self.assertEqual(json.loads(raw)["code"], "not_found")

    def test_api_routes_win_over_static(self):
        code, hdr, raw = self.get("/healthz")
        self.assertEqual(code, 200)
        self.assertEqual(hdr.get("Content-Type"),
                         "application/json; charset=utf-8")
        self.assertEqual(json.loads(raw)["status"], "ok")

    def test_json_only_without_static_root(self):
        api = ClusterHttpApi(self.rt, clock=lambda: self.now)
        port = api.start()
        self.addCleanup(api.stop)
        req = urllib.request.Request("http://127.0.0.1:%d/" % port)
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            urllib.request.urlopen(req, timeout=10)
        self.assertEqual(ctx.exception.code, 404)
        body = json.loads(ctx.exception.read().decode("utf-8"))
        self.assertEqual(body["code"], "not_found")

    def test_static_root_validation(self):
        with self.assertRaises(ValueError):
            ClusterHttpApi(self.rt, static_root="/nonexistent/dir",
                           clock=lambda: self.now)
        file_path = os.path.join(self.tmpdir, "afile")
        with open(file_path, "w") as fh:
            fh.write("x")
        with self.assertRaises(ValueError):
            ClusterHttpApi(self.rt, static_root=file_path,
                           clock=lambda: self.now)

    def test_static_serving_requires_allowed_host(self):
        code, hdr, raw = self.get("/", headers={"Host": "evil.example"})
        self.assertEqual(code, 403)
        self.assertEqual(json.loads(raw)["code"], "host_not_allowed")
