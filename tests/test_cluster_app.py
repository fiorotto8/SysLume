"""Tests for cluster.app (composition root: one runtime + one store + one API).

Verifies config validation, wiring (the app's API serves the runtime's own
store and static files), one complete collection cycle through the *real*
runtime worker (fake aggregator only -- no network, no controller), the JSON
API + static routes seen through the app, and the CLI entry point's error
path.  No production paths are touched; all databases/HTML are temporary.
"""

import json
import os
import time
import unittest
import urllib.error
import urllib.request

import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from cluster.app import ConfigError, build_app, ClusterApp, load_config, main
from cluster.history import ClusterHistoryStore
from cluster.runtime import ClusterRuntime

NOW = 1_760_000_000.0
NODE_IDS = ("dgx01", "dgx02", "dgx03")


def _snapshot(ts, mode="split"):
    nodes = {}
    for nid in NODE_IDS:
        nodes[nid] = {"state": "online", "reachable": True, "fresh": True,
                      "sample_fresh": True, "sample_stale": False,
                      "quality": "ok",
                      "sample": {"timestamp": ts - 1.0, "status": "ok",
                                 "metrics": {"cpu_percent": 12.5,
                                             "gpu_percent": 40.0,
                                             "mem_unified_used_bytes": 5e9,
                                             "mem_unified_total_bytes": 128e9}}}
    return {"schema_version": 1, "timestamp": ts,
            "cluster": {"mode": mode, "healthy": True, "transitioning": False,
                        "controller": "ok", "source": "test", "raw": None,
                        "error": None},
            "nodes": nodes, "warnings": [], "meta": {"total_ms": 1}}


def _services(ts):
    return {"availability": {"vllm": False, "gateway": True},
            "staleness": {"vllm": False, "gateway": False},
            "vllm": {"status": "inactive", "health": None,
                     "served_model": None, "metrics": {}},
            "gateway": {"ts": ts, "age_seconds": 1.0,
                        "metrics": {"requests_total": 42.0,
                                    "errors_total": 0.0, "in_flight": 0.0,
                                    "since_start_seconds": 100.0,
                                    "latency_ms": {"p50": 1.0, "p95": 2.0,
                                                   "max": 3.0}}}}


class FakeAggregator:
    """Stands in for ClusterAggregator: one deterministic snapshot, no I/O."""

    def __init__(self):
        self.calls = 0

    def collect(self):
        self.calls += 1
        return _snapshot(NOW)


def _app_cfg(tmpdir, port=0, static_dir=None):
    return {"bind": "127.0.0.1", "port": port,
            "database": os.path.join(tmpdir, "data", "monitor.db"),
            "retention_days": 30.0, "interval_seconds": 15.0,
            "static_dir": static_dir, "allowed_hostnames": None,
            "nodes": None, "controller": None}


class TestLoadConfig(unittest.TestCase):
    def test_valid_and_relative_paths(self):
        tmp = self.mkdtemp()
        path = os.path.join(tmp, "central.json")
        with open(path, "w") as fh:
            json.dump({"bind": "127.0.0.1", "port": 8765,
                       "database": "data/monitor.db", "static_dir": "webui",
                       "interval_seconds": 15}, fh)
        cfg = load_config(path)
        self.assertEqual(cfg["port"], 8765)
        self.assertTrue(str(cfg["database"]).endswith("data/monitor.db"))
        self.assertEqual(str(cfg["database"]),
                         os.path.abspath(os.path.join(tmp, "data", "monitor.db")))
        self.assertEqual(str(cfg["static_dir"]),
                         os.path.abspath(os.path.join(tmp, "webui")))
        self.assertEqual(cfg["interval_seconds"], 15.0)
        self.assertEqual(cfg["allowed_hostnames"], None)
        self.assertIsNone(cfg["nodes"])
        self.assertIsNone(cfg["controller"])

    def test_defaults(self):
        tmp = self.mkdtemp()
        path = os.path.join(tmp, "central.json")
        with open(path, "w") as fh:
            json.dump({}, fh)
        cfg = load_config(path)
        self.assertEqual(cfg["bind"], "127.0.0.1")
        self.assertEqual(cfg["port"], 8765)
        self.assertEqual(cfg["retention_days"], 30.0)
        self.assertEqual(cfg["static_dir"],
                         os.path.abspath(os.path.join(tmp, "static")))

    def test_rejects_bad_values(self):
        tmp = self.mkdtemp()

        def _case(payload, expect_msg):
            path = os.path.join(tmp, "bad.json")
            with open(path, "w") as fh:
                json.dump(payload, fh)
            with self.assertRaises(ConfigError) as ctx:
                load_config(path)
            self.assertIn(expect_msg, str(ctx.exception))

        _case({"bind": "0.0.0.0"}, "loopback")
        _case({"port": "8765"}, "port")
        _case({"port": 0}, "port")
        _case({"interval_seconds": 0.5}, "interval_seconds")
        _case({"allowed_hostnames": "localhost"}, "allowed_hostnames")
        _case({"retention_days": 0}, "retention_days")

    def test_missing_file_and_wrong_shape(self):
        with self.assertRaises(ConfigError):
            load_config("/nonexistent/central.json")
        tmp = self.mkdtemp()
        path = os.path.join(tmp, "array.json")
        with open(path, "w") as fh:
            json.dump([1, 2], fh)
        with self.assertRaises(ConfigError):
            load_config(path)

    def mkdtemp(self):
        import tempfile
        d = tempfile.TemporaryDirectory()
        self.addCleanup(d.cleanup)
        return d.name


class TestAppWiring(unittest.TestCase):
    def setUp(self):
        import tempfile
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmpdir = self._tmp.name
        os.makedirs(os.path.join(self.tmpdir, "data"))
        self.static_dir = os.path.join(self.tmpdir, "static")
        os.makedirs(self.static_dir)
        with open(os.path.join(self.static_dir, "cluster.html"), "w") as fh:
            fh.write("<!doctype html><title>SysLume Cluster</title>")
        with open(os.path.join(self.static_dir, "cluster.js"), "w") as fh:
            fh.write("window.AppTest = true;")

    def _build(self, port=0):
        cfg = _app_cfg(self.tmpdir, port=port, static_dir=self.static_dir)
        store = ClusterHistoryStore(cfg["database"],
                                    retention_days=cfg["retention_days"],
                                    sample_interval_seconds=15.0,
                                    clock=lambda: NOW)
        aggregator = FakeAggregator()
        runtime = ClusterRuntime(
            interval_seconds=15.0, store=store, aggregator=aggregator,
            service_metrics_fn=lambda snap: _services(snap["timestamp"]),
            clock=lambda: NOW)
        app = ClusterApp(cfg, runtime=runtime, store=store)
        self.aggregator = aggregator
        return app, runtime

    def _http(self, port, path, headers=None):
        url = "http://127.0.0.1:%d%s" % (port, path)
        try:
            with urllib.request.urlopen(
                    urllib.request.Request(url, headers=headers or {}),
                    timeout=10) as resp:
                return resp.status, resp.read()
        except urllib.error.HTTPError as exc:
            return exc.code, exc.read()

    def test_one_cycle_flow_end_to_end(self):
        app, runtime = self._build()
        port = app.start()
        self.addCleanup(app.stop)
        self.assertGreater(port, 0)
        try:
            deadline = time.time() + 10
            while runtime.latest() is None and time.time() < deadline:
                time.sleep(0.05)
            self.assertIsNotNone(runtime.latest(),
                                 "runtime did not complete a cycle")
            self.assertEqual(self.aggregator.calls, 1)

            code, body = self._http(port, "/healthz")
            self.assertEqual(code, 200)
            health = json.loads(body)
            self.assertEqual(health["status"], "ok")
            self.assertEqual(health["cycles"], 1)
            self.assertTrue(health["cache"]["has_snapshot"])
            self.assertTrue(health["running"])
            self.assertEqual(health["collection_errors"],
                             {"collect": None, "service": None,
                              "history": None, "prune": None})

            code, body = self._http(port, "/api/cluster/current")
            self.assertEqual(code, 200)
            current = json.loads(body)
            self.assertTrue(current["available"])
            self.assertEqual(current["snapshot"]["cluster"]["mode"], "split")
            # /api/cluster/current "service" is the flattened service_samples row
            self.assertEqual(current["service"]["gw_requests_total"], 42.0)
            self.assertEqual(current["service"]["gw_available"], 1)

            code, body = self._http(port, "/")
            self.assertEqual(code, 200)
            self.assertIn(b"SysLume Cluster", body)
            code, body = self._http(port, "/static/cluster.js")
            self.assertEqual(code, 200)
            self.assertEqual(body, b"window.AppTest = true;")

            code, body = self._http(port, "/api/history/cluster")
            self.assertEqual(code, 200)
            history = json.loads(body)
            self.assertIn("split", history["series"]["mode"])

            code, body = self._http(
                port, "/api/history/node?metrics=cpu_percent&nodes=dgx01")
            self.assertEqual(code, 200)
            history = json.loads(body)
            series = history["per_node"]["dgx01"]["series"]["cpu_percent"]
            self.assertIn(12.5, series["mean"])
            self.assertIn(1, series["count"])

            code, body = self._http(
                port, "/api/history/service?metrics=gw_requests_total")
            history = json.loads(body)
            self.assertEqual(code, 200)
            self.assertIn(42.0, history["series"]["gw_requests_total"]["mean"])
        finally:
            app.stop()
        self.assertFalse(runtime.is_running)

    def test_port_in_use_is_fatal(self):
        app, _ = self._build(port=8799)
        # Occupy the port first.
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("127.0.0.1", 8799))
        self.addCleanup(sock.close)
        with self.assertRaises(OSError):
            app.start()


class TestMain(unittest.TestCase):
    def test_missing_config_returns_1(self):
        self.assertEqual(main(["--config", "/nonexistent/central.json"]), 1)


if __name__ == "__main__":
    unittest.main()
