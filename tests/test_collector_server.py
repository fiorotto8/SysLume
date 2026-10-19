"""Tests for collectors/collector_server.py.

Covers the 20 required scenarios using an injected fake NodeCollector and a
loopback HTTP server on an ephemeral port.  No network access, no real
collector, no services.
"""

import json
import os
import subprocess
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request

from collectors.collector_server import (
    CollectorServer,
    CollectorServerConfigError,
    read_token_file,
    load_collector_config,
)

TOKEN = "cyg-secret-0123456789abcdef"


_token_counter = [0]


def make_token_file(tmpdir, content=TOKEN, mode=0o600):
    # unique name so the base_cfg default token never clobbers a test override
    _token_counter[0] += 1
    path = os.path.join(tmpdir, "token_%03d.txt" % _token_counter[0])
    with open(path, "w") as fh:
        fh.write(content)
    os.chmod(path, mode)
    return path


def base_cfg(tmpdir, **over):
    cfg = {
        "node_id": "dgx01",
        "bind": "127.0.0.1",
        "port": 0,
        "interval_seconds": 3600.0,   # large: loop stays quiet unless driven
        "stale_threshold_seconds": 30.0,
        "allowed_peers": ["127.0.0.1"],
        "token_file": make_token_file(tmpdir),
    }
    cfg.update(over)
    return cfg


def make_snapshot(node_id="dgx01", status="ok", metrics=None, warnings=None):
    """A NodeCollector-shaped snapshot (the exact 7 canonical keys)."""
    return {
        "schema_version": 1,
        "node_id": node_id,
        "timestamp": 1000.0,
        "status": status,
        "components": {"system": {"metrics": {"cpu_percent": metrics.get("cpu_percent")}}},
        "metrics": {} if metrics is None else metrics,
        "warnings": [] if warnings is None else warnings,
    }


class FakeClock:
    def __init__(self, start=1000.0):
        self.value = start

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += seconds


class FakeCollector:
    """Stand-in for NodeCollector with controlled samples, timing and errors."""

    def __init__(self, snapshots=None, node_id="dgx01", sleep=0.0,
                 fail_after=None, fail_exc=None):
        self.node_id = node_id
        self._snapshots = list(snapshots) if snapshots is not None else []
        self._sleep = sleep
        self.fail_after = fail_after
        self.fail_exc = fail_exc if fail_exc is not None else RuntimeError("boom")
        self.calls = 0
        self._depth = 0
        self.max_depth = 0
        self.default = make_snapshot(node_id=node_id,
                                     metrics={"cpu_percent": 7.0})

    def collect(self):
        self.calls += 1
        self._depth += 1
        self.max_depth = max(self.max_depth, self._depth)
        try:
            if self._sleep:
                time.sleep(self._sleep)
            if self.fail_after is not None and self.calls > self.fail_after:
                raise self.fail_exc
            if self._snapshots:
                # cycle through supplied snapshots, then hold the last one
                idx = min(self.calls - 1, len(self._snapshots) - 1)
                return self._snapshots[idx]
            return make_snapshot(node_id=self.node_id,
                                 metrics=dict(self.default["metrics"]))
        finally:
            self._depth -= 1


def http(method, url, token=None, data=None, timeout=5.0):
    req = urllib.request.Request(url, data=data, method=method)
    if token is not None:
        req.add_header("Authorization", "Bearer " + token)
    if data is not None:
        req.add_header("Content-Length", str(len(data)))
    try:
        resp = urllib.request.urlopen(req, timeout=timeout)
        try:
            return resp.status, resp.read().decode()
        finally:
            resp.close()
    except urllib.error.HTTPError as exc:
        try:
            return exc.code, exc.read().decode()
        finally:
            exc.close()


class ServerTestBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmpdir = self._tmp.name
        self.server = None

    def tearDown(self):
        if self.server is not None:
            self.server.stop(timeout=5.0)
        self._tmp.cleanup()

    def make_server(self, fake, cfg_over=None, start=True, clock=None,
                    sampler=True):
        cfg = base_cfg(self.tmpdir, **(cfg_over or {}))
        srv = CollectorServer(cfg, collector=fake, clock=clock)
        if start:
            if sampler:
                srv.start_sampler()
            srv.start_http()
        self.server = srv
        return srv

    def url(self, path):
        return "http://127.0.0.1:%d%s" % (self.server.bound_port(), path)


class HttpAuthAndRoutingTests(ServerTestBase):
    def _seeded_server(self, fake):
        srv = self.make_server(fake)
        fake.collect()  # prime one cached sample via explicit call
        srv.run_once()  # ensure the cache has a snapshot through the sampler
        return srv

    def test_1_authenticated_telemetry_200(self):
        fake = FakeCollector(snapshots=[make_snapshot(metrics={"cpu_percent": 3.25})])
        srv = self._seeded_server(fake)
        st, body = http("GET", self.url("/api/telemetry"), token=TOKEN)
        self.assertEqual(st, 200)
        data = json.loads(body)
        self.assertEqual(data["node"]["node_id"], "dgx01")
        self.assertEqual(data["node"]["metrics"]["cpu_percent"], 3.25)
        self.assertIs(data["stale"], False)
        self.assertIn("server_time", data)
        self.assertIn("age_seconds", data)
        self.assertIsNone(data["collector_error"])

    def test_2_missing_token_401(self):
        fake = FakeCollector()
        srv = self._seeded_server(fake)
        st, body = http("GET", self.url("/api/telemetry"))
        self.assertEqual(st, 401)
        self.assertNotIn(TOKEN, body)

    def test_3_invalid_token_401(self):
        fake = FakeCollector()
        srv = self._seeded_server(fake)
        st, body = http("GET", self.url("/api/telemetry"), token="not-the-token")
        self.assertEqual(st, 401)
        self.assertNotIn("not-the-token", body)

    def test_4_unauthorized_client_source_403(self):
        # Peer allow-list does NOT include our real loopback peer -> 403.
        fake = FakeCollector()
        srv = self.make_server(fake, cfg_over={"allowed_peers": ["192.168.99.99"]})
        srv.run_once()
        st, body = http("GET", self.url("/api/telemetry"), token=TOKEN)
        self.assertEqual(st, 403)
        st2, _ = http("GET", self.url("/healthz"))
        self.assertEqual(st2, 403)

    def test_5_unknown_route_404(self):
        fake = FakeCollector()
        srv = self._seeded_server(fake)
        st, _ = http("GET", self.url("/api/definitely-not"), token=TOKEN)
        self.assertEqual(st, 404)
        st2, _ = http("GET", self.url("/"), token=TOKEN)
        self.assertEqual(st2, 404)

    def test_6_unsupported_method_405(self):
        fake = FakeCollector()
        srv = self._seeded_server(fake)
        for method, data in [("POST", b"{}"), ("PUT", b""), ("DELETE", None),
                             ("PATCH", b"")]:
            st, _ = http(method, self.url("/api/telemetry"), token=TOKEN,
                         data=data)
            self.assertEqual(st, 405, "method %s should 405" % method)

    def test_18_no_secret_in_any_response(self):
        fake = FakeCollector()
        srv = self._seeded_server(fake)
        bodies = []
        for path, tok in [("/api/telemetry", TOKEN), ("/api/telemetry", None),
                          ("/api/telemetry", "bad"), ("/healthz", None),
                          ("/nope", TOKEN)]:
            _, body = http("GET", self.url(path), token=tok)
            bodies.append(body)
        for body in bodies:
            self.assertNotIn(TOKEN, body)
            self.assertNotIn("0123456789abcdef", body)

    def test_20_no_state_change_via_http(self):
        fake = FakeCollector()
        srv = self._seeded_server(fake)
        baseline = fake.calls
        for method, data in [("POST", b"{}"), ("PUT", b"{}"), ("DELETE", None),
                             ("PATCH", b"{}"), ("OPTIONS", None)]:
            st, _ = http(method, self.url("/api/telemetry"), token=TOKEN, data=data)
            self.assertEqual(st, 405)
        self.assertEqual(fake.calls, baseline)  # no collection triggered


class CacheAndSamplingStateTests(ServerTestBase):
    def test_7_cached_response_does_not_collect(self):
        fake = FakeCollector(snapshots=[make_snapshot(metrics={"cpu_percent": 1.0})])
        srv = self.make_server(fake, sampler=False)   # HTTP only, no loop
        srv.run_once()
        self.assertEqual(fake.calls, 1)
        for _ in range(5):
            st, _ = http("GET", self.url("/api/telemetry"), token=TOKEN)
            self.assertEqual(st, 200)
            st, _ = http("GET", self.url("/healthz"))
            self.assertEqual(st, 200)
        self.assertEqual(fake.calls, 1)  # reads never trigger collection

    def test_8_single_long_lived_collector(self):
        fake = FakeCollector()
        srv = self.make_server(fake, start=False)
        self.assertIs(srv.collector, fake)
        self.assertIs(srv._sampler._nc, fake)   # sampler samples the SAME object
        srv.run_once()
        self.assertEqual(fake.calls, 1)

    def test_9_first_sample_rate_stays_null(self):
        fake = FakeCollector(snapshots=[make_snapshot(metrics={"cpu_percent": None})])
        srv = self.make_server(fake, sampler=False)
        srv.run_once()
        st, body = http("GET", self.url("/api/telemetry"), token=TOKEN)
        self.assertEqual(st, 200)
        self.assertIsNone(json.loads(body)["node"]["metrics"]["cpu_percent"])

    def test_10_second_sample_rate_preserved(self):
        fake = FakeCollector(snapshots=[
            make_snapshot(metrics={"cpu_percent": None}),
            make_snapshot(metrics={"cpu_percent": 42.5}),
        ])
        srv = self.make_server(fake, start=False)
        srv.run_once()  # sample 1
        self.assertIsNone(srv.telemetry_payload()["node"]["metrics"]["cpu_percent"])
        srv.run_once()  # sample 2
        self.assertEqual(srv.telemetry_payload()["node"]["metrics"]["cpu_percent"],
                         42.5)

    def test_11_failure_retains_snapshot_and_marks_error(self):
        good = make_snapshot(metrics={"cpu_percent": 11.0})
        fake = FakeCollector(snapshots=[good, good, good], fail_after=2)
        srv = self.make_server(fake, start=False)
        srv.run_once()  # success
        srv.run_once()  # success
        payload = srv.telemetry_payload()
        self.assertEqual(payload["node"]["metrics"]["cpu_percent"], 11.0)
        self.assertIsNone(payload["collector_error"])
        srv.run_once()  # failure (calls > fail_after)
        payload = srv.telemetry_payload()
        self.assertEqual(payload["node"]["metrics"]["cpu_percent"], 11.0)  # retained
        self.assertIsNotNone(payload["collector_error"])
        self.assertIn("boom", payload["collector_error"])
        health = srv.health_payload()
        self.assertEqual(health["node_status"], "ok")   # last good snapshot kept
        self.assertGreater(health["failures_total"], 0)

    def test_12_no_sample_available(self):
        fake = FakeCollector()
        srv = self.make_server(fake, sampler=False)   # HTTP only, nothing sampled
        st, body = http("GET", self.url("/api/telemetry"), token=TOKEN)
        self.assertEqual(st, 200)
        data = json.loads(body)
        self.assertIsNone(data["node"])
        self.assertIsNone(data["age_seconds"])
        self.assertIs(data["stale"], True)
        health = json.loads(http("GET", self.url("/healthz"))[1])
        self.assertEqual(health["sample"], "missing")
        self.assertIsNone(health["node_status"])
        self.assertIs(health["stale"], True)

    def test_13_stale_threshold(self):
        clock = FakeClock(start=1000.0)
        fake = FakeCollector(snapshots=[make_snapshot(metrics={"cpu_percent": 2.0})])
        srv = self.make_server(fake, start=False, clock=clock)
        srv.run_once()   # success at clock == 1000.0
        self.assertIs(srv.telemetry_payload()["stale"], False)
        self.assertEqual(srv.health_payload()["sample"], "fresh")
        clock.advance(10.0)   # 10 s old < 30 s threshold
        self.assertIs(srv.telemetry_payload()["stale"], False)
        clock.advance(25.0)   # 35 s old > 30 s threshold
        self.assertIs(srv.telemetry_payload()["stale"], True)
        self.assertEqual(srv.health_payload()["sample"], "stale")
        self.assertAlmostEqual(srv.telemetry_payload()["age_seconds"], 35.0)


class PerformanceAndLifecycleTests(ServerTestBase):
    def test_14_slow_sampling_does_not_block_http(self):
        fake = FakeCollector(sleep=0.6)
        srv = self.make_server(fake, cfg_over={"interval_seconds": 0.05})
        time.sleep(0.15)   # let the (slow) sampler be active
        starts = []
        for _ in range(3):
            t0 = time.time()
            st, _ = http("GET", self.url("/api/telemetry"), token=TOKEN)
            starts.append(time.time() - t0)
            self.assertEqual(st, 200)
        self.assertLess(max(starts), 0.35)  # responses are fast, not blocked

    def test_15_no_overlapping_collections(self):
        fake = FakeCollector(sleep=0.02)
        srv = self.make_server(fake, cfg_over={"interval_seconds": 0.05})
        deadline = time.time() + 1.0
        def kick():
            while time.time() < deadline:
                srv.run_once()
                time.sleep(0.01)
        threads = [threading.Thread(target=kick) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertGreater(fake.calls, 5)
        self.assertEqual(fake.max_depth, 1)   # never two collections at once

    def test_16_clean_bounded_shutdown(self):
        fake = FakeCollector(sleep=0.1)
        srv = self.make_server(fake, cfg_over={"interval_seconds": 0.05})
        time.sleep(0.1)
        st, _ = http("GET", self.url("/healthz"))
        self.assertEqual(st, 200)
        port = srv.bound_port()
        t0 = time.time()
        srv.stop(timeout=5.0)
        elapsed = time.time() - t0
        self.assertLess(elapsed, 5.0)
        self.assertIsNone(srv._httpd)
        self.assertIsNone(srv._http_thread)
        self.assertFalse(srv._sampler.started())
        # and the listener is actually gone (connection refused)
        try:
            http("GET", "http://127.0.0.1:%d/healthz" % port, timeout=2.0)
            self.fail("expected connection refused after stop")
        except (urllib.error.URLError, ConnectionError, OSError):
            pass


class ConfigAndCredentialRejectionTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmpdir = self._tmp.name
        self.fake = FakeCollector()

    def tearDown(self):
        self._tmp.cleanup()

    def cfg(self, **over):
        c = base_cfg(self.tmpdir, **over)
        return c

    def test_17_rejects_invalid_bind(self):
        for bind in ["0.0.0.0", "192.168.50.9", "100.64.20.4", "999.1.1.1"]:
            with self.assertRaises(CollectorServerConfigError, msg=bind):
                CollectorServer(self.cfg(bind=bind), collector=self.fake)

    def test_17_rejects_node_bind_mismatch(self):
        with self.assertRaises(CollectorServerConfigError):
            CollectorServer(self.cfg(node_id="dgx02", bind="127.0.0.1"),
                            collector=self.fake)
        with self.assertRaises(CollectorServerConfigError):
            CollectorServer(self.cfg(node_id="dgx01", bind="192.168.0.2"),
                            collector=self.fake)

    def test_17_rejects_bad_port_and_intervals(self):
        for port in (70000, -5, "8080", True):
            with self.assertRaises(CollectorServerConfigError, msg=repr(port)):
                CollectorServer(self.cfg(port=port), collector=self.fake)
        # ephemeral 0 is accepted
        srv = CollectorServer(self.cfg(port=0), collector=self.fake)
        self.assertEqual(srv.config["port"], 0)
        for iv in (0, -1, 99999999):
            with self.assertRaises(CollectorServerConfigError, msg=repr(iv)):
                CollectorServer(self.cfg(interval_seconds=iv), collector=self.fake)
        with self.assertRaises(CollectorServerConfigError):
            CollectorServer(self.cfg(stale_threshold_seconds=0), collector=self.fake)

    def test_17_rejects_bad_peers(self):
        for peers in ([], ["not-an-ip"], ["0.0.0.0"], "127.0.0.1"):
            with self.assertRaises(CollectorServerConfigError, msg=repr(peers)):
                CollectorServer(self.cfg(allowed_peers=peers), collector=self.fake)

    def test_17_rejects_bad_token_file(self):
        # missing
        with self.assertRaises(CollectorServerConfigError):
            CollectorServer(self.cfg(token_file=os.path.join(self.tmpdir, "nope")),
                            collector=self.fake)
        # world-readable
        wt = make_token_file(self.tmpdir, mode=0o644)
        with self.assertRaises(CollectorServerConfigError):
            CollectorServer(self.cfg(token_file=wt), collector=self.fake)
        # empty
        et = make_token_file(self.tmpdir, content="   \n")
        with self.assertRaises(CollectorServerConfigError):
            CollectorServer(self.cfg(token_file=et), collector=self.fake)
        # unknown node_id
        with self.assertRaises(CollectorServerConfigError):
            CollectorServer(self.cfg(node_id="dgx99", bind="127.0.0.1"),
                            collector=self.fake)

    def test_17_load_collector_config_rejects_bad_file(self):
        with self.assertRaises(CollectorServerConfigError):
            load_collector_config(os.path.join(self.tmpdir, "missing.json"))
        bad = os.path.join(self.tmpdir, "bad.json")
        with open(bad, "w") as fh:
            fh.write("{ not json")
        with self.assertRaises(CollectorServerConfigError):
            load_collector_config(bad)
        # a valid file round-trips through the loader
        good = os.path.join(self.tmpdir, "good.json")
        with open(good, "w") as fh:
            fh.write(json.dumps(base_cfg(self.tmpdir, port=8165)))
        loaded = load_collector_config(good)
        self.assertEqual(loaded["port"], 8165)
        self.assertEqual(loaded["node_id"], "dgx01")


class SchemaPreservationTests(ServerTestBase):
    def test_19_node_schema_preserved_verbatim(self):
        snap = make_snapshot(node_id="dgx01", status="partial",
                             metrics={"cpu_percent": 9.9, "mem_bytes": 1},
                             warnings=["warn-a"])
        fake = FakeCollector(snapshots=[snap])
        srv = self.make_server(fake)
        srv.run_once()
        st, body = http("GET", self.url("/api/telemetry"), token=TOKEN)
        self.assertEqual(st, 200)
        data = json.loads(body)
        # wrapper keys exactly as specified
        self.assertEqual(
            set(data.keys()),
            {"node", "server_time", "age_seconds", "stale", "collector_error"})
        # node is the collector output, unchanged
        self.assertEqual(set(data["node"].keys()),
                         {"schema_version", "node_id", "timestamp", "status",
                          "components", "metrics", "warnings"})
        self.assertEqual(data["node"], snap)   # deep-equal, verbatim
        self.assertEqual(data["node"]["node_id"], "dgx01")
        self.assertEqual(data["node"]["status"], "partial")
        self.assertEqual(data["node"]["warnings"], ["warn-a"])
