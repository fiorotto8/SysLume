"""Tests for cluster.aggregator (central aggregation layer, first stage).

All three collector endpoints and the controller are mocked.  A small set of
tests drives the *real* default HTTP client (:func:`cluster.aggregator._http_get`)
and the *real* subprocess controller (:class:`ClusterModelController`) against
loopback fake servers / fake executables to verify bounded timeouts, bounded
sizes, a fixed read-only command, and ``shell=False``.

No remote collectors are started and the live cluster is never touched; the
only live (opt-in) test is skipped unless explicitly enabled.
"""

import json
import os
import sys
import tempfile
import threading
import time
import unittest
import warnings
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# ThreadingHTTPServer handler threads can raise a benign "unclosed socket"
# ResourceWarning after the server stops; keep the test output clean.
warnings.filterwarnings("ignore", category=ResourceWarning)

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from cluster import (
    ClusterAggregator,
    ClusterModelController,
    ControllerError,
    load_aggregator_config,
    read_node_token,
)
from cluster import aggregator as CA
from cluster.aggregator import NodeFetchError


# --------------------------------------------------------------------------
# Fixtures / fakes
# --------------------------------------------------------------------------

def split_metrics(**over):
    m = {"cpu_percent": 12.0, "ram_used_bytes": 100, "ram_total_bytes": 200,
         "ollama_up": 1, "ollama_daemon_count": 1, "ollama_runner_count": 1,
         "ollama_models_installed": 4, "ollama_models_resident": 2,
         "ray_process_count": 0, "vllm_process_count": 0,
         "vllm_engine_core_count": 0, "vllm_rank_worker_count": 0}
    m.update(over)
    return m


def heavy_metrics(**over):
    m = {"cpu_percent": 45.0, "ram_used_bytes": 900, "ram_total_bytes": 2000,
         "ollama_up": 0, "ollama_daemon_count": 0, "ollama_runner_count": 0,
         "ollama_models_installed": 4, "ollama_models_resident": 0,
         "ray_process_count": 2, "vllm_process_count": 2,
         "vllm_engine_core_count": 2, "vllm_rank_worker_count": 2}
    m.update(over)
    return m


def stop_metrics(**over):
    m = {"cpu_percent": 3.0, "ram_used_bytes": 50, "ram_total_bytes": 2000,
         "ollama_up": 0, "ollama_daemon_count": 0, "ollama_runner_count": 0,
         "ray_process_count": 0, "vllm_process_count": 0}
    m.update(over)
    return m


def node_sample(nid, metrics, status="ok", schema_version=1, node_id=None,
                timestamp=1000.0):
    return {
        "schema_version": schema_version,
        "node_id": node_id if node_id is not None else nid,
        "timestamp": timestamp,
        "status": status,
        "components": {},
        "metrics": metrics,
        "warnings": [],
    }


def telemetry(nid, metrics, status="ok", schema_version=1, node_id=None,
              timestamp=1000.0, remote_stale=False, collector_error=None):
    """The /api/telemetry *wrapper* returned by the node collector server."""
    return {
        "node": node_sample(nid, metrics, status=status,
                            schema_version=schema_version, node_id=node_id,
                            timestamp=timestamp),
        "server_time": timestamp + 1.0,
        "age_seconds": 1.0,
        "stale": remote_stale,
        "collector_error": collector_error,
    }


def controller_status(mode="split", healthy=True, transitioning=False, schema=1):
    return {
        "schema": schema, "ts": "2026-10-15T00:00:00Z", "mode": mode,
        "healthy": healthy, "transitioning": transitioning,
        "notes": [], "transition": None,
        "dgx01": {"ollama_up": True, "ollama_installed": ["a"]},
        "dgx02": {"ollama": {"service_active": mode == "split"},
                  "heavy": {"vllm_procs": 0}},
        "dgx03": {"ollama": {"service_active": mode == "split"},
                  "heavy": {"vllm_procs": 0}},
        "heavy_api": {"health": "000", "models": []},
        "ray": {"active": False, "gpus": 0, "healthy": False},
    }


def fail(msg="connection error"):
    return NodeFetchError(msg)


class FakeClock:
    def __init__(self, start=1000.0):
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


_HOST = {"dgx01": "127.0.0.1", "dgx02": "192.168.0.2", "dgx03": "192.168.2.2"}


def make_request_fn(payloads):
    """Build a request_fn mapping the default node URLs to supplied payloads.

    ``payloads`` maps node_id -> (wrapper dict | Exception instance).  A node
    missing from the map raises a generic connection error.
    """
    def fn(url, token, timeout):
        nid = None
        for node, host in _HOST.items():
            if host in url:
                nid = node
                break
        if nid is None:
            raise NodeFetchError("unknown url %s" % url)
        if nid not in payloads:
            raise NodeFetchError("connection error to %s" % host)
        value = payloads[nid]
        if isinstance(value, Exception):
            raise value
        return value
    return fn


AGG_KW = dict(fresh_threshold_seconds=15.0, request_timeout_seconds=2.0,
              collection_deadline_seconds=8.0)


def make_aggregator(request_fn, controller=None, clock=None, **kw):
    return ClusterAggregator(request_fn=request_fn, controller=controller,
                             clock=clock, **{**AGG_KW, **kw})


def split_aggregator(payloads=None, controller=None, clock=None, **kw):
    """An aggregator where all three nodes report healthy split telemetry."""
    payloads = payloads if payloads is not None else {
        "dgx01": telemetry("dgx01", split_metrics()),
        "dgx02": telemetry("dgx02", split_metrics(cpu_percent=9.0)),
        "dgx03": telemetry("dgx03", split_metrics(cpu_percent=11.0)),
    }
    if controller is None:
        controller = FakeController(controller_status())
    return make_aggregator(make_request_fn(payloads), controller=controller,
                           clock=clock, **kw)


class AggBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmpdir = self._tmp.name

    def tearDown(self):
        self._tmp.cleanup()


class HealthySplitTests(AggBase):
    def test_all_nodes_fresh_healthy_split(self):
        clock = FakeClock(start=1000.0)
        agg = split_aggregator(clock=clock)
        snap = agg.collect()
        self.assertEqual(snap["schema_version"], CA.AGGREGATOR_SCHEMA_VERSION)
        self.assertEqual(set(snap.keys()),
                         {"schema_version", "timestamp", "cluster", "nodes",
                          "warnings", "meta"})
        self.assertEqual(snap["timestamp"], 1000.0)
        # Cluster block comes from the controller, verbatim.
        self.assertEqual(snap["cluster"]["mode"], "split")
        self.assertIs(snap["cluster"]["healthy"], True)
        self.assertIs(snap["cluster"]["transitioning"], False)
        self.assertEqual(snap["cluster"]["controller"], "ok")
        self.assertEqual(snap["warnings"], [])
        # All three nodes online, fresh, with their own sample.
        self.assertEqual(set(snap["nodes"].keys()), {"dgx01", "dgx02", "dgx03"})
        for nid in ("dgx01", "dgx02", "dgx03"):
            n = snap["nodes"][nid]
            self.assertEqual(n["state"], "online", nid)
            self.assertIs(n["reachable"], True, nid)
            self.assertIs(n["fresh"], True, nid)
            self.assertIs(n["sample_stale"], False, nid)
            self.assertEqual(n["quality"], "ok", nid)
            self.assertIs(n["schema_valid"], True, nid)
            self.assertIs(n["identity_valid"], True, nid)
            self.assertEqual(n["age_seconds"], 0.0, nid)
            self.assertIsNotNone(n["sample"], nid)
        # Each worker node's own metrics are present and correct.
        self.assertEqual(snap["nodes"]["dgx01"]["sample"]["metrics"]
                         ["cpu_percent"], 12.0)
        self.assertEqual(snap["nodes"]["dgx02"]["sample"]["metrics"]
                         ["cpu_percent"], 9.0)

    def test_workers_expected_in_split_dgxy1_not_judged(self):
        snap = split_aggregator().collect()
        for nid in ("dgx02", "dgx03"):
            n = snap["nodes"][nid]
            self.assertEqual(n["as_expected"], True, nid)
            self.assertEqual(n["mode_expected"],
                             {"ollama": "active", "heavy": "inactive"}, nid)
        # DGX01 is the control node: not judged against a worker expectation.
        self.assertIsNone(snap["nodes"]["dgx01"]["as_expected"])
        self.assertEqual(snap["nodes"]["dgx01"]["mode_expected"],
                         {"ollama": "any", "heavy": "any"})

    def test_node_metrics_not_flattened_across_nodes(self):
        snap = split_aggregator().collect()
        self.assertNotIn("metrics", snap)          # no merged namespace
        self.assertNotIn("metrics", snap["cluster"])
        for nid in ("dgx01", "dgx02", "dgx03"):
            # each sample keeps its own node identity and metric namespace
            self.assertEqual(snap["nodes"][nid]["sample"]["node_id"], nid)
            self.assertIn("metrics", snap["nodes"][nid]["sample"])
        # distinct per-node values prove no cross-node bleed
        self.assertNotEqual(
            snap["nodes"]["dgx01"]["sample"]["metrics"]["cpu_percent"],
            snap["nodes"]["dgx02"]["sample"]["metrics"]["cpu_percent"])


class NodeFailureTests(AggBase):
    def test_one_node_unreachable_others_unaffected(self):      # scenario 2
        payloads = {
            "dgx01": telemetry("dgx01", split_metrics()),
            "dgx02": fail("connection error to 192.168.0.2: [Errno 111]"),
            "dgx03": telemetry("dgx03", split_metrics()),
        }
        snap = make_aggregator(make_request_fn(payloads),
                               controller=FakeController(controller_status())).collect()
        self.assertEqual(snap["nodes"]["dgx01"]["state"], "online")
        self.assertEqual(snap["nodes"]["dgx03"]["state"], "online")
        d = snap["nodes"]["dgx02"]
        self.assertEqual(d["state"], "never")
        self.assertIs(d["reachable"], False)
        self.assertIs(d["fresh"], False)
        self.assertIsNone(d["sample"])          # never zero-filled
        self.assertIsNotNone(d["error"])
        self.assertTrue(any(w.startswith("dgx02") for w in snap["warnings"]))
        # the cluster controller still read fine
        self.assertEqual(snap["cluster"]["mode"], "split")

    def test_all_nodes_unreachable(self):                           # scenario 3
        payloads = {nid: fail("down") for nid in ("dgx01", "dgx02", "dgx03")}
        snap = make_aggregator(make_request_fn(payloads),
                               controller=FakeController(controller_status())).collect()
        for nid in ("dgx01", "dgx02", "dgx03"):
            self.assertEqual(snap["nodes"][nid]["state"], "never")
            self.assertIsNone(snap["nodes"][nid]["sample"])
            self.assertEqual(snap["nodes"][nid]["reachable"], False)
        self.assertEqual(len(snap["warnings"]), 3)
        self.assertEqual(snap["cluster"]["mode"], "split")  # controller ok

    def test_malformed_telemetry_payload(self):                     # scenario 4
        # (a) payload that is not a JSON object
        payloads = {
            "dgx01": telemetry("dgx01", split_metrics()),
            "dgx02": [1, 2, 3],           # a list, not an object
            "dgx03": telemetry("dgx03", split_metrics()),
        }
        snap = make_aggregator(make_request_fn(payloads),
                               controller=FakeController(controller_status())).collect()
        d = snap["nodes"]["dgx02"]
        self.assertEqual(d["state"], "never")
        self.assertIs(d["reachable"], True)      # transport ok, content bad
        self.assertIs(d["schema_valid"], False)  # top-level not an object
        self.assertIsNone(d["sample"])
        self.assertTrue(any("dgx02" in w for w in snap["warnings"]))
        self.assertEqual(snap["nodes"]["dgx01"]["state"], "online")
        # (b) object payload missing the 'node' snapshot
        payloads_b = {
            "dgx01": {"server_time": 1, "stale": False},   # no 'node'
            "dgx02": telemetry("dgx02", split_metrics()),
            "dgx03": telemetry("dgx03", split_metrics()),
        }
        snap_b = make_aggregator(make_request_fn(payloads_b),
                                 controller=FakeController(controller_status())).collect()
        self.assertEqual(snap_b["nodes"]["dgx01"]["state"], "never")
        self.assertIs(snap_b["nodes"]["dgx01"]["reachable"], True)
        self.assertIs(snap_b["nodes"]["dgx01"]["schema_valid"], False)
        self.assertIsNone(snap_b["nodes"]["dgx01"]["sample"])

    def test_wrong_node_identity_rejected(self):                    # scenario 5
        payloads = {
            "dgx01": telemetry("dgx01", split_metrics()),
            # dgx02 endpoint returns a snapshot claiming to be dgx03
            "dgx02": telemetry("dgx02", split_metrics(), node_id="dgx03"),
            "dgx03": telemetry("dgx03", split_metrics()),
        }
        snap = make_aggregator(make_request_fn(payloads),
                               controller=FakeController(controller_status())).collect()
        d = snap["nodes"]["dgx02"]
        self.assertIs(d["reachable"], True)
        self.assertIs(d["identity_valid"], False)
        self.assertEqual(d["state"], "never")
        self.assertIsNone(d["sample"])
        self.assertTrue(any("mismatch" in w for w in snap["warnings"]))

    def test_unsupported_schema_version_rejected(self):             # scenario 6
        payloads = {
            "dgx01": telemetry("dgx01", split_metrics(), schema_version=99),
            "dgx02": telemetry("dgx02", split_metrics()),
            "dgx03": telemetry("dgx03", split_metrics()),
        }
        snap = make_aggregator(make_request_fn(payloads),
                               controller=FakeController(controller_status())).collect()
        d = snap["nodes"]["dgx01"]
        self.assertIs(d["reachable"], True)
        self.assertIs(d["schema_valid"], False)
        self.assertEqual(d["state"], "never")
        self.assertIsNone(d["sample"])
        self.assertTrue(any("schema_version" in w for w in snap["warnings"]))


class StaleAndRetentionTests(AggBase):
    def _stateful(self, clock, fresh=15.0):
        """One aggregator + request_fn whose per-node up/down is mutable."""
        down = set()

        def rf(url, token, timeout):
            for nid, host in _HOST.items():
                if host in url:
                    if nid in down:
                        raise NodeFetchError("connection error: down")
                    return telemetry(nid, split_metrics())
            raise NodeFetchError("unknown url")

        agg = make_aggregator(rf, controller=FakeController(controller_status()),
                              clock=clock, fresh_threshold_seconds=fresh)
        return agg, down

    def test_stale_node_sample_past_fresh_window(self):               # scenario 7
        clock = FakeClock(start=1000.0)
        agg, down = self._stateful(clock, fresh=60.0)
        s0 = agg.collect()
        self.assertEqual(s0["nodes"]["dgx02"]["state"], "online")
        down.update(["dgx01", "dgx02", "dgx03"])   # kill all fetches
        clock.advance(120.0)                       # beyond the 60 s window
        s1 = agg.collect()
        d = s1["nodes"]["dgx02"]
        self.assertEqual(d["state"], "error")          # older than window
        self.assertEqual(d["age_seconds"], 120.0)
        self.assertIs(d["sample_stale"], True)
        self.assertTrue(any("dgx02: telemetry error" in w for w in s1["warnings"]))

    def test_recent_down_is_stale_not_error(self):
        clock = FakeClock(start=1000.0)
        agg, down = self._stateful(clock, fresh=60.0)
        agg.collect()                                    # good sample at t=1000
        down.update(["dgx01", "dgx02", "dgx03"])
        clock.advance(20.0)                    # within window -> 'stale' (not 'error')
        s = agg.collect()
        self.assertEqual(s["nodes"]["dgx02"]["state"], "stale")
        self.assertEqual(s["nodes"]["dgx02"]["age_seconds"], 20.0)

    def test_previous_sample_retained_but_explicitly_stale(self):    # scenario 8
        clock = FakeClock(start=1000.0)
        down = set()

        def rf(url, token, timeout):
            for nid, host in _HOST.items():
                if host in url:
                    if nid in down:
                        raise NodeFetchError("connection reset")
                    return telemetry(nid, split_metrics(
                        cpu_percent={"dgx02": 9.0}.get(nid, 5.0)))
            raise NodeFetchError("unknown url")

        agg = make_aggregator(rf, controller=FakeController(controller_status()),
                              clock=clock)
        s0 = agg.collect()               # dgx02 online, cpu 9.0
        self.assertEqual(s0["nodes"]["dgx02"]["sample"]["metrics"]["cpu_percent"], 9.0)
        down.add("dgx02")               # only dgx02 down
        s1 = agg.collect()
        d = s1["nodes"]["dgx02"]
        self.assertEqual(d["state"], "stale")          # not 'never'
        self.assertIs(d["sample_stale"], True)         # explicitly stale
        self.assertIs(d["fresh"], False)
        self.assertIsNotNone(d["sample"])              # retained
        self.assertEqual(d["sample"]["node_id"], "dgx02")
        self.assertEqual(d["sample"]["metrics"]["cpu_percent"], 9.0)  # real value
        self.assertIsNotNone(d["last_success_at"])
        # the node that never had a sample must stay 'never' with no sample
        self.assertEqual(s1["nodes"]["dgx01"]["state"], "online")


class ControllerTests(AggBase):
    def test_controller_unavailable_reports_unknown_not_split(self):  # scenario 9
        payloads = {
            "dgx01": telemetry("dgx01", split_metrics()),
            "dgx02": telemetry("dgx02", split_metrics()),
            "dgx03": telemetry("dgx03", split_metrics()),
        }
        ctl = FakeController(exc=ControllerError("controller timed out after 1.0s"))
        snap = make_aggregator(make_request_fn(payloads), controller=ctl).collect()
        self.assertEqual(snap["cluster"]["mode"], "unknown")
        self.assertIsNone(snap["cluster"]["healthy"])
        self.assertIsNone(snap["cluster"]["transitioning"])
        self.assertEqual(snap["cluster"]["controller"], "error")
        # still not guessed as split, and nodes are unaffected
        self.assertNotEqual(snap["cluster"]["mode"], "split")
        self.assertEqual(snap["nodes"]["dgx01"]["state"], "online")
        self.assertTrue(any("controller unavailable" in w and "unknown" in w
                            for w in snap["warnings"]))
        # mode expectations become neutral when the controller is unknown
        self.assertEqual(snap["nodes"]["dgx02"]["mode_expected"],
                         {"ollama": "any", "heavy": "any"})

    def test_controller_explicitly_disabled_is_unknown(self):
        payloads = {nid: telemetry(nid, split_metrics())
                    for nid in ("dgx01", "dgx02", "dgx03")}
        snap = make_aggregator(make_request_fn(payloads), controller=False).collect()
        self.assertEqual(snap["cluster"]["mode"], "unknown")
        self.assertEqual(snap["cluster"]["controller"], "error")

    def test_default_controller_reads_cluster_model(self):
        # the out-of-the-box aggregator wires up the standard cluster-model
        # read (fixed executable + argument list);
        agg = ClusterAggregator()                    # needs ClusterModelController
        from cluster import ClusterModelController as _C
        self.assertIsInstance(agg._controller, _C)
        exe = os.path.expanduser("~/bin/cluster-model")
        self.assertEqual(agg._controller.command, [exe, "status", "--json"])

    def test_controller_transitioning_verbatim(self):                # scenario 10
        # even if worker activity looks "split-like", a transitioning controller
        # must be reported verbatim and workers must NOT be judged.
        payloads = {
            "dgx01": telemetry("dgx01", split_metrics()),
            "dgx02": telemetry("dgx02", split_metrics()),
            "dgx03": telemetry("dgx03", split_metrics()),
        }
        ctl = FakeController(controller_status(mode="transitioning",
                                               healthy=False, transitioning=True))
        snap = make_aggregator(make_request_fn(payloads), controller=ctl).collect()
        self.assertEqual(snap["cluster"]["mode"], "transitioning")
        self.assertIs(snap["cluster"]["healthy"], False)
        self.assertIs(snap["cluster"]["transitioning"], True)
        for nid in ("dgx02", "dgx03"):
            self.assertIsNone(snap["nodes"][nid]["as_expected"], nid)
        # no deviation warnings during a transition
        self.assertFalse(any("deviates" in w for w in snap["warnings"]))


class HeavyModeTests(AggBase):
    def _heavy_aggregator(self, dgx02_metrics=None, dgx03_metrics=None):
        payloads = {
            "dgx01": telemetry("dgx01", stop_metrics()),     # front-end host
            "dgx02": telemetry("dgx02",
                               dgx02_metrics or heavy_metrics()),
            "dgx03": telemetry("dgx03",
                               dgx03_metrics or heavy_metrics()),
        }
        ctl = FakeController(controller_status(mode="heavy-qwen", healthy=True))
        return make_aggregator(make_request_fn(payloads), controller=ctl).collect()

    def test_heavy_workers_as_expected(self):                        # scenario 11
        snap = self._heavy_aggregator()
        self.assertEqual(snap["cluster"]["mode"], "heavy-qwen")
        for nid in ("dgx02", "dgx03"):
            n = snap["nodes"][nid]
            self.assertEqual(n["state"], "online")
            self.assertEqual(n["as_expected"], True, nid)
            self.assertEqual(n["mode_expected"],
                             {"ollama": "inactive", "heavy": "active"}, nid)
        self.assertIsNone(snap["nodes"]["dgx01"]["as_expected"])
        self.assertFalse(any("deviates" in w for w in snap["warnings"]))

    def test_heavy_worker_deviation_flagged_not_fatal(self):
        # dgx02 deviates (ollama still active) -> as_expected False + warning
        snap = self._heavy_aggregator(
            dgx02_metrics=heavy_metrics(ollama_up=1, ollama_daemon_count=2))
        d = snap["nodes"]["dgx02"]
        self.assertEqual(d["as_expected"], False)
        self.assertTrue(any("dgx02" in w and "deviates" in w
                            for w in snap["warnings"]))
        # still online, not an unexplained hard failure
        self.assertEqual(d["state"], "online")
        # dgx03 is fine
        self.assertEqual(snap["nodes"]["dgx03"]["as_expected"], True)

    def test_heavy_worker_missing_activity_flagged(self):
        snap = self._heavy_aggregator(
            dgx03_metrics=stop_metrics())   # heavy node with no heavy work
        self.assertEqual(snap["nodes"]["dgx03"]["as_expected"], False)
        self.assertTrue(any("dgx03" in w and "deviates" in w
                            for w in snap["warnings"]))


class CredentialTests(AggBase):
    def _write(self, name, content, mode):
        p = os.path.join(self.tmpdir, name)
        with open(p, "w") as f:
            f.write(content)
        os.chmod(p, mode)
        return p

    def test_read_token_ok(self):                                     # scenario 12
        p = self._write("tok", "abc123\n", 0o600)
        self.assertEqual(read_node_token(p).strip(), "abc123")

    def test_read_token_missing_raises(self):
        with self.assertRaises(ControllerError):
            read_node_token(os.path.join(self.tmpdir, "nope"))

    def test_read_token_world_readable_rejected(self):
        p = self._write("tok", "abc123\n", 0o644)
        with self.assertRaises(ControllerError) as ctx:
            read_node_token(p)
        self.assertIn("0600", str(ctx.exception))

    def test_read_token_empty_rejected(self):
        p = self._write("tok", "   \n", 0o600)
        with self.assertRaises(ControllerError):
            read_node_token(p)

    def test_load_config_reads_tokens_from_files(self):
        cfg = os.path.join(self.tmpdir, "ag.json")
        toks = {n: self._write("t_%s" % n, "secret-%s\n" % n, 0o600)
                for n in ("dgx01", "dgx02", "dgx03")}
        with open(cfg, "w") as f:
            json.dump({"nodes": {n: {"url": "http://%s:8165/api/telemetry" % h,
                                     "token_file": toks[n]}
                                 for n, h in _HOST.items()},
                       "controller": {"executable": "/usr/bin/true",
                                      "node_id_map_file": None}}, f)
        agg = load_aggregator_config(cfg)
        for n in ("dgx01", "dgx02", "dgx03"):
            src = agg._nodes[n]
            self.assertEqual(src.token, "secret-%s" % n)
            self.assertEqual(src.url, "http://%s:8165/api/telemetry" % _HOST[n])

    def test_load_config_rejects_bad_token_file(self):
        bad = self._write("bad", "x\n", 0o644)
        cfg = os.path.join(self.tmpdir, "ag.json")
        with open(cfg, "w") as f:
            json.dump({"nodes": {"dgx01": {"url": "http://127.0.0.1:8165/api/telemetry",
                                           "token_file": bad}}}, f)
        with self.assertRaises(ControllerError):
            load_aggregator_config(cfg)

    def test_token_never_appears_in_output(self):
        # a 401 must surface without leaking the bearer token into any field
        token = "super-secret-bearer-xyz"
        seen_tokens = []

        def rf(url, tok, timeout):
            seen_tokens.append(tok)
            raise NodeFetchError("telemetry error: HTTP 401")
        agg = make_aggregator(rf, controller=FakeController(controller_status()))
        for s in agg._nodes.values():
            s.token = token
        snap = agg.collect()
        self.assertTrue(all(t == token for t in seen_tokens))
        blob = json.dumps(snap)
        self.assertNotIn(token, blob)
        self.assertEqual(len(snap["warnings"]), 3)


# --------------------------------------------------------------------------
# Scenario 13: bounded timeouts and response sizes (+ slow node)
# --------------------------------------------------------------------------

def _serve(handler_cls):
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, port


def _stop_server(server):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ResourceWarning)
        server.shutdown()
        server.server_close()


def _handler(body=b"{}", code=200, pre_sleep=0.0):
    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            if pre_sleep:
                time.sleep(pre_sleep)
            try:
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass  # the (timed-out) client already went away

        def log_message(self, *a):
            pass
    return H


class BoundedHttpTests(AggBase):
    def _get(self, handler_cls, timeout=3.0, **kw):
        kw.setdefault("max_bytes", 4 << 20)
        server, port = _serve(handler_cls)
        try:
            return CA._http_get("http://127.0.0.1:%d/api/telemetry" % port,
                                "tok", timeout, **kw)
        finally:
            _stop_server(server)

    def test_real_http_ok(self):
        payload = {"node": node_sample("dgx01", split_metrics())}
        body = json.dumps(payload).encode()
        got = self._get(_handler(body))
        self.assertEqual(got, payload)

    def test_http_error_code(self):
        with self.assertRaises(NodeFetchError) as ctx:
            self._get(_handler(b"no", code=401))
        self.assertIn("401", str(ctx.exception))

    def test_malformed_json_body(self):                       # scenario 4 (transport)
        with self.assertRaises(NodeFetchError) as ctx:
            self._get(_handler(b"this is not json"))
        self.assertIn("JSON", str(ctx.exception))

    def test_response_size_bound(self):                       # scenario 13
        big = (b'"k":' + b"x" * 500 + b'')
        body = b"{" + big + b",\"filler\":" + b"y" * 400 + b"}"
        self.assertGreater(len(body), 256)
        with self.assertRaises(NodeFetchError) as ctx:
            self._get(_handler(body), max_bytes=256)
        self.assertIn("too large", str(ctx.exception))

    def test_timeout_is_bounded(self):                        # scenario 13
        t0 = time.time()
        with self.assertRaises(NodeFetchError):
            self._get(_handler(b"{}", pre_sleep=2.0), timeout=0.4)
        elapsed = time.time() - t0
        self.assertLess(elapsed, 1.5)     # bounded, did not wait 2 s
        self.assertGreater(elapsed, 0.3)

    def test_slow_node_does_not_block_other_nodes(self):      # scenario 13
        def slow(url, token, timeout):
            for nid, host in _HOST.items():
                if host in url:
                    if nid == "dgx02":
                        time.sleep(3.0)     # far beyond the collection deadline
                    return telemetry(nid, split_metrics())
            raise NodeFetchError("unknown")
        deadline = 1.5
        t0 = time.time()
        snap = make_aggregator(slow,
                               controller=FakeController(controller_status()),
                               collection_deadline_seconds=deadline).collect()
        elapsed = time.time() - t0
        self.assertLess(elapsed, deadline + 1.2)
        self.assertGreater(elapsed, deadline - 0.05)
        # the two fast nodes were still collected
        self.assertEqual(snap["nodes"]["dgx01"]["state"], "online")
        self.assertEqual(snap["nodes"]["dgx03"]["state"], "online")
        d = snap["nodes"]["dgx02"]        # the slow one is reported, not fatal
        self.assertEqual(d["state"], "never")
        self.assertIsNone(d["sample"])
        self.assertIs(d["reachable"], False)
        self.assertIn("timed out", d["error"] or "")
        self.assertTrue(any("dgx02" in w and "timed out" in w
                            for w in snap["warnings"]))


# --------------------------------------------------------------------------
# Scenario 14: no cluster-mode mutation
# --------------------------------------------------------------------------

class NoMutationTests(AggBase):
    def test_fixed_command_list(self):
        c = ClusterModelController(executable="/bin/fake-cluster-model")
        self.assertEqual(c.command,
                         ["/bin/fake-cluster-model", "status", "--json"])
        self.assertEqual(c.command[1], "status")
        for verb in ("split", "stop", "restart", "reset", "reload", "up", "down"):
            self.assertNotIn(verb, c.command[1:])

    def test_refuses_mutating_verb(self):
        # the head verb must be the read-only status verb
        for bad in (("split",), ("up",), ("health", "--json"), ("stop", "--hard")):
            with self.assertRaises(ControllerError) as ctx:
                ClusterModelController(executable="/bin/x", arguments=bad)
            self.assertIn("status", str(ctx.exception))
        # a mutating verb anywhere after 'status' is also refused
        with self.assertRaises(ControllerError) as ctx:
            ClusterModelController(executable="/bin/x", arguments=("status", "split"))
        self.assertIn("mutating", str(ctx.exception))
        # the read-only status command itself is accepted
        ok = ClusterModelController(executable="/bin/x", arguments=("status", "--json"))
        self.assertEqual(ok.command, ["/bin/x", "status", "--json"])

    def test_runner_receives_fixed_list(self):
        captured = {}

        def runner(cmd, timeout, max_bytes):
            captured["cmd"] = cmd
            captured["timeout"] = timeout
            captured["max_bytes"] = max_bytes
            return 0, (b'{"schema":1,"mode":"split","healthy":true,'
                       b'"transitioning":false}')
        c = ClusterModelController(executable="/bin/fake", runner=runner)
        data = c.status()
        self.assertEqual(captured["cmd"], ["/bin/fake", "status", "--json"])
        self.assertGreater(captured["timeout"], 0)
        self.assertGreater(captured["max_bytes"], 0)
        self.assertEqual(data["mode"], "split")

    def test_real_subprocess_shell_false(self):
        argv_file = os.path.join(self.tmpdir, "argv.json")
        exe = os.path.join(self.tmpdir, "fake_cm.py")
        with open(exe, "w") as fh:
            fh.write(
                "#!/usr/bin/env python3\n"
                "import sys, json\n"
                "open(%r, 'w').write(json.dumps(sys.argv[1:]))\n"
                "print(json.dumps({'schema': 1, 'mode': 'split', 'healthy': "
                "True, 'transitioning': False}))\n" % argv_file)
        os.chmod(exe, 0o755)
        # a space-containing token must arrive as ONE argv element: proves
        # shell=False (the list is the exact command, no re-splitting).
        c = ClusterModelController(
            executable=exe, arguments=("status", "--label=hello world"))
        data = c.status()
        self.assertEqual(data["mode"], "split")
        with open(argv_file) as fh:
            self.assertEqual(json.load(fh), ["status", "--label=hello world"])

    def test_never_passes_a_shell_string(self):
        c = CA.ClusterModelController(executable="/bin/fake")
        # .command must always be a list (the _run_bounded path requires it)
        self.assertIsInstance(c.command, list)
        self.assertTrue(all(isinstance(a, str) for a in c.command))
        # and the controller must not expose a raw shell invocation helper
        self.assertFalse(hasattr(c, "system"))


# --------------------------------------------------------------------------
# Scenario 15: complete JSON serialization
# --------------------------------------------------------------------------

class SerializationTests(AggBase):
    def test_complete_json_serialization_roundtrip(self):
        clock = FakeClock(start=1000.0)
        snap = split_aggregator(clock=clock).collect()
        blob = json.dumps(snap, allow_nan=False)     # must not raise
        self.assertEqual(json.loads(blob), snap)
        self.assertEqual(set(snap.keys()),
                         {"schema_version", "timestamp", "cluster", "nodes",
                          "warnings", "meta"})

    def test_non_finite_numbers_sanitized(self):
        payloads = {
            "dgx01": telemetry("dgx01", split_metrics(cpu_percent=float("nan"))),
            "dgx02": telemetry("dgx02",
                               split_metrics(cpu_percent=float("inf"))),
            "dgx03": telemetry("dgx03",
                               split_metrics(cpu_percent=-float("inf"))),
        }
        snap = make_aggregator(make_request_fn(payloads),
                               controller=FakeController(controller_status())).collect()
        blob = json.dumps(snap, allow_nan=False)     # would raise otherwise
        self.assertNotIn("NaN", blob)
        self.assertNotIn("Infinity", blob)
        for nid in ("dgx01", "dgx02", "dgx03"):
            self.assertIsNone(
                snap["nodes"][nid]["sample"]["metrics"]["cpu_percent"])

    def test_snapshot_has_required_keys(self):
        clock = FakeClock(start=1000.0)
        snap = split_aggregator(clock=clock).collect()
        self.assertIsInstance(snap["schema_version"], int)
        self.assertIsInstance(snap["timestamp"], (int, float))
        self.assertEqual(snap["cluster"]["controller"], "ok")
        self.assertIsInstance(snap["warnings"], list)
        self.assertIsInstance(snap["meta"], dict)
        required = ("state", "reachable", "fresh", "sample_stale", "quality",
                    "schema_valid", "identity_valid", "last_success_at",
                    "age_seconds", "error", "sample", "sample_timestamp",
                    "remote_reported_stale", "mode_expected", "as_expected")
        for nid in ("dgx01", "dgx02", "dgx03"):
            for key in required:
                self.assertIn(key, snap["nodes"][nid], nid)


# --------------------------------------------------------------------------
# Robustness + config/HTTP integration
# --------------------------------------------------------------------------

class RobustnessTests(AggBase):
    def test_collect_never_raises_on_total_failure(self):
        def dead(url, token, timeout):
            raise NodeFetchError("deadline exceeded (dead)")
        snap = make_aggregator(dead,
                               controller=FakeController(
                                   exc=ControllerError("controller down"))).collect()
        self.assertEqual(snap["cluster"]["mode"], "unknown")
        for nid in ("dgx01", "dgx02", "dgx03"):
            self.assertEqual(snap["nodes"][nid]["state"], "never")
            self.assertIsNone(snap["nodes"][nid]["sample"])
        self.assertTrue(snap["warnings"])
        json.dumps(snap, allow_nan=False)     # still fully serializable

    def test_single_node_error_does_not_contaminate_others(self):
        # dgx02 returns a huge (but valid) payload; the others must stay clean
        big = telemetry("dgx02", split_metrics())
        big["filler"] = "x" * 100000
        payloads = {
            "dgx01": telemetry("dgx01", split_metrics()),
            "dgx02": big,
            "dgx03": telemetry("dgx03", split_metrics()),
        }
        snap = make_aggregator(make_request_fn(payloads),
                               controller=FakeController(controller_status())).collect()
        self.assertEqual(snap["nodes"]["dgx01"]["state"], "online")
        self.assertEqual(snap["nodes"]["dgx03"]["state"], "online")
        self.assertEqual(snap["nodes"]["dgx02"]["state"], "online")
        json.dumps(snap, allow_nan=False)


class ConfigIntegrationTests(AggBase):
    def test_loaded_config_uses_real_http_path(self):
        payload = {"node": node_sample("dgx01", split_metrics()),
                   "stale": False, "server_time": 1.0, "age_seconds": 1.0,
                   "collector_error": None}
        server, port = _serve(_handler(json.dumps(payload).encode()))
        try:
            cfg = os.path.join(self.tmpdir, "ag.json")
            with open(cfg, "w") as fh:
                json.dump({"nodes": {
                    "dgx01": {"url": "http://127.0.0.1:%d/api/telemetry" % port},
                    "dgx02": {"url": "http://127.0.0.1:1/api/telemetry"},
                    "dgx03": {"url": "http://127.0.0.1:1/api/telemetry"},
                }}, fh)          # no controller block -> controller disabled
            agg = load_aggregator_config(cfg)
            snap = agg.collect()
            # dgx01 served by the real loopback HTTP client
            self.assertEqual(snap["nodes"]["dgx01"]["state"], "online")
            self.assertEqual(snap["nodes"]["dgx01"]["sample"]["node_id"], "dgx01")
            # the dead nodes are isolated, not fatal
            self.assertEqual(snap["nodes"]["dgx02"]["state"], "never")
            self.assertEqual(snap["nodes"]["dgx03"]["state"], "never")
            # controller disabled -> unknown, never guessed
            self.assertEqual(snap["cluster"]["mode"], "unknown")
        finally:
            _stop_server(server)


# --------------------------------------------------------------------------
# Task 1A: cached-data freshness — remote stale indicator + central
# receipt/change tracking.  A node endpoint that keeps answering HTTP 200
# with stale=true must stay STALE across repeated successful polls.
# --------------------------------------------------------------------------

class RemoteStaleSampleTests(AggBase):
    def _stale_serving(self, clock):
        """All nodes answer, but dgx02 serves a cached (remote-stale) sample."""
        stale = {"dgx02": True}

        def rf(url, token, timeout):
            for nid, host in _HOST.items():
                if host in url:
                    return telemetry(nid, split_metrics(cpu_percent=5.0),
                                     remote_stale=stale.get(nid, False))
            raise NodeFetchError("unknown url")

        return make_aggregator(rf, controller=FakeController(controller_status()),
                               clock=clock), stale

    def test_stale_sample_stays_stale_across_repeated_successes(self):
        clock = FakeClock(start=1000.0)
        agg, stale = self._stale_serving(clock)
        s0 = agg.collect()                     # first response already stale
        self.assertEqual(s0["nodes"]["dgx02"]["state"], "stale")
        self.assertIsNone(s0["nodes"]["dgx02"]["sample_age_seconds"])
        for i in range(5):                     # repeated successful HTTP polls
            clock.advance(5.0)
            s = agg.collect()
            d = s["nodes"]["dgx02"]
            # endpoint reachable and the response was just received ...
            self.assertIs(d["reachable"], True, i)
            self.assertEqual(d["age_seconds"], 0.0, i)
            # ... but the underlying sample remains stale, never 'online'
            self.assertEqual(d["state"], "stale", i)
            self.assertIs(d["fresh"], False, i)
            self.assertIs(d["sample_fresh"], False, i)
            self.assertIs(d["sample_stale"], True, i)
            self.assertIs(d["remote_reported_stale"], True, i)
            # the retained real sample is still shown (never zero-filled)
            self.assertEqual(d["sample"]["metrics"]["cpu_percent"], 5.0, i)
            # a stale sample is not judged against the mode expectation
            self.assertIsNone(d["as_expected"], i)
        # explicit, sanitized warning every cycle
        w = [x for x in s["warnings"] if x.startswith("dgx02")]
        self.assertTrue(w, s["warnings"])
        self.assertIn("cached sample", w[0])
        # the other nodes are unaffected and fully fresh
        for nid in ("dgx01", "dgx03"):
            self.assertEqual(s["nodes"][nid]["state"], "online")
            self.assertIs(s["nodes"][nid]["sample_fresh"], True)
        # the whole snapshot stays JSON-serializable
        json.dumps(s, allow_nan=False)

    def test_sample_age_grows_then_recovers_when_fresh_returns(self):
        clock = FakeClock(start=1000.0)
        agg, stale = self._stale_serving(clock)
        stale["dgx02"] = False                  # starts fresh ...
        agg.collect()                              # fresh receipt at t=1000
        stale["dgx02"] = True                      # collector begins serving cache
        clock.advance(12.0)
        s1 = agg.collect()
        self.assertEqual(s1["nodes"]["dgx02"]["sample_age_seconds"], 12.0)
        clock.advance(8.0)
        s2 = agg.collect()
        self.assertEqual(s2["nodes"]["dgx02"]["sample_age_seconds"], 20.0)
        self.assertIn("last fresh central receipt 20s ago",
                      next(w for w in s2["warnings"] if w.startswith("dgx02")))
        stale["dgx02"] = False                     # collector recovers
        clock.advance(5.0)
        s3 = agg.collect()
        d = s3["nodes"]["dgx02"]
        self.assertEqual(d["state"], "online")
        self.assertIs(d["sample_fresh"], True)
        self.assertIs(d["sample_stale"], False)
        self.assertEqual(d["sample_age_seconds"], 0.0)

    def test_missing_stale_indicator_is_not_treated_as_stale(self):
        clock = FakeClock(start=1000.0)

        def rf(url, token, timeout):
            for nid, host in _HOST.items():
                if host in url:
                    wrapper = {"node": node_sample(nid, split_metrics()),
                               "server_time": clock(), "age_seconds": 1.0}
                    if nid == "dgx02":
                        return wrapper            # no "stale" key at all
                    wrapper["stale"] = False
                    return wrapper
            raise NodeFetchError("unknown url")

        agg = make_aggregator(rf, controller=FakeController(controller_status()),
                              clock=clock)
        s = agg.collect()
        d = s["nodes"]["dgx02"]
        self.assertEqual(d["state"], "online")
        self.assertIsNone(d["remote_reported_stale"])
        self.assertIs(d["sample_fresh"], True)


# --------------------------------------------------------------------------
# Task 1B: bounded thread lifetime — repeated node timeouts must not
# accumulate abandoned threads, overlap unbounded fetches, or delay
# process shutdown (aggregate fetch threads are daemons).
# --------------------------------------------------------------------------

def _agg_thread_names():
    return {"agg-dgx01", "agg-dgx02", "agg-dgx03"}


class BoundedThreadLifetimeTests(AggBase):
    def _aggregator_with_slow_node(self, sleep_seconds, deadline_seconds):
        slow_calls = {"n": 0}
        slow_threads = set()
        observed = {}

        def rf(url, token, timeout):
            for nid, host in _HOST.items():
                if host in url:
                    if nid == "dgx02":
                        slow_calls["n"] += 1
                        slow_threads.add(id(threading.current_thread()))
                        time.sleep(sleep_seconds)     # far beyond the deadline
                        observed["dgx02_thread"] = threading.current_thread()
                        return telemetry("dgx02", split_metrics(cpu_percent=42.0))
                    observed.setdefault("fast_thread", threading.current_thread())
                    return telemetry(nid, split_metrics())
            raise NodeFetchError("unknown url")

        agg = make_aggregator(rf, controller=FakeController(controller_status()),
                              collection_deadline_seconds=deadline_seconds)
        return agg, slow_calls, slow_threads, observed

    def test_repeated_timeouts_do_not_accumulate_or_duplicate(self):
        sleep_s, deadline_s = 2.0, 0.25
        agg, slow_calls, slow_threads, observed = self._aggregator_with_slow_node(
            sleep_s, deadline_s)
        baseline = len([t for t in threading.enumerate()
                        if t.name in _agg_thread_names()])
        peak = baseline
        t0 = time.time()
        for i in range(4):
            t_start = time.time()
            snap = agg.collect()
            elapsed = time.time() - t_start
            self.assertLess(elapsed, deadline_s + 1.0, "collect exceeded budget")
            # fast nodes unaffected; slow node isolated + reported
            self.assertEqual(snap["nodes"]["dgx01"]["state"], "online", i)
            self.assertEqual(snap["nodes"]["dgx03"]["state"], "online", i)
            d = snap["nodes"]["dgx02"]
            self.assertIn("timed out", d["error"] or "", i)
            live = len([t for t in threading.enumerate()
                        if t.name in _agg_thread_names() and t.is_alive()])
            peak = max(peak, live)
        total = time.time() - t0
        self.assertLess(total, sleep_s + 0.5)       # well inside the sleep window
        # one slow fetch in flight, NOT one per failed cycle
        self.assertEqual(slow_calls["n"], 1)
        self.assertEqual(len(slow_threads), 1)
        # no duplicate fetch in flight; one slow fetch only
        self.assertEqual(slow_calls["n"], 1)
        self.assertEqual(len(slow_threads), 1)
        # no abandoned-thread accumulation: live agg threads stay bounded
        self.assertLessEqual(peak, baseline + 3)
        # daemon threads: cannot delay process shutdown
        self.assertTrue(observed["fast_thread"].daemon)
        # the slow result is eventually consumed on a later cycle
        time.sleep(sleep_s + 0.2 - total)
        self.assertTrue(observed["dgx02_thread"].daemon)
        snap2 = agg.collect()
        self.assertEqual(snap2["nodes"]["dgx02"]["state"], "online")
        self.assertEqual(snap2["nodes"]["dgx02"]["sample"]["metrics"]
                         ["cpu_percent"], 42.0)
        # no duplicate fetch was spawned in the meantime
        self.assertEqual(slow_calls["n"], 1)

    def test_deadline_still_isolates_a_slow_node_per_cycle(self):
        # each collect remains bounded by the shared deadline even while the
        # previous cycle's fetch is still in flight (reused, not re-launched)
        agg, slow_calls, _, _ = self._aggregator_with_slow_node(1.5, 0.3)
        t0 = time.time()
        for i in range(3):
            t_start = time.time()
            snap = agg.collect()
            self.assertLess(time.time() - t_start, 1.5)
            self.assertIn("timed out", snap["nodes"]["dgx02"]["error"] or "")
        self.assertLess(time.time() - t0, 3.0)
        self.assertEqual(slow_calls["n"], 1)
