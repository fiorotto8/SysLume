"""Tests for collectors.heavy_metrics using mocked local vLLM endpoints."""
import importlib.util
import json
import socket
import sys
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location("hm_under_test",
                                              ROOT / "collectors" / "heavy_metrics.py")
hm = importlib.util.module_from_spec(spec)
module = importlib.util.module_from_spec(spec)
sys.modules["hm_under_test"] = module
spec.loader.exec_module(module)
hm = module


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.server.requests.append(("GET", self.path))
        if getattr(self.server, "sleep_all", False):
            time.sleep(1.5)
            return
        route = self.server.routes.get(self.path)
        if route is None:
            self.send_response(404)
            self.end_headers()
            return
        status, body = route
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


PROM_GOOD = """\
# HELP vllm:num_requests_running Number of running requests.
# TYPE vllm:num_requests_running gauge
vllm:num_requests_running{model_name="heavy-qwen"} 3
# HELP vllm:num_requests_waiting Number of waiting requests.
# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_waiting{model_name="heavy-qwen"} 1
vllm:kv_cache_usage_perc{model_name="heavy-qwen"} 0.42
vllm:prompt_tokens_total{model_name="heavy-qwen"} 123456
vllm:generation_tokens_total{model_name="heavy-qwen"} 654321
vllm:http_requests_total{method="GET",type="health"} 42
vllm:time_to_first_token_seconds_bucket{le="0.5"} 7
vllm:time_to_first_token_seconds_count 5
vllm:time_to_first_token_seconds_sum 3.2
this line is not prometheus at all
"""


def make_server(routes, sleep_all=False):
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    server.routes = {path: (status, body)
                     for path, (status, body) in routes.items()}
    server.sleep_all = sleep_all
    server.requests = []
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def base_url(server):
    return "http://127.0.0.1:%d" % server.server_address[1]


def closed_port():
    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()
    return port


class PrometheusParserTests(unittest.TestCase):
    def test_parses_labeled_samples_and_skips_noise(self):
        series = hm.parse_prometheus(PROM_GOOD)
        self.assertEqual(series["vllm:num_requests_running"], [({"model_name": "heavy-qwen"}, 3.0)])
        self.assertEqual(series["vllm:kv_cache_usage_perc"], [({"model_name": "heavy-qwen"}, 0.42)])
        self.assertEqual(series["vllm:prompt_tokens_total"], [({"model_name": "heavy-qwen"}, 123456.0)])
        self.assertEqual(series["vllm:generation_tokens_total"], [({"model_name": "heavy-qwen"}, 654321.0)])
        # HELP/TYPE, histogram buckets/count/sum, and junk never appear.
        self.assertNotIn("vllm:time_to_first_token_seconds_bucket", series)
        self.assertNotIn("vllm:time_to_first_token_seconds_count", series)
        self.assertNotIn("vllm:time_to_first_token_seconds_sum", series)

    def test_mixed_model_labels_refuse_to_sum(self):
        text = 'vllm:num_requests_running{model_name="a"} 1\nvllm:num_requests_running{model_name="b"} 2\n'
        warnings = []
        self.assertIsNone(hm.pick_value(hm.parse_prometheus(text),
                                        ("vllm:num_requests_running",), warnings))
        self.assertTrue(warnings)

    def test_duplicate_identical_series_keeps_first(self):
        text = 'vllm:num_requests_running 2\nvllm:num_requests_running 2.5\n'
        warnings = []
        self.assertEqual(hm.pick_value(hm.parse_prometheus(text),
                                       ("vllm:num_requests_running",), warnings), 2.0)
        self.assertTrue(warnings)

    def test_legacy_counter_fallback(self):
        text = "vllm:num_prompt_tokens 55\n"
        warnings = []
        self.assertEqual(hm.pick_value(hm.parse_prometheus(text),
                                       hm.TOKEN_METRICS["prompt"], warnings), 55.0)


HEALTHY_ROUTES = {
    "/health": (200, b"OK"),
    "/v1/models": (200, json.dumps({"data": [{"id": "heavy-qwen"}]}).encode()),
    "/metrics": (200, PROM_GOOD.encode()),
}


class StateMachineTests(unittest.TestCase):
    """Expected-mode handling: split=inactive, transition=starting, heavy=ok."""

    def _stop_test_services(self):
        for server in self.servers:
            server.shutdown()
            server.server_close()

    def setUp(self):
        self.servers = []

    def tearDown(self):
        self._stop_test_services()

    def test_split_mode_inactive(self):
        reader = hm.VllmReader("http://127.0.0.1:%d" % closed_port(),
                               expected_mode="split", timeout=1.0)
        s = reader.collect()
        self.assertEqual(s["status"], "inactive")
        self.assertIsNone(s["error"])
        self.assertFalse(s["health"])
        self.assertIsNone(s["served_model"])
        for value in s["metrics"].values():
            self.assertIn(value, (None, 0))
        self.assertEqual(s["metrics"]["vllm_up"], 0)

    def test_split_mode_up_api_is_anomaly(self):
        server = make_server({"/health": (200, b"OK")})
        self.servers.append(server)
        s = hm.VllmReader(base_url(server), expected_mode="split", timeout=1.0).collect()
        self.assertEqual(s["status"], "error")
        self.assertTrue(s["health"])
        self.assertIn("split", s["error"])

    def test_transition_is_starting_without_requests(self):
        server = make_server(HEALTHY_ROUTES)
        self.servers.append(server)
        s = hm.VllmReader(base_url(server), expected_mode="transition", timeout=1.0).collect()
        self.assertEqual(s["status"], "starting")
        self.assertIsNone(s["health"])
        self.assertIsNone(s["metrics"]["vllm_running_reqs"])   # nothing invented
        self.assertEqual(server.requests, [])                  # no requests at all

    def test_healthy_heavy_qwen(self):
        server = make_server(HEALTHY_ROUTES)
        self.servers.append(server)
        reader = hm.VllmReader(base_url(server), expected_mode="heavy-qwen",
                               expected_model_id="heavy-qwen", timeout=2.0)
        s = reader.collect()
        self.assertEqual(s["status"], "ok")
        self.assertTrue(s["health"])
        self.assertIsNone(s["error"])
        self.assertEqual(s["served_model"], "heavy-qwen")
        self.assertFalse(s["details"]["model_mismatch"])
        m = s["metrics"]
        self.assertEqual(m["vllm_up"], 1)
        self.assertEqual(m["vllm_running_reqs"], 3.0)
        self.assertEqual(m["vllm_waiting_reqs"], 1.0)
        self.assertEqual(m["vllm_kv_cache_usage"], 0.42)
        self.assertEqual(m["vllm_prompt_tokens_total"], 123456.0)
        self.assertEqual(m["vllm_generation_tokens_total"], 654321.0)

    def test_healthy_heavy_deepseek(self):
        routes = dict(HEALTHY_ROUTES)
        routes["/v1/models"] = (200, json.dumps(
            {"data": [{"id": "deepseek-v4-flash-0731"}]}).encode())
        server = make_server(routes)
        self.servers.append(server)
        s = hm.VllmReader(base_url(server), expected_mode="heavy-deepseek",
                          expected_model_id="deepseek-v4-flash-0731").collect()
        self.assertEqual(s["status"], "ok")
        self.assertEqual(s["served_model"], "deepseek-v4-flash-0731")
        self.assertEqual(s["metrics"]["vllm_kv_cache_usage"], 0.42)

    def test_wrong_served_model_flagged(self):
        server = make_server(HEALTHY_ROUTES)   # serves heavy-qwen...
        self.servers.append(server)
        s = hm.VllmReader(base_url(server), expected_mode="heavy-deepseek",
                          expected_model_id="deepseek-v4-flash-0731").collect()
        self.assertEqual(s["status"], "ok")
        self.assertEqual(s["served_model"], "heavy-qwen")
        self.assertTrue(s["details"]["model_mismatch"])
        self.assertTrue(any("does not match" in w for w in s["details"]["warnings"]))

    def test_unreachable_in_heavy_mode(self):
        reader = hm.VllmReader("http://127.0.0.1:%d" % closed_port(),
                               expected_mode="heavy-qwen", timeout=1.0)
        s = reader.collect()
        self.assertEqual(s["status"], "unreachable")
        self.assertFalse(s["health"])
        self.assertIsNotNone(s["error"])
        self.assertEqual(s["metrics"]["vllm_up"], 0)
        self.assertIsNone(s["metrics"]["vllm_running_reqs"])

    def test_engine_warming_returns_503_is_starting(self):
        server = make_server({"/health": (503, b"{\"error\": \"not ready\"}"),
                              "/v1/models": (503, b""), "/metrics": (503, b"")})
        self.servers.append(server)
        s = hm.VllmReader(base_url(server), expected_mode="heavy-qwen").collect()
        self.assertEqual(s["status"], "starting")
        self.assertFalse(s["health"])
        self.assertIsNone(s["metrics"]["vllm_running_reqs"])   # nothing invented


class PartialFailureTests(unittest.TestCase):
    def setUp(self):
        self.servers = []

    def tearDown(self):
        for server in self.servers:
            server.shutdown()
            server.server_close()

    def test_metrics_endpoint_404_with_healthy_api(self):
        routes = dict(HEALTHY_ROUTES)
        del routes["/metrics"]
        server = make_server(routes)
        self.servers.append(server)
        s = hm.VllmReader(base_url(server), expected_mode="heavy-qwen").collect()
        # API is healthy; the /metrics endpoint itself is unavailable.
        self.assertEqual(s["status"], "error")
        self.assertTrue(s["health"])
        self.assertEqual(s["served_model"], "heavy-qwen")
        self.assertIn("/metrics", s["error"])
        for key in ("vllm_running_reqs", "vllm_waiting_reqs", "vllm_kv_cache_usage",
                    "vllm_prompt_tokens_total", "vllm_generation_tokens_total"):
            self.assertIsNone(s["metrics"][key])
        self.assertEqual(s["metrics"]["vllm_up"], 1)

    def test_malformed_metrics_body(self):
        routes = dict(HEALTHY_ROUTES)
        routes["/metrics"] = (200, b"%\x01 not prometheus {{{")
        server = make_server(routes)
        self.servers.append(server)
        s = hm.VllmReader(base_url(server), expected_mode="heavy-qwen").collect()
        self.assertEqual(s["status"], "ok")
        self.assertIsNone(s["metrics"]["vllm_kv_cache_usage"])  # absent -> null, not zero
        self.assertEqual(s["metrics"]["vllm_up"], 1)

    def test_missing_individual_metrics_stay_null(self):
        reduced = 'vllm:num_requests_running{model_name="heavy-qwen"} 2\n'
        routes = dict(HEALTHY_ROUTES)
        routes["/metrics"] = (200, reduced.encode())
        server = make_server(routes)
        self.servers.append(server)
        m = hm.VllmReader(base_url(server), expected_mode="heavy-qwen").collect()["metrics"]
        self.assertEqual(m["vllm_running_reqs"], 2.0)
        for key in ("vllm_waiting_reqs", "vllm_kv_cache_usage",
                    "vllm_prompt_tokens_total", "vllm_generation_tokens_total"):
            self.assertIsNone(m[key])

    def test_kv_cache_is_fraction(self):
        server = make_server(HEALTHY_ROUTES)
        self.servers.append(server)
        m = hm.VllmReader(base_url(server), expected_mode="heavy-qwen").collect()["metrics"]
        self.assertIsInstance(m["vllm_kv_cache_usage"], float)
        self.assertGreaterEqual(m["vllm_kv_cache_usage"], 0.0)
        self.assertLessEqual(m["vllm_kv_cache_usage"], 1.0)

    def test_cumulative_counters_preserved_verbatim(self):
        body = ('vllm:prompt_tokens_total 111\n'
                'vllm:generation_tokens_total 222\n').encode()
        routes = dict(HEALTHY_ROUTES)
        routes["/metrics"] = (200, body)
        server = make_server(routes)
        self.servers.append(server)
        m = hm.VllmReader(base_url(server), expected_mode="heavy-qwen").collect()["metrics"]
        self.assertEqual(m["vllm_prompt_tokens_total"], 111.0)   # cumulative, not a rate
        self.assertEqual(m["vllm_generation_tokens_total"], 222.0)


class RobustnessTests(unittest.TestCase):
    def setUp(self):
        self.servers = []

    def tearDown(self):
        for server in self.servers:
            server.shutdown()
            server.server_close()

    def test_bounded_timeout(self):
        server = make_server(HEALTHY_ROUTES, sleep_all=True)
        self.servers.append(server)
        started = time.time()
        s = hm.VllmReader(base_url(server), expected_mode="heavy-qwen", timeout=0.3).collect()
        self.assertEqual(s["status"], "unreachable")
        self.assertLess(time.time() - started, 3.0)

    def test_bounded_response_size(self):
        big = b"x" * 512
        server = make_server({"/health": (200, big), "/v1/models": (200, b"{}"), "/metrics": (200, b"")})
        self.servers.append(server)
        s = hm.VllmReader(base_url(server), expected_mode="heavy-qwen",
                          max_response_bytes=64).collect()
        self.assertEqual(s["status"], "error")
        self.assertIn("exceeds", s["error"])

    def test_no_inference_requests_issued(self):
        server = make_server(HEALTHY_ROUTES)
        self.servers.append(server)
        hm.VllmReader(base_url(server), expected_mode="heavy-qwen").collect()
        hm.VllmReader(base_url(server), expected_mode="split").collect()
        methods = {method for method, _ in server.requests}
        paths = {path for _, path in server.requests}
        self.assertEqual(methods, {"GET"})
        self.assertTrue(paths <= {"/health", "/v1/models", "/metrics"})
        self.assertNotIn("/v1/chat/completions", paths)

    def test_output_is_json_serializable(self):
        for routes in (HEALTHY_ROUTES, {}):
            server = make_server(routes)
            self.servers.append(server)
            for mode in ("split", "transition", "heavy-qwen", "heavy-deepseek"):
                s = hm.VllmReader(base_url(server), expected_mode=mode).collect()
                round_tripped = json.loads(json.dumps(s))
                self.assertEqual(round_tripped["status"], s["status"])
                self.assertEqual(round_tripped["metrics"], s["metrics"])

    def test_url_injection_rejected(self):
        for bad in ("http://h:8000/v1", "ftp://h:8000", "http://u:p@h:8000"):
            with self.assertRaises(ValueError):
                hm.validate_base_url(bad)


class LiveSplitModeTests(unittest.TestCase):
    """Read-only split-mode check against the real DGX02 heavy endpoint."""

    def test_heavy_endpoint_inactive_in_split_mode(self):
        reader = hm.VllmReader("http://192.168.0.2:8000", expected_mode="split",
                               timeout=3.0)
        s = reader.collect()
        self.assertEqual(s["status"], "inactive")
        self.assertFalse(s["health"])
        self.assertIsNone(s["error"])                       # not a failure
        self.assertIsNone(s["metrics"]["vllm_running_reqs"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
