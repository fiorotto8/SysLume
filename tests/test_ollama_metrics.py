"""Tests for collectors.ollama_metrics using a mocked local Ollama endpoint."""
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

spec = importlib.util.spec_from_file_location("om_under_test",
                                              ROOT / "collectors" / "ollama_metrics.py")
om = importlib.util.module_from_spec(spec)
spec.loader.exec_module(om)

# Fixture values in exact SI units: bytes for sizes, tokens for context.
RESIDENT_SIZE = 29978242072          # bytes
SECOND_SIZE_VRAM = 200               # bytes
CONTEXT_MAIN, CONTEXT_SECOND = 32768, 8192   # tokens
EXPIRES_ISO = "2027-01-01T00:00:00Z"
EXPIRES_EPOCH = 1798761600.0

TAGS = json.dumps({"models": [
    {"name": "mod-a:tag", "size": RESIDENT_SIZE, "digest": "digest-a",
     "details": {"quantization_level": "Q8_0"}, "capabilities": ["completion", "tools"]},
    {"name": "mod-only:tag", "size": 111, "digest": "digest-only", "details": {}},
    {"name": "mod-b:tag", "size": SECOND_SIZE_VRAM + 22, "digest": "digest-b"},
]}).encode()

PS = json.dumps({"models": [
    {"name": "mod-a:tag", "size": RESIDENT_SIZE, "digest": "digest-a",
     "size_vram": RESIDENT_SIZE, "context_length": CONTEXT_MAIN,
     "expires_at": EXPIRES_ISO, "processor": {"major": "100% GPU", "minor": ""}},
    {"name": "mod-b:tag", "size": SECOND_SIZE_VRAM + 22, "size_vram": SECOND_SIZE_VRAM,
     "context_length": CONTEXT_SECOND},
]}).encode()


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if getattr(self.server, "sleep_all", False):
            time.sleep(1.5)   # outruns any short reader timeout
            return
        if self.path == "/SLEEP":
            time.sleep(1.5)
            return
        body = self.server.routes.get(self.path)
        if body is None:
            self.send_response(404)
            self.end_headers()
            return
        if isinstance(body, tuple):
            self.send_response(body[0])
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


def make_server(routes, sleep_all=False):
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    server.routes = dict(routes)
    server.sleep_all = sleep_all
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


class HealthyEndpointTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = make_server({
            "/api/version": b'{"version": "0.34.4"}',
            "/api/tags": TAGS,
            "/api/ps": PS,
        })
        cls.sample = om.OllamaReader(base_url(cls.server)).collect()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def test_healthy_fields(self):
        s = self.sample
        self.assertEqual(s["status"], "ok")
        self.assertIsNone(s["error"])
        self.assertEqual(s["version"], "0.34.4")
        self.assertEqual(s["installed_models"], ["mod-a:tag", "mod-b:tag", "mod-only:tag"])
        self.assertEqual(s["resident_models"], ["mod-a:tag", "mod-b:tag"])
        self.assertLess(abs(s["timestamp"] - time.time()), 30)
        main = s["models"]["mod-a:tag"]
        self.assertTrue(main["installed"] and main["resident"])
        self.assertEqual(main["digest"], "digest-a")
        self.assertEqual(main["disk_size_bytes"], RESIDENT_SIZE)
        self.assertEqual(main["quantization_level"], "Q8_0")

    def test_processor_only_when_reported(self):
        # Fixture model carries the newer-OLLAMA `processor` dict -> GPU.
        self.assertEqual(self.sample["models"]["mod-a:tag"]["processor"], "gpu")
        # Ollama 0.34.4 omits `processor` entirely -> None, not a guess.
        self.assertIsNone(self.sample["models"]["mod-b:tag"]["processor"])

    def test_installed_but_not_resident(self):
        entry = self.sample["models"]["mod-only:tag"]
        self.assertTrue(entry["installed"])
        self.assertFalse(entry["resident"])
        self.assertIsNone(entry["size_vram_bytes"])
        self.assertIn("mod-only:tag", self.sample["installed_models"])
        self.assertNotIn("mod-only:tag", self.sample["resident_models"])

    def test_multiple_resident_metrics(self):
        m = self.sample["metrics"]
        self.assertEqual(m["ollama_up"], 1)
        self.assertEqual(m["ollama_models_installed"], 3)
        self.assertEqual(m["ollama_models_resident"], 2)
        self.assertEqual(m["ollama_resident_bytes"], RESIDENT_SIZE + SECOND_SIZE_VRAM)
        self.assertEqual(m["ollama_context_tokens_max"], CONTEXT_MAIN)

    def test_units_and_expiration(self):
        main = self.sample["models"]["mod-a:tag"]
        self.assertIsInstance(main["context_length_tokens"], int)
        self.assertEqual(main["context_length_tokens"], CONTEXT_MAIN)
        self.assertIsInstance(main["size_vram_bytes"], int)
        self.assertEqual(main["size_vram_bytes"], RESIDENT_SIZE)
        self.assertEqual(main["expires_at"], EXPIRES_ISO)
        self.assertAlmostEqual(main["expires_at_epoch"], EXPIRES_EPOCH, delta=1)
        self.assertGreater(main["expires_at_epoch"], self.sample["timestamp"])


class FailureModeTests(unittest.TestCase):
    def setUp(self):
        self.servers = []

    def tearDown(self):
        for server in self.servers:
            server.shutdown()
            server.server_close()

    def _server(self, routes, sleep_all=False):
        server = make_server(routes, sleep_all=sleep_all)
        self.servers.append(server)
        return server

    def test_stopped_ollama_is_unreachable_not_empty(self):
        reader = om.OllamaReader("http://127.0.0.1:%d" % closed_port(), timeout=1.0)
        s = reader.collect()
        self.assertEqual(s["status"], "unreachable")
        self.assertIsNotNone(s["error"])
        self.assertIsNone(s["version"])
        self.assertIsNone(s["installed_models"])   # not [] - inventory unknown
        self.assertIsNone(s["resident_models"])    # not [] - residency unknown
        self.assertEqual(s["metrics"]["ollama_up"], 0)
        self.assertIsNone(s["metrics"]["ollama_models_installed"])
        self.assertIsNone(s["metrics"]["ollama_models_resident"])
        self.assertIsNone(s["metrics"]["ollama_resident_bytes"])

    def test_connection_timeout_is_unreachable(self):
        server = self._server({}, sleep_all=True)
        started = time.time()
        s = om.OllamaReader(base_url(server), timeout=0.3).collect()
        self.assertEqual(s["status"], "unreachable")
        self.assertIsNotNone(s["error"])
        self.assertIsNone(s["version"])
        self.assertIsNone(s["installed_models"])
        self.assertIsNone(s["resident_models"])
        self.assertLess(time.time() - started, 3.0)   # bounded by timeout, not by sleep

    def test_malformed_ps_json_is_partial_error(self):
        server = self._server({"/api/version": b'{"version": "0.34.4"}',
                               "/api/tags": TAGS, "/api/ps": b"this is {not json"})
        s = om.OllamaReader(base_url(server)).collect()
        self.assertEqual(s["status"], "error")
        self.assertIsNotNone(s["error"])
        self.assertIn("malformed", s["error"])
        self.assertEqual(s["installed_models"], ["mod-a:tag", "mod-b:tag", "mod-only:tag"])
        self.assertIsNone(s["resident_models"])
        self.assertEqual(s["metrics"]["ollama_up"], 1)
        self.assertIsNone(s["metrics"]["ollama_models_resident"])

    def test_missing_optional_fields_stay_null(self):
        bare_ps = json.dumps({"models": [
            "junk-string", 42, None, {"name": "bare:tag"},
            {"size_vram": "huge", "context_length": -5, "expires_at": "not-a-date"},
        ]}).encode()
        server = self._server({
            "/api/version": b'{"version": "0.34.4"}',
            "/api/tags": json.dumps({"models": [{"name": "bare:tag"}]}).encode(),
            "/api/ps": bare_ps,
        })
        s = om.OllamaReader(base_url(server)).collect()
        self.assertEqual(s["status"], "ok")
        self.assertEqual(set(s["models"]), {"bare:tag"})
        entry = s["models"]["bare:tag"]
        self.assertTrue(entry["installed"] and entry["resident"])
        for field in ("digest", "disk_size_bytes", "size_vram_bytes",
                      "context_length_tokens", "expires_at", "expires_at_epoch"):
            self.assertIsNone(entry[field])
        self.assertIsNone(s["metrics"]["ollama_resident_bytes"])


class MemorySemanticsTests(unittest.TestCase):
    """Ollama's size_vram is a model allocation estimate on GB10 unified
    memory; the collector must never add it onto a host memory total."""

    @classmethod
    def setUpClass(cls):
        cls.server = make_server({
            "/api/version": b'{"version": "0.34.4"}', "/api/tags": TAGS, "/api/ps": PS,
        })
        cls.sample = om.OllamaReader(base_url(cls.server)).collect()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def test_resident_bytes_is_attribution_only_sum(self):
        views = self.sample["models"].values()
        allocated = [v["size_vram_bytes"] for v in views
                     if v["size_vram_bytes"] is not None]
        self.assertEqual(self.sample["metrics"]["ollama_resident_bytes"],
                         sum(allocated))

    def test_no_aggregate_memory_key_exists(self):
        forbidden = ("total", "unified", "sum", "added")
        for key in self.sample:
            self.assertFalse(any(token in key.lower() for token in forbidden), key)
        for entry in self.sample["models"].values():
            for key in entry:
                self.assertFalse(any(token in key.lower() for token in forbidden), key)

    def test_sample_is_json_serializable(self):
        round_tripped = json.loads(json.dumps(self.sample))
        self.assertEqual(round_tripped["metrics"], self.sample["metrics"])


class BaseUrlValidationTests(unittest.TestCase):
    def test_rejects_injection_shapes(self):
        for bad in ("ftp://host:11434", "http://user:pass@host:11434",
                    "http://host:11434?next=http://evil", "http://host/api/ps",
                    "http://host:11434#frag", "not a url"):
            with self.assertRaises(ValueError, msg=bad):
                om.validate_base_url(bad)

    def test_accepts_node_endpoints(self):
        self.assertEqual(om.validate_base_url("http://192.168.0.2:11434"),
                         "http://192.168.0.2:11434")
        self.assertEqual(om.validate_base_url("http://192.168.2.2:11434/"),
                         "http://192.168.2.2:11434")
        self.assertEqual(om.validate_base_url("http://127.0.0.1:11434"),
                         "http://127.0.0.1:11434")


class LiveDgx01Tests(unittest.TestCase):
    """Read-only check against the real DGX01 Ollama (skipped if offline)."""

    @classmethod
    def setUpClass(cls):
        probe = socket.socket()
        probe.settimeout(1.0)
        try:
            probe.connect(("127.0.0.1", 11434))
        except OSError:
            raise unittest.SkipTest("DGX01 Ollama not reachable")
        finally:
            probe.close()
        cls.sample = om.OllamaReader("http://127.0.0.1:11434").collect()

    def test_live_collect(self):
        s = self.sample
        self.assertEqual(s["status"], "ok")
        self.assertIsInstance(s["version"], str)
        self.assertIsInstance(s["timestamp"], float)
        self.assertIsInstance(s["installed_models"], list)
        self.assertIsInstance(s["resident_models"], list)
        for name, entry in s["models"].items():
            self.assertIsInstance(entry["installed"], bool)
            self.assertIsInstance(entry["resident"], bool)
            if entry["resident"]:
                self.assertGreater(entry["size_vram_bytes"], 0)
                self.assertGreater(entry["context_length_tokens"], 0)
            if not entry["resident"]:
                self.assertIsNone(entry["size_vram_bytes"])  # not loaded -> no allocation


if __name__ == "__main__":
    unittest.main(verbosity=2)
