"""Tests for collectors.node_collector.

All three readers are mocked with fakes; no network, no /proc walk, no
subprocess. A final guarded test performs a read-only live DGX01 smoke
run against the real readers.
"""

import json
import math
import os
import time
import unittest
import unittest.mock

from collectors import node_collector as NC
from collectors import process_metrics as PM
from collectors import system_metrics as SM


# --------------------------------------------------------------------------
# Fakes
# --------------------------------------------------------------------------

def system_sample(overrides=None, gpu_state="ready"):
    metrics = {key: None for key in SM.METRIC_KEYS}
    metrics.update({
        "cpu_percent": 12.0, "ram_used_bytes": 1000, "ram_total_bytes": 2000,
        "gpu_vram_used_bytes": 90000, "gpu_vram_total_bytes": 100000,
        "mem_unified_used_bytes": 1000, "mem_unified_total_bytes": 2000,
    })
    updates = overrides or {}
    for key, value in updates.items():
        metrics[key] = value
    return {
        "timestamp": 1000.0,
        "metrics": metrics,
        "details": {"gpu_status": {"available": True, "state": gpu_state,
                                   "message": "ok"},
                    "mem_unified_source": "nvidia-smi"},
    }


def ollama_sample(status="ok", **overrides):
    sample = {
        "timestamp": 1001.0, "status": status, "error": None,
        "version": "0.34.4",
        "installed_models": ["a:1", "b:2"], "resident_models": ["b:2"],
        "models": {
            "a:1": {"installed": True, "resident": False,
                    "size_vram_bytes": None},
            "b:2": {"installed": True, "resident": True,
                    "digest": "sha256-x", "size_bytes": 500,
                    "size_vram_bytes": 400,
                    "context_length_tokens": 8192},
        },
        "metrics": {"ollama_up": 1, "ollama_models_installed": 2,
                    "ollama_models_resident": 1, "ollama_resident_bytes": 400,
                    "ollama_context_tokens_max": 8192},
    }
    for key, value in overrides.items():
        sample[key] = value
    return sample


def process_sample(status="ok", warnings=()):
    metrics = {k: None for k in
               ("ollama_daemon_count", "ollama_runner_count",
                "ray_process_count", "vllm_process_count",
                "vllm_engine_core_count", "vllm_rank_worker_count",
                "docker_container_count", "tmux_session_count")}
    metrics.update({"ollama_daemon_count": 1, "ollama_runner_count": 2,
                    "ray_process_count": 0, "vllm_process_count": 0,
                    "vllm_engine_core_count": 0, "vllm_rank_worker_count": 0,
                    "docker_container_count": 0, "tmux_session_count": 0})
    return {"timestamp": 1002.0, "status": status, "error": None,
            "warnings": list(warnings), "metrics": metrics,
            "details": {}}


class FakeReader:
    def __init__(self, sample=None, exc=None):
        self._sample = sample
        self._exc = exc
        self.calls = 0

    def collect(self):
        self.calls += 1
        if self._exc is not None:
            raise self._exc
        return self._sample


def healthy(node_id="dgx01"):
    return {
        "system": FakeReader(system_sample()),
        "ollama": FakeReader(ollama_sample()),
        "processes": FakeReader(process_sample()),
    }


class NodeCollectorTestCase(unittest.TestCase):
    def collector(self, readers, node_id="dgx01", **kw):
        return NC.NodeCollector(node_id=node_id, **{**readers, **kw})


class HealthySnapshotTests(NodeCollectorTestCase):
    def test_all_three_components_healthy(self):
        readers = healthy()
        s = self.collector(readers).collect()
        self.assertEqual(s["schema_version"], 1)
        self.assertEqual(s["node_id"], "dgx01")
        self.assertEqual(s["status"], "ok")
        self.assertEqual(s["warnings"], [])
        self.assertEqual(list(s["components"].keys()),
                         ["system", "ollama", "processes"])
        for reader in readers.values():
            self.assertEqual(reader.calls, 1)
        self.assertGreaterEqual(s["timestamp"], time.time() - 5)

    def test_gb10_unified_memory_values_unchanged(self):
        s = self.collector(healthy()).collect()
        m = s["metrics"]
        self.assertEqual(m["mem_unified_used_bytes"], 1000)
        self.assertEqual(m["mem_unified_total_bytes"], 2000)
        # The unified gauge is never the GPU figure, and never a sum:
        self.assertNotEqual(m["mem_unified_used_bytes"],
                            m["gpu_vram_used_bytes"])
        self.assertLess(m["mem_unified_used_bytes"],
                        m["gpu_vram_used_bytes"])
        self.assertEqual(s["components"]["system"]["details"]
                         ["mem_unified_source"], "nvidia-smi")

    def test_installed_vs_resident_models_preserved(self):
        s = self.collector(healthy()).collect()
        ol = s["components"]["ollama"]
        self.assertEqual(ol["installed_models"], ["a:1", "b:2"])
        self.assertEqual(ol["resident_models"], ["b:2"])
        self.assertTrue(ol["models"]["a:1"]["installed"])
        self.assertFalse(ol["models"]["a:1"]["resident"])
        self.assertTrue(ol["models"]["b:2"]["resident"])
        self.assertEqual(ol["models"]["b:2"]["size_vram_bytes"], 400)
        # Flattened counters agree with the component detail:
        self.assertEqual(s["metrics"]["ollama_models_installed"], 2)
        self.assertEqual(s["metrics"]["ollama_models_resident"], 1)
        self.assertEqual(s["metrics"]["ollama_resident_bytes"], 400)

    def test_no_metric_name_collisions(self):
        s = self.collector(healthy()).collect()
        m = s["metrics"]
        expected_keys = (set(SM.METRIC_KEYS)
                         | set(NC._FALLBACK_METRIC_KEYS["ollama"])
                         | set(NC._FALLBACK_METRIC_KEYS["processes"]))
        self.assertEqual(set(m.keys()), expected_keys)
        self.assertFalse(any(k.startswith(("system_", "ollama_", "processes_"))
                             and not k.startswith("ollama_") for k in m))
        # No component-prefixed fallback keys appeared for healthy readers:
        for name in ("system", "processes"):
            for key in NC._FALLBACK_METRIC_KEYS[name]:
                self.assertNotIn("%s_%s" % (name, key), m)
        self.assertEqual(len(m), len(expected_keys))


class FailureIsolationTests(NodeCollectorTestCase):
    def test_ollama_unavailable_other_readers_unaffected(self):
        readers = healthy()
        readers["ollama"] = FakeReader(ollama_sample(
            status="unreachable", error="http://192.168.0.2:11434 ...",
            installed_models=None, resident_models=None,
            metrics={"ollama_up": 0, "ollama_models_installed": None,
                     "ollama_models_resident": None,
                     "ollama_resident_bytes": None,
                     "ollama_context_tokens_max": None}))
        s = self.collector(readers, "dgx02").collect()
        # Successful observation of an offline endpoint: still a complete
        # snapshot; the reader warning surfaces at the top level.
        self.assertEqual(s["status"], "ok")
        self.assertEqual(readers["system"].calls, 1)
        self.assertEqual(readers["processes"].calls, 1)
        self.assertEqual(readers["ollama"].calls, 1)
        self.assertEqual(s["components"]["system"]["metrics"]["cpu_percent"],
                         12.0)
        self.assertEqual(s["components"]["processes"]["metrics"]
                         ["ollama_daemon_count"], 1)
        self.assertEqual(s["metrics"]["ollama_up"], 0)
        self.assertIsNone(s["metrics"]["ollama_models_installed"])
        self.assertTrue(any("unreachable" in w for w in s["warnings"]))
        self.assertEqual(s["node_id"], "dgx02")

    def test_ollama_api_error_makes_node_partial(self):
        readers = healthy()
        readers["ollama"] = FakeReader(ollama_sample(
            status="error", error="/api/tags: malformed JSON"))
        s = self.collector(readers).collect()
        self.assertEqual(s["status"], "partial")
        self.assertTrue(any("ollama" in w for w in s["warnings"]))
        # Other telemetry is fully intact:
        self.assertEqual(s["metrics"]["cpu_percent"], 12.0)
        self.assertEqual(s["metrics"]["docker_container_count"], 0)
        # Failed component metrics fall back to all-null:
        self.assertIsNone(s["metrics"]["ollama_up"])
        self.assertIsNone(s["metrics"]["ollama_models_installed"])

    def test_exception_isolation_per_reader(self):
        readers = healthy()
        readers["processes"] = FakeReader(exc=OSError("proc gone"))
        s = self.collector(readers).collect()
        self.assertEqual(s["status"], "partial")
        self.assertNotIn("processes", s["components"])
        self.assertTrue(any("processes raised OSError" in w
                            for w in s["warnings"]))
        self.assertIsNone(s["metrics"]["ray_process_count"])
        self.assertIsNone(s["metrics"]["tmux_session_count"])
        # The remaining two components are fully present:
        self.assertEqual(s["components"]["system"]["metrics"]
                         ["mem_unified_total_bytes"], 2000)
        self.assertEqual(s["components"]["ollama"]["metrics"]
                         ["ollama_up"], 1)

    def test_all_readers_failing_is_error(self):
        readers = {
            "system": FakeReader(exc=RuntimeError("a")),
            "ollama": FakeReader(exc=RuntimeError("b")),
            "processes": FakeReader(exc=RuntimeError("c")),
        }
        s = self.collector(readers).collect()
        self.assertEqual(s["status"], "error")
        self.assertEqual(s["components"], {})
        self.assertEqual(len(s["warnings"]), 3)
        for key in NC._FALLBACK_METRIC_KEYS["system"]:
            self.assertIsNone(s["metrics"][key])
        for key in NC._FALLBACK_METRIC_KEYS["ollama"]:
            self.assertIsNone(s["metrics"][key])
        for key in NC._FALLBACK_METRIC_KEYS["processes"]:
            self.assertIsNone(s["metrics"][key])

    def test_process_incomplete_scan_still_ok_with_warning(self):
        readers = healthy()
        readers["processes"] = FakeReader(process_sample(
            warnings=["/proc scan incomplete: 2 unreadable pid(s); "
                      "process counts reported as null"]))
        # Rewrite the process metrics to nulls as the reader would:
        sample = readers["processes"]._sample
        for key in sample["metrics"]:
            if key != "docker_container_count":
                sample["metrics"][key] = None
        s = self.collector(readers).collect()
        self.assertEqual(s["status"], "ok")
        self.assertIsNone(s["metrics"]["ray_process_count"])
        self.assertEqual(s["metrics"]["docker_container_count"], 0)
        self.assertTrue(any("unreadable pid" in w for w in s["warnings"]))


class SystemDegradedTests(NodeCollectorTestCase):
    def test_gpu_telemetry_error_makes_node_partial(self):
        readers = healthy()
        readers["system"] = FakeReader(system_sample(
            {"gpu_percent": None, "gpu_temp_c": None,
             "cpu_percent": 5.0}, gpu_state="error"))
        s = self.collector(readers).collect()
        self.assertEqual(s["status"], "partial")
        self.assertTrue(any("gpu telemetry error" in w
                            for w in s["warnings"]))
        # Non-GPU system telemetry and other components are preserved:
        self.assertEqual(s["metrics"]["cpu_percent"], 5.0)
        self.assertIsNone(s["metrics"]["gpu_temp_c"])
        self.assertEqual(s["metrics"]["ollama_up"], 1)
        self.assertEqual(s["metrics"]["ollama_daemon_count"], 1)

    def test_system_no_metrics_map_is_partial(self):
        readers = healthy()
        readers["system"] = FakeReader({"timestamp": 1.0,
                                        "details": {}})
        s = self.collector(readers).collect()
        self.assertEqual(s["status"], "partial")
        self.assertTrue(any("no metrics map" in w for w in s["warnings"]))
        self.assertIsNone(s["metrics"]["cpu_percent"])


class ConfigurationTests(NodeCollectorTestCase):
    def test_correct_node_identity_and_endpoint(self):
        expected = {
            "dgx01": "http://127.0.0.1:11434",
            "dgx02": "http://192.168.0.2:11434",
            "dgx03": "http://192.168.2.2:11434",
        }
        for node_id, endpoint in expected.items():
            c = NC.NodeCollector(node_id=node_id, **healthy())
            self.assertEqual(c.node_id, node_id)
            self.assertEqual(c.base_url, endpoint)
            self.assertEqual(c.collect()["node_id"], node_id)
        with self.assertRaises(ValueError):
            NC.NodeCollector(node_id="dgx09", **healthy())
        with self.assertRaises(ValueError):
            NC.NodeCollector(**healthy())  # no identity at all

    def test_config_file_identity(self):
        import tempfile
        with tempfile.NamedTemporaryFile("w", suffix=".json",
                                         delete=False) as fh:
            json.dump({"node_id": "dgx03",
                       "ollama_endpoint": "http://192.168.2.2:11434"}, fh)
            path = fh.name
        self.addCleanup(os.unlink, path)
        c = NC.NodeCollector(config_path=path, **healthy())
        self.assertEqual(c.node_id, "dgx03")
        self.assertEqual(c.base_url, "http://192.168.2.2:11434")

    def test_injected_base_url_must_still_validate(self):
        with self.assertRaises(ValueError):
            NC.NodeCollector(node_id="dgx01", base_url="http://h:1//x",
                             **healthy())


class ShapeAndSafetyTests(NodeCollectorTestCase):
    def test_json_serializable_no_nan(self):
        readers = healthy()
        sample = system_sample({"load_1": float("nan"),
                                "disk_read_bps": float("inf")})
        readers["system"] = FakeReader(sample)
        s = self.collector(readers).collect()
        blob = json.dumps(s, allow_nan=False)   # raises on NaN/Infinity
        clone = json.loads(blob)
        self.assertIsNone(clone["metrics"]["load_1"])
        self.assertIsNone(clone["metrics"]["disk_read_bps"])
        self.assertEqual(clone["schema_version"], 1)

    def test_collision_explicitly_resolved(self):
        clash = process_sample()
        clash["metrics"]["cpu_percent"] = 99.0  # same key the system uses
        readers = {"system": FakeReader(system_sample()),
                   "ollama": FakeReader(ollama_sample()),
                   "processes": FakeReader(clash)}
        s = self.collector(readers).collect()
        m = s["metrics"]
        self.assertEqual(m["cpu_percent"], 12.0)      # system keeps bare key
        self.assertEqual(m["processes_cpu_percent"], 99.0)
        self.assertTrue(any("collision" in w for w in s["warnings"]))
        self.assertEqual(len(m), len(NC._FALLBACK_METRIC_KEYS["system"]) +
                         len(NC._FALLBACK_METRIC_KEYS["ollama"]) +
                         len(NC._FALLBACK_METRIC_KEYS["processes"]) + 1)


class BoundedAndMutationTests(NodeCollectorTestCase):
    def test_sampling_is_bounded(self):
        import time as _t

        class SlowReader(FakeReader):
            def __init__(self, delay, sample):
                super().__init__(sample=sample)
                self.delay = delay

            def collect(self):
                self.calls += 1
                _t.sleep(self.delay)
                return self._sample

        readers = {
            "system": SlowReader(0.2, system_sample()),
            "ollama": SlowReader(0.2, ollama_sample()),
            "processes": SlowReader(0.2, process_sample()),
        }
        started = _t.time()
        s = self.collector(readers).collect()
        elapsed = _t.time() - started
        self.assertEqual(s["status"], "ok")
        self.assertLess(elapsed, 10.0)   # well under the reader budgets
        self.assertLessEqual(s["timestamp"], started + 0.001)

    def test_default_reader_timeouts_are_small(self):
        import collectors.ollama_metrics as OL
        import collectors.process_metrics as PM
        self.assertLess(OL.DEFAULT_TIMEOUT_SECONDS, 10.0)
        c = NC.NodeCollector("dgx01")
        self.assertIsInstance(c._system, SM.SystemMetrics)
        self.assertIsInstance(c._ollama, OL.OllamaReader)
        self.assertIsInstance(c._processes, PM.ProcessMetrics)
        self.assertLess(c._ollama.timeout, 10.0)

    def test_no_inference_or_mutation_requests(self):
        import urllib.request

        class Guard:
            def __init__(self):
                self.requests = []

            def __call__(self, *args, **kwargs):
                self.requests.append((args, kwargs))
                raise AssertionError("network disabled in this test")

        guard = Guard()
        readers = healthy()
        with unittest.mock.patch("urllib.request.urlopen", guard):
            self.collector(readers).collect()
        self.assertEqual(guard.requests, [])


class LiveDgx01SmokeTests(unittest.TestCase):
    """Read-only live snapshot on this node (DGX01)."""

    def test_live_dgx01_snapshot(self):
        c = NC.NodeCollector(node_id="dgx01")
        s = c.collect()
        self.assertEqual(s["schema_version"], 1)
        self.assertEqual(s["node_id"], "dgx01")
        self.assertIn(s["status"], ("ok", "partial"))
        blob = json.dumps(s, allow_nan=False)
        self.assertLess(len(blob), 512 * 1024)
        m = s["metrics"]
        # GB10 invariant: unified gauge present, not the tiny GPU figure
        self.assertIsNotNone(m["mem_unified_total_bytes"])
        self.assertGreater(m["mem_unified_total_bytes"], 100 * 2**30)
        self.assertLess(m["mem_unified_used_bytes"],
                        m["mem_unified_total_bytes"] + 1)
        self.assertIsNone(m["gpu_vram_used_bytes"])  # [N/A] on this driver
        self.assertGreater(m["ollama_up"], 0)
        self.assertIsNotNone(m["ollama_models_installed"])
        self.assertLessEqual(m["ollama_models_resident"],
                             m["ollama_models_installed"])
        self.assertGreaterEqual(m["ollama_daemon_count"], 1)
        # Component detail agrees with flattened counters:
        self.assertEqual(
            len(s["components"]["ollama"]["installed_models"]),
            m["ollama_models_installed"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
