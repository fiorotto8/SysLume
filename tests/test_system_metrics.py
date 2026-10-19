"""Tests for collectors.system_metrics (GB10-aware system reader)."""
import importlib.util
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


mon = _load("mon_under_test", ROOT / "monitor.py")
sm = _load("sm_under_test", ROOT / "collectors" / "system_metrics.py")


class StubLinux:
    def __init__(self, metrics, details=None):
        self._metrics = metrics
        self._details = details or {"cpu_cores": [], "network_interfaces": []}

    def sample(self):
        return dict(self._metrics), dict(self._details)


class StubGpu:
    def __init__(self, metrics, status=None, details=None):
        self._metrics = metrics
        self._status = status or {"available": False, "state": "absent", "message": ""}

    def sample(self):
        return dict(self._metrics), dict(self._status)


def make_collector(linux, gpu):
    collector = sm.SystemMetrics(sm.default_config())
    collector.linux, collector.gpu = linux, gpu
    return collector


GiB = 1024 ** 3


class UnifiedMemoryTests(unittest.TestCase):
    def test_prefers_nvidia_sm_view(self):
        nvidia = {"gpu_vram_used_bytes": 60 * GiB, "gpu_vram_total_bytes": 121 * GiB}
        linux = {"ram_used_bytes": 55 * GiB, "ram_total_bytes": 128 * GiB}
        used, total, source = sm.unified_memory(linux, nvidia)
        self.assertEqual((used, total, source), (60 * GiB, 121 * GiB, "nvidia-smi"))

    def test_falls_back_to_meminfo_when_nvidia_na(self):
        # Live GB10 driver reports memory.used/total as [N/A] -> None.
        nvidia = {"gpu_vram_used_bytes": None, "gpu_vram_total_bytes": None}
        linux = {"ram_used_bytes": 90 * GiB, "ram_total_bytes": 137 * GiB}
        used, total, source = sm.unified_memory(linux, nvidia)
        self.assertEqual((used, total, source), (90 * GiB, 137 * GiB, "meminfo"))

    def test_never_sums_gpu_and_ram(self):
        nvidia = {"gpu_vram_used_bytes": 60 * GiB, "gpu_vram_total_bytes": 121 * GiB}
        linux = {"ram_used_bytes": 55 * GiB, "ram_total_bytes": 128 * GiB}
        used, total, _ = sm.unified_memory(linux, nvidia)
        self.assertNotEqual(total, 121 * GiB + 128 * GiB)
        self.assertNotEqual(used, 60 * GiB + 55 * GiB)
        self.assertEqual(total, 121 * GiB)

    def test_all_unavailable_is_null(self):
        used, total, source = sm.unified_memory({}, {})
        self.assertEqual((used, total, source), (None, None, None))


class SampleAssemblyTests(unittest.TestCase):
    def test_all_keys_present_and_nulls_preserved(self):
        collector = make_collector(StubLinux({}), StubGpu({}))
        metrics, details = collector.sample()
        self.assertEqual(set(metrics), set(sm.METRIC_KEYS))
        self.assertTrue(all(v is None for v in metrics.values()))
        self.assertIsNone(details["mem_unified_source"])
        self.assertFalse(details["gpu_status"]["available"])

    def test_gpu_and_linux_fields_populated(self):
        linux = {"ram_used_bytes": 1.0, "ram_total_bytes": 2.0, "cpu_percent": 12.5}
        gpu = {"gpu_percent": 42.0, "gpu_temp_c": 61.0}
        collector = make_collector(StubLinux(linux), StubGpu(gpu))
        metrics, _ = collector.sample()
        self.assertEqual(metrics["cpu_percent"], 12.5)
        self.assertEqual(metrics["ram_total_bytes"], 2.0)
        self.assertEqual(metrics["gpu_percent"], 42.0)
        self.assertEqual(metrics["gpu_temp_c"], 61.0)
        # GB10 [N/A] driver: unified view comes from meminfo, not a sum.
        self.assertEqual(metrics["mem_unified_used_bytes"], 1.0)
        self.assertEqual(metrics["mem_unified_total_bytes"], 2.0)

    def test_collect_is_json_serializable(self):
        linux = {"ram_used_bytes": 1.0, "ram_total_bytes": 2.0}
        collector = make_collector(StubLinux(linux), StubGpu({}))
        sample = collector.collect(timestamp=123.0)
        self.assertEqual(sample["timestamp"], 123.0)
        self.assertEqual(set(sample), {"timestamp", "metrics", "details"})
        round_tripped = json.loads(json.dumps(sample))
        self.assertEqual(round_tripped["metrics"]["mem_unified_total_bytes"], 2.0)


class LiveSmokeTests(unittest.TestCase):
    def test_live_sample_on_real_node(self):
        if not Path("/proc/meminfo").is_file():
            self.skipTest("/proc not available")
        collector = sm.SystemMetrics(sm.default_config())
        first = collector.sample()
        second = collector.sample()
        metrics = second[0]
        self.assertEqual(set(metrics), set(sm.METRIC_KEYS))
        self.assertTrue(all(
            v is None or (isinstance(v, (int, float)) and v == v) for v in metrics.values()))
        self.assertIsNotNone(metrics["ram_total_bytes"])
        self.assertIsNotNone(metrics["mem_unified_total_bytes"])
        self.assertGreaterEqual(metrics["mem_unified_used_bytes"], 0)
        self.assertLessEqual(metrics["mem_unified_used_bytes"],
                             metrics["mem_unified_total_bytes"])
        # Second sample must have resolved CPU utilisation from the delta.
        self.assertIsNotNone(metrics["cpu_percent"])
        self.assertIn(second[1]["mem_unified_source"], ("nvidia-smi", "meminfo"))

