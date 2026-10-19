"""Tests for cluster.service_metrics (service-level observability).

All vLLM and I/O is mocked; no network, no live services, no inference.
"""

import json
import os
import sys
import tempfile
import unittest
from unittest.mock import MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from cluster.service_metrics import (
    collect_service_metrics,
    read_gateway_metrics,
    HEAVY_EXPECTED_MODELS,
    SERVICE_SCHEMA_VERSION,
)


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

def vllm_sample(status="ok", served_model=None, timestamp=1000.0,
                metrics=None, details=None, error=None, health=None):
    """Build a dict shaped like VllmReader.collect()."""
    sample = {
        "timestamp": timestamp,
        "status": status,
        "error": error,
        "health": health,
        "served_model": served_model,
        "metrics": metrics or {"vllm_up": 1},
        "details": details or {"expected_mode": "split", "http": {}, "warnings": []},
    }
    return sample


def make_reader(sample):
    """A fake vLLM reader whose .collect() returns the given sample."""
    reader = MagicMock()
    reader.collect.return_value = sample
    return reader


def split_state(mode="split"):
    """Aggregator snapshot with a cluster block."""
    return {"cluster": {"mode": mode, "controller": "ok"}}


def gw_file_content(ts=1000.0, counters=None):
    """Valid gateway metrics file content (version 1)."""
    doc = {"version": 1, "ts": ts, "in_flight": 0,
           "requests_total": 100, "latency_ms": {"p50": 10, "p95": 50, "max": 100}}
    if counters is not None:
        doc["counters"] = counters
    return doc


def write_gw_file(tmpdir, content, name="metrics.json"):
    path = os.path.join(tmpdir, name)
    with open(path, "w") as fh:
        fh.write(content if isinstance(content, str) else json.dumps(content))
    return path


class SMBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmpdir = self._tmp.name

    def tearDown(self):
        self._tmp.cleanup()


class SplitModeTests(SMBase):
    """vLLM inactive in split/stop modes: no warning expected."""

    def test_split_vllm_inactive_no_warning(self):
        reader = make_reader(vllm_sample(
            status="inactive", served_model=None, health=False,
            metrics={"vllm_up": 0}))
        gw_path = write_gw_file(self.tmpdir, gw_file_content(ts=1000.0))
        result = collect_service_metrics(
            model_state=split_state("split"),
            vllm_reader=reader,
            gateway_metrics_path=gw_path,
            clock=lambda: 1000.0)
        self.assertEqual(result["cluster_mode"], "split")
        self.assertEqual(result["vllm"]["status"], "inactive")
        # No vLLM-related warning for a correctly-inactive API in split mode.
        vllm_warnings = [w for w in result["warnings"] if "vLLM" in w]
        self.assertEqual(vllm_warnings, [])

    def test_transition_vllm_starting(self):
        reader = make_reader(vllm_sample(
            status="starting", served_model=None,
            details={"expected_mode": "transition", "http": "503",
                     "warnings": ["cluster transition in progress"]}))
        gw_path = write_gw_file(self.tmpdir, gw_file_content(ts=1000.0))
        result = collect_service_metrics(
            model_state=split_state("transitioning"),
            vllm_reader=reader,
            gateway_metrics_path=gw_path,
            clock=lambda: 1000.0)
        self.assertEqual(result["cluster_mode"], "transitioning")
        self.assertEqual(result["vllm"]["status"], "starting")
        # Transition mode has no expected model; no mismatch warning expected.
        self.assertNotIn(result["vllm"]["model_identity"], ("mismatch",))


class HeavyModelTests(SMBase):
    """Correct served model in heavy modes: model_identity verified."""

    def test_heavy_qwen_correct_model(self):
        reader = make_reader(vllm_sample(
            status="ok", served_model="heavy-qwen", health=True,
            metrics={"vllm_up": 1, "vllm_running_reqs": 1}))
        gw_path = write_gw_file(self.tmpdir, gw_file_content(ts=1000.0))
        result = collect_service_metrics(
            model_state=split_state("heavy-qwen"),
            vllm_reader=reader,
            gateway_metrics_path=gw_path,
            clock=lambda: 1000.0)
        self.assertEqual(result["vllm"]["served_model"], "heavy-qwen")
        self.assertEqual(result["vllm"]["model_identity"], "verified")
        self.assertFalse(any("mismatch" in w for w in result["warnings"]))

    def test_heavy_deepseek_correct_model(self):
        reader = make_reader(vllm_sample(
            status="ok", served_model="deepseek-v4-flash-0731", health=True))
        gw_path = write_gw_file(self.tmpdir, gw_file_content(ts=1000.0))
        result = collect_service_metrics(
            model_state=split_state("heavy-deepseek"),
            vllm_reader=reader,
            gateway_metrics_path=gw_path,
            clock=lambda: 1000.0)
        self.assertEqual(result["vllm"]["served_model"], "deepseek-v4-flash-0731")
        self.assertEqual(result["vllm"]["model_identity"], "verified")
        self.assertFalse(any("mismatch" in w for w in result["warnings"]))

    def test_wrong_served_model_mismatch_warning(self):
        reader = make_reader(vllm_sample(
            status="ok", served_model="wrong-model", health=True))
        gw_path = write_gw_file(self.tmpdir, gw_file_content(ts=1000.0))
        result = collect_service_metrics(
            model_state=split_state("heavy-qwen"),
            vllm_reader=reader,
            gateway_metrics_path=gw_path,
            clock=lambda: 1000.0)
        self.assertEqual(result["vllm"]["served_model"], "wrong-model")
        self.assertEqual(result["vllm"]["model_identity"], "mismatch")
        self.assertTrue(any("mismatch" in w for w in result["warnings"]))


class NonHeavyActivityTests(SMBase):
    """Non-heavy mode with unexpected vLLM activity produces a warning."""

    def test_non_heavy_unexpected_vllm_active(self):
        reader = make_reader(vllm_sample(
            status="ok", served_model="some-model", health=True))
        gw_path = write_gw_file(self.tmpdir, gw_file_content(ts=1000.0))
        result = collect_service_metrics(
            model_state=split_state("split"),
            vllm_reader=reader,
            gateway_metrics_path=gw_path,
            clock=lambda: 1000.0)
        # In split mode with vLLM status ok -> unexpected
        self.assertTrue(any("non-heavy mode" in w for w in result["warnings"]))

    def test_non_heavy_vllm_unreachable_warns(self):
        reader = make_reader(vllm_sample(
            status="unreachable", served_model=None,
            error="connection refused"))
        gw_path = write_gw_file(self.tmpdir, gw_file_content(ts=1000.0))
        result = collect_service_metrics(
            model_state=split_state("split"),
            vllm_reader=reader,
            gateway_metrics_path=gw_path,
            clock=lambda: 1000.0)
        # unreachable is not 'inactive' so a warning is expected
        self.assertTrue(any("non-heavy mode" in w for w in result["warnings"]))


class GatewayTests(SMBase):
    """Gateway metrics file: present, absent, malformed, stale."""

    def test_gateway_metadata_present(self):
        counters = {"requests_total": 100, "errors:model-x": 3,
                    "tokens_in:model-x": 100, "tokens_out:model-x": 200,
                    "status_200:model-x": 97}
        gw_path = write_gw_file(self.tmpdir,
                                gw_file_content(ts=1000.0, counters=counters))
        reader = make_reader(vllm_sample(status="inactive"))
        result = collect_service_metrics(
            model_state=split_state("split"),
            vllm_reader=reader,
            gateway_metrics_path=gw_path,
            clock=lambda: 1000.0)
        self.assertTrue(result["gateway"]["available"])
        self.assertFalse(result["gateway"]["stale"])
        m = result["gateway"]["metrics"]
        self.assertEqual(m["requests_total"], 100)
        self.assertEqual(m["errors_by_model"]["model-x"], 3)
        self.assertEqual(m["tokens_in_by_model"]["model-x"], 100)
        self.assertEqual(m["tokens_out_by_model"]["model-x"], 200)
        self.assertEqual(m["status_by_code"]["200"], 97)

    def test_gateway_file_absent(self):
        gw_path = os.path.join(self.tmpdir, "does_not_exist.json")
        reader = make_reader(vllm_sample(status="inactive"))
        result = collect_service_metrics(
            model_state=split_state("split"),
            vllm_reader=reader,
            gateway_metrics_path=gw_path,
            clock=lambda: 1000.0)
        self.assertFalse(result["gateway"]["available"])
        self.assertTrue(result["gateway"]["stale"])
        self.assertIsNotNone(result["gateway"]["metrics"]["requests_total"]
                             if False else result["gateway"]["error"])
        self.assertIn("not found", result["gateway"]["error"])
        # vLLM still works independently
        self.assertEqual(result["vllm"]["status"], "inactive")

    def test_gateway_malformed_json(self):
        gw_path = write_gw_file(self.tmpdir, "this is not json at all {")
        reader = make_reader(vllm_sample(status="inactive"))
        result = collect_service_metrics(
            model_state=split_state("split"),
            vllm_reader=reader,
            gateway_metrics_path=gw_path,
            clock=lambda: 1000.0)
        self.assertFalse(result["gateway"]["available"])
        self.assertEqual(result["gateway"]["error"],
                         "gateway metrics file is not valid JSON")

    def test_gateway_wrong_version(self):
        gw_path = write_gw_file(self.tmpdir,
                                {"version": 99, "ts": 1000.0,
                                 "requests_total": 5})
        reader = make_reader(vllm_sample(status="inactive"))
        result = collect_service_metrics(
            model_state=split_state("split"),
            vllm_reader=reader,
            gateway_metrics_path=gw_path,
            clock=lambda: 1000.0)
        self.assertFalse(result["gateway"]["available"])
        self.assertIn("unsupported", result["gateway"]["error"])

    def test_gateway_stale_file(self):
        # ts is 60s in the past, threshold is 30s
        gw_path = write_gw_file(self.tmpdir, gw_file_content(ts=940.0))
        reader = make_reader(vllm_sample(status="inactive"))
        result = collect_service_metrics(
            model_state=split_state("split"),
            vllm_reader=reader,
            gateway_metrics_path=gw_path,
            clock=lambda: 1000.0,
            stale_threshold_seconds=30.0)
        self.assertTrue(result["gateway"]["available"])
        self.assertTrue(result["gateway"]["stale"])
        self.assertTrue(any("stale" in w for w in result["warnings"]))


class IndependenceTests(SMBase):
    """vLLM and gateway failures are independent of each other."""

    def test_vllm_fails_gateway_ok(self):
        reader = MagicMock()
        reader.collect.side_effect = Exception("connection refused")
        gw_path = write_gw_file(self.tmpdir, gw_file_content(ts=1000.0))
        result = collect_service_metrics(
            model_state=split_state("split"),
            vllm_reader=reader,
            gateway_metrics_path=gw_path,
            clock=lambda: 1000.0)
        # vLLM is unavailable with error
        self.assertFalse(result["vllm"]["available"])
        self.assertIsNotNone(result["vllm"]["error"])
        # gateway still works
        self.assertTrue(result["gateway"]["available"])
        # result is still JSON-serializable
        json.dumps(result, allow_nan=False)

    def test_gateway_fails_vllm_ok(self):
        reader = make_reader(vllm_sample(status="ok", served_model="heavy-qwen"))
        gw_path = os.path.join(self.tmpdir, "nonexistent.json")
        result = collect_service_metrics(
            model_state=split_state("heavy-qwen"),
            vllm_reader=reader,
            gateway_metrics_path=gw_path,
            clock=lambda: 1000.0)
        self.assertTrue(result["vllm"]["available"])
        self.assertEqual(result["vllm"]["status"], "ok")
        self.assertFalse(result["gateway"]["available"])

    def test_both_fail(self):
        reader = MagicMock()
        reader.collect.side_effect = ConnectionError("refused")
        gw_path = os.path.join(self.tmpdir, "gone.json")
        result = collect_service_metrics(
            model_state=split_state("split"),
            vllm_reader=reader,
            gateway_metrics_path=gw_path,
            clock=lambda: 1000.0)
        self.assertFalse(result["vllm"]["available"])
        self.assertFalse(result["gateway"]["available"])
        json.dumps(result, allow_nan=False)


class NullValueTests(SMBase):
    """Missing values stay null (None), never zero-filled."""

    def test_gateway_missing_values_are_none(self):
        # File with only version and ts
        path = write_gw_file(self.tmpdir, {"version": 1, "ts": 1000.0})
        reader = make_reader(vllm_sample(status="inactive"))
        result = collect_service_metrics(
            model_state=split_state("split"),
            vllm_reader=reader,
            gateway_metrics_path=path,
            clock=lambda: 1000.0)
        m = result["gateway"]["metrics"]
        self.assertIsNone(m["requests_total"])
        self.assertIsNone(m["in_flight"])
        self.assertIsNone(m["since_start_seconds"])
        self.assertIsNone(m["latency_ms"]["p50"])
        self.assertIsNone(m["latency_ms"]["p95"])
        self.assertIsNone(m["latency_ms"]["max"])

    def test_vllm_unavailable_fields_are_none(self):
        reader = MagicMock()
        reader.collect.side_effect = Exception("boom")
        gw_path = write_gw_file(self.tmpdir, gw_file_content(ts=1000.0))
        result = collect_service_metrics(
            model_state=split_state("split"),
            vllm_reader=reader,
            gateway_metrics_path=gw_path,
            clock=lambda: 1000.0)
        v = result["vllm"]
        self.assertIsNone(v["served_model"])
        self.assertIsNone(v["health"])
        self.assertIsNone(v["metrics"])
        self.assertIsNone(v["sample_ts"])


class SerializationTests(SMBase):
    """Complete JSON round-trip for the result."""

    def test_json_serializable_roundtrip(self):
        counters = {"requests_total": 10, "status_200:m": 10}
        gw_path = write_gw_file(self.tmpdir,
                                gw_file_content(ts=1000.0, counters=counters))
        reader = make_reader(vllm_sample(status="ok", served_model="heavy-qwen",
                                         metrics={"vllm_up": 1}))
        result = collect_service_metrics(
            model_state=split_state("heavy-qwen"),
            vllm_reader=reader,
            gateway_metrics_path=gw_path,
            clock=lambda: 1000.0)
        blob = json.dumps(result, allow_nan=False)
        restored = json.loads(blob)
        self.assertEqual(restored, result)
        self.assertEqual(result["schema_version"], SERVICE_SCHEMA_VERSION)


class NoMutationTests(SMBase):
    """No cluster mutations or inference requests are made."""

    def test_no_inference_requests(self):
        """The vLLM reader is only .collect()ed; no /v1/completions."""
        reader = make_reader(vllm_sample(status="ok", served_model="heavy-qwen"))
        gw_path = write_gw_file(self.tmpdir, gw_file_content(ts=1000.0))
        result = collect_service_metrics(
            model_state=split_state("heavy-qwen"),
            vllm_reader=reader,
            gateway_metrics_path=gw_path,
            clock=lambda: 1000.0)
        # reader.collect called exactly once, no other methods
        reader.collect.assert_called_once()
        self.assertEqual(len(reader.method_calls), 1)
        self.assertEqual(reader.method_calls[0][0], "collect")

    def test_model_state_not_mutated(self):
        state = split_state("heavy-qwen")
        original = json.dumps(state)
        reader = make_reader(vllm_sample(status="ok", served_model="heavy-qwen"))
        gw_path = write_gw_file(self.tmpdir, gw_file_content(ts=1000.0))
        collect_service_metrics(
            model_state=state,
            vllm_reader=reader,
            gateway_metrics_path=gw_path,
            clock=lambda: 1000.0)
        self.assertEqual(json.dumps(state), original)

    def test_no_cluster_controller_subprocess(self):
        """When model_state is provided, no controller subprocess is invoked."""
        from cluster import ClusterModelController
        import unittest.mock as mock
        with mock.patch.object(ClusterModelController, "status") as m:
            reader = make_reader(vllm_sample(status="inactive"))
            gw_path = write_gw_file(self.tmpdir, gw_file_content(ts=1000.0))
            collect_service_metrics(
                model_state=split_state("split"),
                vllm_reader=reader,
                gateway_metrics_path=gw_path,
                clock=lambda: 1000.0)
            self.assertFalse(m.called)


if __name__ == "__main__":
    unittest.main()
