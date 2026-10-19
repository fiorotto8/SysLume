"""Read-only service-level observability for the SysLume DGX cluster (DGX01).

Complements the per-node telemetry of :mod:`cluster.aggregator` with two
service components, collected independently so each can fail on its own:

* **vLLM (DGX02 only)** -- the heavy-inference API at the fixed endpoint
  ``http://192.168.0.2:8000``.  Collected with the validated
  :class:`collectors.heavy_metrics.VllmReader` (read-only ``/health``,
  ``/v1/models``, ``/metrics``; never an inference call).  The expected
  API state and physical served-model identity come from the same
  authoritative cluster-model state the aggregator already read -- pass
  that snapshot (or a raw cluster-model status object) as ``model_state``
  so the controller is not read a second time.

* **Inference API gateway (DGX01)** -- the metadata-only metrics file the
  gateway writes at the fixed path
  ``~/.local/state/cygno-api-gateway/metrics.json``.  The reader is built
  against that file's *actual* schema (gateway.py ``MetricsWriter``):
  ``version`` / ``ts`` / ``since_start_seconds`` / ``in_flight`` /
  ``requests_total`` / ``latency_ms{p50,p95,max}`` / ``counters`` with
  cumulative keys ``requests_total``, ``status_<code>:<model>``,
  ``errors:<model>``, ``tokens_in:<model>``, ``tokens_out:<model>``.

Guarantees:

* JSON-serializable output; missing values stay ``null``; cumulative
  counters stay cumulative (rates are derived later from timestamped
  samples).
* No prompts, model responses, credentials, reasoning content, or request
  bodies are read or copied -- only the gateway's own metadata counters.
* A missing/unreadable gateway file never prevents the vLLM component
  (and vice versa).  No storage, no services, no state writes.
"""

from __future__ import annotations

import json
import math
import os
import re
import time

from .aggregator import ControllerError, ClusterModelController, _jsonify
from collectors.heavy_metrics import (
    DEFAULT_TIMEOUT_SECONDS as DEFAULT_VLLM_TIMEOUT_SECONDS,
    VllmReader,
)

__all__ = [
    "collect_service_metrics",
    "read_gateway_metrics",
    "HEAVY_EXPECTED_MODELS",
    "SERVICE_SCHEMA_VERSION",
    "DEFAULT_VLLM_BASE_URL",
    "DEFAULT_GATEWAY_METRICS_PATH",
    "DEFAULT_STALE_THRESHOLD_SECONDS",
]

#: Version of this module's own result schema (not the node snapshots').
SERVICE_SCHEMA_VERSION = 1

#: Fixed, trusted vLLM endpoint (DGX02 only; DGX03 is a rank-1 worker with
#: no API and is never probed here).
DEFAULT_VLLM_BASE_URL = "http://192.168.0.2:8000"

#: Fixed, trusted gateway metrics file (written by gateway.py MetricsWriter
#: on DGX01; the gateway snapshots every 10 s by default).
DEFAULT_GATEWAY_METRICS_PATH = os.path.expanduser(
    "~/.local/state/cygno-api-gateway/metrics.json")

#: Default vLLM probe timeout and staleness window (gateway snapshots every
#: 10 s, so 3x that is a safe "stale" line for its file; the live vLLM probe
#: is same-cycle and will normally never reach the window).
DEFAULT_STALE_THRESHOLD_SECONDS = 30.0

#: heavy cluster mode -> physical served model id the vLLM API must report.
#: (Logical alias heavy-deepseek serves the physical DeepSeek model.)
HEAVY_EXPECTED_MODELS = {
    "heavy-qwen": "heavy-qwen",
    "heavy-deepseek": "deepseek-v4-flash-0731",
}

_STATUS_COUNTER = re.compile(r"^status_(\d+):(.+)$")
_ERRORS_COUNTER = re.compile(r"^errors:(.+)$")
_TOKENS_IN_COUNTER = re.compile(r"^tokens_in:(.+)$")
_TOKENS_OUT_COUNTER = re.compile(r"^tokens_out:(.+)$")


def _numeric(value):
    """Finite int/float (bools are not numbers); anything else is None."""
    if isinstance(value, (int, float)) and not isinstance(value, bool) \
            and math.isfinite(value):
        return value
    return None


def _sanitize_text(text):
    if not isinstance(text, str):
        text = str(text)
    return " ".join(text.split())[:200]


def _mode_from_state(model_state):
    """``(mode, source)`` from an existing authoritative cluster state.

    Accepts a :meth:`cluster.aggregator.ClusterAggregator.collect` snapshot
    (dict with a ``cluster`` block) or a raw cluster-model status object
    (dict with ``mode``).  The mode is taken verbatim -- never guessed.
    """
    if not isinstance(model_state, dict):
        return "unknown", "invalid"
    cluster = model_state.get("cluster")
    if isinstance(cluster, dict) and ("mode" in cluster
                                      or "controller" in cluster):
        mode = cluster.get("mode")
        return (mode if isinstance(mode, str) and mode else "unknown"), \
            "aggregator-snapshot"
    if "mode" in model_state:
        mode = model_state.get("mode")
        return (mode if isinstance(mode, str) and mode else "unknown"), \
            "cluster-status"
    return "unknown", "invalid"


# --- gateway metrics file reader ------------------------------------------

def _empty_gateway_metrics():
    """Chartable gateway fields; None means unavailable (never zero-filled)."""
    return {
        "requests_total": None,        # cumulative
        "errors_total": None,          # cumulative (sum of errors:*)
        "in_flight": None,             # gauge
        "since_start_seconds": None,   # gauge (gateway uptime)
        "requests_by_model": {},       # cumulative, per model
        "errors_by_model": {},         # cumulative, per model
        "tokens_in_by_model": {},      # cumulative, per model
        "tokens_out_by_model": {},     # cumulative, per model
        "status_by_code": {},          # cumulative, per HTTP status
        "latency_ms": {"p50": None, "p95": None, "max": None},
        "counters": {},                # raw cumulative counters, verbatim
    }


def read_gateway_metrics(path, now=None,
                         stale_threshold_seconds=DEFAULT_STALE_THRESHOLD_SECONDS):
    """Read the gateway's metadata-only metrics file (fixed trusted path).

    Never raises.  Returns a JSON-safe dict:

    * ``available``  -- file read and schema-valid (version 1, object);
    * ``stale``      -- ``ts`` older than ``stale_threshold_seconds``, or the
      file has no usable ``ts`` (missing files report stale=True too: no
      fresh observation exists);
    * ``ts`` / ``age_seconds`` -- the file's own timestamp (same-host clock);
    * ``metrics``    -- request counts, errors, in-flight, per-model usage,
      latency observations, the raw cumulative counters verbatim; missing
      values stay ``null``;
    * ``error`` / ``warnings``.

    Only the file's metadata counters are read; no prompt or response
    content is ever present in this file or copied here.
    """
    warnings = []
    out = {"available": False, "stale": True, "ts": None, "age_seconds": None,
           "error": None, "metrics": _empty_gateway_metrics(), "warnings": warnings}
    if now is None:
        now = time.time()
    if not isinstance(path, str) or not path:
        out["error"] = "gateway metrics path must be a non-empty string"
        return out
    try:
        with open(path, "r", encoding="utf-8") as fh:
            raw = fh.read()
    except FileNotFoundError:
        out["error"] = "gateway metrics file not found"
        return out
    except OSError as exc:
        out["error"] = "gateway metrics file unreadable (%s)" % type(exc).__name__
        return out
    try:
        doc = json.loads(raw)
    except ValueError:
        out["error"] = "gateway metrics file is not valid JSON"
        warnings.append("gateway metrics file could not be parsed; content ignored")
        return out
    if not isinstance(doc, dict):
        out["error"] = "gateway metrics document is not a JSON object"
        return out
    version = doc.get("version")
    if isinstance(version, bool) or not isinstance(version, int) or version != 1:
        out["error"] = "unsupported gateway metrics version %r" % (version,)
        return out
    out["available"] = True

    ts = _numeric(doc.get("ts"))
    out["ts"] = ts
    if ts is None:
        warnings.append("gateway metrics document has no usable 'ts'; treated as stale")
    else:
        age = now - ts
        if age < 0:
            warnings.append("gateway metrics 'ts' is ahead of the local clock by "
                            "%.1fs; age clamped to 0" % -age)
            age = 0.0
        out["age_seconds"] = age
        if age > stale_threshold_seconds:
            out["stale"] = True
            warnings.append("gateway metrics file is stale (age %.1fs > %.1fs; "
                            "gateway may be stopped)" % (age, stale_threshold_seconds))
        else:
            out["stale"] = False

    metrics = _empty_gateway_metrics()
    metrics["requests_total"] = _numeric(doc.get("requests_total"))
    metrics["in_flight"] = _numeric(doc.get("in_flight"))
    metrics["since_start_seconds"] = _numeric(doc.get("since_start_seconds"))
    latency = doc.get("latency_ms")
    if isinstance(latency, dict):
        metrics["latency_ms"] = {
            "p50": _numeric(latency.get("p50")),
            "p95": _numeric(latency.get("p95")),
            "max": _numeric(latency.get("max")),
        }
    raw_counters = doc.get("counters")
    counters = {}
    if isinstance(raw_counters, dict):
        for key, value in raw_counters.items():
            if isinstance(key, str):
                number = _numeric(value)
                if number is not None:
                    counters[key] = number
    metrics["counters"] = counters
    if metrics["requests_total"] is None:
        metrics["requests_total"] = counters.get("requests_total")
    for key, value in counters.items():
        match = _STATUS_COUNTER.match(key)
        if match is not None:
            code, model = match.group(1), match.group(2)
            metrics["requests_by_model"][model] = \
                metrics["requests_by_model"].get(model, 0) + value
            metrics["status_by_code"][code] = \
                metrics["status_by_code"].get(code, 0) + value
            continue
        errors = _ERRORS_COUNTER.match(key)
        tokens_in = _TOKENS_IN_COUNTER.match(key)
        tokens_out = _TOKENS_OUT_COUNTER.match(key)
        if errors is not None:
            metrics["errors_by_model"][errors.group(1)] = value
        elif tokens_in is not None:
            metrics["tokens_in_by_model"][tokens_in.group(1)] = value
        elif tokens_out is not None:
            metrics["tokens_out_by_model"][tokens_out.group(1)] = value
    if metrics["errors_by_model"]:
        metrics["errors_total"] = sum(metrics["errors_by_model"].values())
    out["metrics"] = metrics
    return out


# --- vLLM component -------------------------------------------------------

def _vllm_component(mode, reader, now, stale_threshold_seconds):
    """One read-only vLLM observation, judged against the authoritative mode.

    Never raises: a reader failure yields an unavailable component with an
    error, leaving the rest of the result intact.
    """
    comp = {
        "status": "unknown",            # inactive|starting|ok|unreachable|error
        "expected_mode": mode,
        "expected_model_id": HEAVY_EXPECTED_MODELS.get(mode),
        "served_model": None,
        "model_identity": None,         # verified|mismatch|unverifiable|None
        "health": None,
        "metrics": None,                # cumulative vLLM counters/gauges
        "available": False,
        "stale": True,
        "sample_ts": None,
        "error": None,
        "details": None,
    }
    try:
        sample = reader.collect()
    except Exception as exc:  # a reader bug or surprise must not break output
        comp["error"] = "%s: %s" % (type(exc).__name__, _sanitize_text(str(exc)))
        return comp
    if not isinstance(sample, dict):
        comp["error"] = "vLLM reader returned a non-object sample"
        return comp
    comp["available"] = True
    status = sample.get("status")
    comp["status"] = status if isinstance(status, str) else "unknown"
    health = sample.get("health")
    comp["health"] = health if isinstance(health, bool) else None
    served = sample.get("served_model")
    comp["served_model"] = served if isinstance(served, str) and served else None
    metrics = sample.get("metrics")
    if isinstance(metrics, dict):
        comp["metrics"] = {key: _numeric(value) for key, value in metrics.items()}
    details = sample.get("details")
    if isinstance(details, dict):
        comp["details"] = {key: details.get(key)
                           for key in ("http", "warnings", "model_mismatch")
                           if key in details}
    error = sample.get("error")
    comp["error"] = _sanitize_text(error) if isinstance(error, str) and error else None
    stamp = _numeric(sample.get("timestamp"))
    comp["sample_ts"] = stamp
    comp["stale"] = stamp is None or (now - stamp) > stale_threshold_seconds
    # Physically served model identity is only checkable when the heavy API
    # answered fully (status ok) in a heavy mode.
    if mode in HEAVY_EXPECTED_MODELS:
        comp["model_identity"] = (
            "verified" if comp["served_model"] == HEAVY_EXPECTED_MODELS[mode]
            else "mismatch") if comp["status"] == "ok" else "unverifiable"
    return comp


# --- public entry point ----------------------------------------------------

def collect_service_metrics(model_state=None, vllm_reader=None,
                            vllm_base_url=DEFAULT_VLLM_BASE_URL,
                            gateway_metrics_path=None,
                            controller=None, clock=None,
                            vllm_timeout_seconds=DEFAULT_VLLM_TIMEOUT_SECONDS,
                            stale_threshold_seconds=DEFAULT_STALE_THRESHOLD_SECONDS):
    """One read-only service-level sample: heavy vLLM + inference gateway.

    ``model_state`` should be the cluster snapshot (or raw cluster-model
    status) the aggregator already read, so the authoritative mode is never
    re-read; when it is omitted the fixed read-only
    ``cluster-model status --json`` command is run once.  ``vllm_reader``
    may be injected (tests); otherwise a fresh :class:`VllmReader` on the
    fixed DGX02 endpoint is used.  Both components fail independently.
    The result is JSON-serializable; missing values are ``null``.
    """
    if clock is None:
        clock = time.time
    now = clock()
    warnings = []

    # 1) authoritative cluster mode (reused, never guessed)
    if model_state is None:
        try:
            model_state = (controller if controller is not None
                           else ClusterModelController()).status()
            mode, source = _mode_from_state(model_state)
            source = "controller-read"
        except Exception as exc:
            if isinstance(exc, ControllerError):
                detail = str(exc)
            else:
                detail = "%s: %s" % (type(exc).__name__, _sanitize_text(str(exc)))
            model_state, mode, source = None, "unknown", "controller-unavailable"
            warnings.append("cluster-model status could not be read (%s); "
                            "vLLM expected mode unverifiable" % _sanitize_text(detail))
    else:
        mode, source = _mode_from_state(model_state)
    if mode == "unknown":
        warnings.append("cluster mode unknown (source=%s); vLLM probed without "
                        "a mode expectation" % source)

    # 2) vLLM component (independent failure)
    if vllm_reader is None:
        reader_mode = "transition" if mode == "transitioning" else mode
        vllm_reader = VllmReader(
            base_url=vllm_base_url, timeout=vllm_timeout_seconds,
            expected_mode=reader_mode,
            expected_model_id=HEAVY_EXPECTED_MODELS.get(mode))
    vllm = _vllm_component(mode, vllm_reader, now, stale_threshold_seconds)
    expected_model = HEAVY_EXPECTED_MODELS.get(mode)
    if vllm["available"]:
        if expected_model is not None:
            if vllm["model_identity"] == "mismatch":
                warnings.append(
                    "vLLM model identity mismatch: mode %s expects served model "
                    "%r but the API reports %r"
                    % (mode, expected_model, vllm["served_model"]))
            elif vllm["status"] in ("unreachable", "error"):
                warnings.append(
                    "vLLM expected for mode %s but status is %s (%s)"
                    % (mode, vllm["status"], vllm["error"] or "no detail"))
        elif vllm["status"] != "inactive":
            warnings.append("vLLM status %s in non-heavy mode %s (inactive expected; %s)"
                            % (vllm["status"], mode,
                               vllm["error"] or "see details"))
        reader_details = vllm["details"] or {}
        for item in reader_details.get("warnings") or []:
            warnings.append("vLLM: %s" % _sanitize_text(str(item)))
        if vllm["stale"]:
            warnings.append("vLLM sample is stale (older than %.0fs)"
                            % stale_threshold_seconds)
    else:
        warnings.append("vLLM component unavailable: %s"
                        % (vllm["error"] or "no observation"))

    # 3) gateway component (independent failure)
    if gateway_metrics_path is None:
        gateway_metrics_path = DEFAULT_GATEWAY_METRICS_PATH
    gw = read_gateway_metrics(gateway_metrics_path, now=now,
                              stale_threshold_seconds=stale_threshold_seconds)
    for item in gw["warnings"]:
        warnings.append("gateway: %s" % _sanitize_text(item))
    if not gw["available"]:
        warnings.append("gateway metrics unavailable: %s" % gw["error"])
    gateway = {
        "available": gw["available"],
        "stale": gw["stale"],
        "ts": gw["ts"],
        "age_seconds": gw["age_seconds"],
        "source_path": gateway_metrics_path,
        "metrics": gw["metrics"],
        "error": gw["error"],
    }

    result = {
        "schema_version": SERVICE_SCHEMA_VERSION,
        "timestamp": now,
        "cluster_mode": mode,
        "model_state_source": source,
        "vllm": vllm,
        "gateway": gateway,
        "metrics": {"vllm": vllm["metrics"], "gateway": gateway["metrics"]},
        "availability": {"vllm": vllm["available"], "gateway": gw["available"]},
        "staleness": {"vllm": vllm["stale"], "gateway": gw["stale"]},
        "warnings": warnings,
    }
    return _jsonify(result)
