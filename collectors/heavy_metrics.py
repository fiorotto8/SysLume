"""Read-only vLLM heavy-mode telemetry for the SysLume-Cluster node collector.

The heavy vLLM API serves on DGX02 only (DGX03 is a Ray worker/rank 1 with
no API of its own, so an absent port there is normal and must not be
classified as offline). Both heavy-qwen and heavy-deepseek use the same
endpoint, never simultaneously; the authoritative active mode comes from
cluster-model, which this reader only reflects - it never changes it.

Read-only endpoints used:

    GET /health
    GET /v1/models
    GET /metrics

No inference or chat requests are ever issued.

States:
    inactive     - cluster mode does not run heavy vLLM; API absent is expected
    starting     - cluster is transitioning; an absent/unhealthy API is expected
    ok           - API healthy; individual missing metrics stay null
    unreachable  - connection failure while heavy mode is expected
    error        - unexpected HTTP/parse failure

Prometheus values are parsed with label awareness; duplicate or
multi-model series are not summed and degrade to null. KV-cache usage is
a fraction in 0-1 (the dashboard may render it as a percentage later).
vLLM counters are cumulative here; the central aggregator may derive
rates. No value is ever added onto the host memory total.
"""
from __future__ import annotations

import json
import math
import re
import time
import urllib.error
import urllib.request
from urllib.parse import urlsplit

DEFAULT_BASE_URL = "http://192.168.0.2:8000"
DEFAULT_TIMEOUT_SECONDS = 4.0
DEFAULT_MAX_RESPONSE_BYTES = 1024 * 1024
HEAVY_MODES = ("heavy-qwen", "heavy-deepseek")
MODES_PROBED = HEAVY_MODES + ("transition",)
ENDPOINTS = ("/health", "/v1/models", "/metrics")

# Primary metric names on our deployed vLLM runtimes, with documented
# legacy counter fallbacks only.
TOKEN_METRICS = {
    "prompt": ("vllm:prompt_tokens_total", "vllm:num_prompt_tokens"),
    "generation": ("vllm:generation_tokens_total", "vllm:num_generation_tokens"),
}
GAUGE_METRICS = {
    "running": ("vllm:num_requests_running",),
    "waiting": ("vllm:num_requests_waiting",),
    "kv_cache": ("vllm:kv_cache_usage_perc",),
}


def validate_base_url(base_url):
    """Accept a plain vLLM base URL and reject injection-shaped values."""
    parsed = urlsplit(base_url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError("base_url must be plaintext http:// or https://")
    if not parsed.hostname:
        raise ValueError("base_url must include a host")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("base_url must not embed credentials")
    if parsed.query or parsed.fragment:
        raise ValueError("base_url must not contain a query or fragment")
    if parsed.path not in ("", "/"):
        raise ValueError("base_url must not contain a path")
    return parsed.geturl().rstrip("/")


class _FetchError(Exception):
    """Categorized fetch failure; kind is connection|http|malformed|oversize."""

    def __init__(self, kind, message):
        super().__init__(message)
        self.kind = kind
        self.message = message


def _read_bounded(response, max_bytes):
    chunks = []
    received = 0
    while True:
        chunk = response.read(65536)
        if not chunk:
            return b"".join(chunks)
        received += len(chunk)
        if received > max_bytes:
            raise _FetchError("oversize", "response exceeds %d bytes" % max_bytes)
        chunks.append(chunk)


def _get_text(base_url, path, timeout, max_bytes):
    """One bounded read-only GET.

    Returns ``text`` (str, possibly empty) or raises _FetchError:
    connection  - refused/timeout/DNS (service absent)
    http        - any non-2xx status; the code is in the message
    oversize    - body larger than max_bytes
    """
    url = base_url + path
    request = urllib.request.Request(url, method="GET",
                                     headers={"Accept": "text/plain, application/json"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = _read_bounded(response, max_bytes)
    except urllib.error.HTTPError as exc:
        error = _FetchError("http", "HTTP %s for %s" % (exc.code, url))
        error.code = exc.code
        raise error from None
    except (OSError, TimeoutError) as exc:
        raise _FetchError("connection", "connection to %s failed: %s" % (url, exc)) from None
    return body.decode("utf-8", "replace")


_PROM_LINE = re.compile(
    r"^([a-zA-Z_:][a-zA-Z0-9_:]*)"
    r"(?:\s*\{([^}]*)})?"          # optional label block
    r"\s+([-+]?[0-9]*\.?[0-9]+(?:[eE][-+]?[0-9]+)?|[-+]?[a-zA-Z]+[a-zA-Z0-9]*)"
    r"(?:\s+[^\s}]+)?\s*$")        # optional timestamp
_LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*"((?:[^"\\]|\\.)*)"')


def _prom_value(raw):
    """Finite float or None; +Inf/-Inf/NaN are unavailable, not numbers."""
    try:
        value = float(raw)
    except ValueError:
        return None
    return value if math.isfinite(value) else None


def parse_prometheus(text):
    """Parse a Prometheus exposition into ``{name: [(labels, value), ...]}``.

    Ignores comments/HELP/TYPE lines, histogram buckets/counts/sums and
    malformed lines; keeps only plain gauge/counter sample lines with
    finite values.
    """
    series = {}
    for line in (text or "").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        match = _PROM_LINE.match(line)
        if not match:
            continue
        name, label_raw, value_raw = match.group(1), match.group(2), match.group(3)
        if name.endswith(("_bucket", "_count", "_sum")):
            continue
        value = _prom_value(value_raw)
        if value is None:
            continue
        labels = dict(_LABEL.findall(label_raw)) if label_raw else {}
        series.setdefault(name, []).append((labels, value))
    return series


def pick_value(series, names, warnings):
    """Value for the first of ``names`` present; never sums across models.

    A single series (or identical duplicates) yields the value; series
    with differing model labels are refused and reported as null with a
    warning instead of mixing attribution.
    """
    for name in names:
        entries = series.get(name)
        if not entries:
            continue
        model_labels = sorted({(labels.get("model_name") or labels.get("model") or "")
                                for labels, _ in entries})
        if len(entries) > 1 and len(model_labels) > 1:
            warnings.append("multiple model labels on %s; not summed" % name)
            return None
        value = entries[0][1]
        if len(entries) > 1:
            warnings.append("duplicate series for %s; first value kept" % name)
        return value
    return None


def _empty_metrics():
    """Chartable vLLM fields; None means unavailable, never zero-filled."""
    return {
        "vllm_up": 0,
        "vllm_running_reqs": None,
        "vllm_waiting_reqs": None,
        "vllm_kv_cache_usage": None,          # fraction 0-1
        "vllm_prompt_tokens_total": None,     # cumulative; rates done centrally
        "vllm_generation_tokens_total": None,  # cumulative; rates done centrally
    }


class VllmReader:
    """Collect vLLM heavy-mode telemetry from one fixed endpoint.

    ``expected_mode`` mirrors cluster-model: "split" (or any non-heavy
    mode) expects a stopped API, "transition" reports "starting",
    and "heavy-qwen"/"heavy-deepseek" expect a healthy one. An optional
    ``expected_model_id`` enables physical served-model verification.
    This reader never sends requests other than the fixed read-only set.
    """

    def __init__(self, base_url=DEFAULT_BASE_URL, timeout=DEFAULT_TIMEOUT_SECONDS,
                 expected_mode="split", expected_model_id=None,
                 max_response_bytes=DEFAULT_MAX_RESPONSE_BYTES):
        self.base_url = validate_base_url(base_url)
        self.timeout = self._positive(timeout, "timeout")
        self.max_response_bytes = self._positive(max_response_bytes, "max_response_bytes")
        if not isinstance(expected_mode, str) or not expected_mode:
            raise ValueError("expected_mode must be a non-empty cluster mode string")
        self.expected_mode = expected_mode
        self.expected_model_id = expected_model_id

    @staticmethod
    def _positive(value, name):
        if (isinstance(value, bool) or not isinstance(value, (int, float))
                or not math.isfinite(value) or value <= 0):
            raise ValueError(name + " must be a positive number")
        return float(value)

    def _fetch(self, path):
        """GET one path; returns str body or a _FetchError instance."""
        try:
            return _get_text(self.base_url, path, self.timeout, self.max_response_bytes)
        except _FetchError as exc:
            return exc

    def _sample(self, stamp, status, error, health, served_model, metrics, details):
        return {"timestamp": stamp, "status": status, "error": error, "health": health,
                "served_model": served_model, "metrics": metrics, "details": details}

    def collect(self):
        """One read-only sample; JSON-serializable; None means unknown."""
        stamp = time.time()
        details = {"expected_mode": self.expected_mode, "http": {}, "warnings": []}

        if self.expected_mode == "transition":
            details["warnings"].append("cluster transition in progress; API expected absent or warming up")
            return self._sample(stamp, "starting", None, None, None, _empty_metrics(), details)

        if self.expected_mode not in HEAVY_MODES:
            # Split/stop modes expect a stopped API; verify with one probe.
            result = self._fetch("/health")
            if isinstance(result, _FetchError):
                if result.kind == "connection":
                    details["http"]["health"] = "absent"
                    return self._sample(stamp, "inactive", None, False, None, _empty_metrics(), details)
                details["http"]["health"] = "http:%s" % getattr(result, "code", "error")
                return self._sample(stamp, "error", result.message, False, None,
                                    _empty_metrics(), details)
            details["http"]["health"] = 200
            details["warnings"].append(
                "vLLM API answered /health but expected mode is %s" % self.expected_mode)
            return self._sample(stamp, "error",
                                "vLLM API is up but cluster mode is %s (unexpected)" % self.expected_mode,
                                True, None, _empty_metrics(), details)

        result = self._fetch("/health")
        if isinstance(result, _FetchError):
            if result.kind == "connection":
                details["http"]["health"] = "unreachable"
                return self._sample(stamp, "unreachable", result.message, False, None,
                                    _empty_metrics(), details)
            code = getattr(result, "code", 0)
            details["http"]["health"] = code
            if code in (500, 502, 503):
                return self._sample(stamp, "starting",
                                    "vLLM engine not ready (HTTP %d)" % code,
                                    False, None, _empty_metrics(), details)
            return self._sample(stamp, "error", result.message, False, None,
                                _empty_metrics(), details)
        details["http"]["health"] = 200

        result = self._fetch("/v1/models")
        served_model = None
        if isinstance(result, _FetchError):
            details["http"]["models"] = "unreachable" if result.kind == "connection" else "http"
            return self._sample(stamp, "error", "vLLM /v1/models failed: " + result.message,
                                True, None, _empty_metrics(), details)
        details["http"]["models"] = 200
        try:
            payload = json.loads(result)
            items = payload.get("data") if isinstance(payload, dict) else None
            if isinstance(items, list) and items and isinstance(items[0], dict):
                candidate = items[0].get("id")
                served_model = candidate if isinstance(candidate, str) and candidate else None
        except ValueError:
            return self._sample(stamp, "error", "malformed JSON from vLLM /v1/models",
                                True, None, _empty_metrics(), details)
        if self.expected_model_id is not None and served_model != self.expected_model_id:
            details["model_mismatch"] = True
            details["warnings"].append(
                "served model %r does not match expected %r" % (served_model, self.expected_model_id))
        else:
            details["model_mismatch"] = False

        metrics = _empty_metrics()
        metrics["vllm_up"] = 1
        result = self._fetch("/metrics")
        if isinstance(result, _FetchError):
            details["http"]["metrics"] = "unreachable" if result.kind == "connection" else "http"
            details["metrics_status"] = "unavailable"
            return self._sample(stamp, "error",
                                "vLLM /metrics failed (request/wait/KV/token values unknown): " + result.message,
                                True, served_model, metrics, details)
        details["http"]["metrics"] = 200
        series = parse_prometheus(result)
        details["prometheus_metric_names"] = sorted(series)
        warnings = details["warnings"]
        metrics["vllm_running_reqs"] = pick_value(series, GAUGE_METRICS["running"], warnings)
        metrics["vllm_waiting_reqs"] = pick_value(series, GAUGE_METRICS["waiting"], warnings)
        metrics["vllm_kv_cache_usage"] = pick_value(series, GAUGE_METRICS["kv_cache"], warnings)
        if metrics["vllm_kv_cache_usage"] is not None and metrics["vllm_kv_cache_usage"] > 1.0:
            warnings.append("kv_cache usage reported above 1.0; raw value preserved")
        metrics["vllm_prompt_tokens_total"] = pick_value(series, TOKEN_METRICS["prompt"], warnings)
        metrics["vllm_generation_tokens_total"] = pick_value(series, TOKEN_METRICS["generation"], warnings)
        details["metrics_status"] = "ok"
        return self._sample(stamp, "ok", None, True, served_model, metrics, details)


__all__ = ["VllmReader", "DEFAULT_BASE_URL", "DEFAULT_TIMEOUT_SECONDS",
           "HEAVY_MODES", "parse_prometheus", "validate_base_url"]
