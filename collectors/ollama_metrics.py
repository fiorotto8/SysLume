"""Read-only Ollama metrics reader for the SysLume-Cluster node collector.

Uses only the Ollama HTTP API (0.34.x) read-only endpoints:

    GET /api/version
    GET /api/tags
    GET /api/ps

No inference requests are issued and models are never loaded, unloaded
or removed. A missing Ollama service is a normal offline state: the
collector reports status "unreachable" with null lists (never an empty
installed list, which would falsely mean "no models installed").

Semantics: installed != resident. A model present in /api/tags but not
in /api/ps is ``installed: true, resident: false`` - not missing.

DGX Spark / GB10 note: ``size_vram`` is Ollama's model allocation
estimate against the node's unified memory pool, not dedicated VRAM.
These values must never be added onto the host memory total; the
collector only sums them into ``ollama_resident_bytes`` as an
attribution breakdown. All sizes are bytes; context length is tokens.
Processor allocation is reported only when Ollama provides it.
"""
from __future__ import annotations

import json
import math
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from urllib.parse import urlsplit

DEFAULT_BASE_URL = "http://127.0.0.1:11434"
DEFAULT_TIMEOUT_SECONDS = 4.0
MAX_RESPONSE_BYTES = 16 * 1024 * 1024
ENDPOINTS = ("/api/version", "/api/tags", "/api/ps")


def validate_base_url(base_url):
    """Accept a plain Ollama base URL and reject injection-shaped values."""
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


def _read_bounded(response):
    chunks = []
    received = 0
    while True:
        chunk = response.read(65536)
        if not chunk:
            return b"".join(chunks)
        received += len(chunk)
        if received > MAX_RESPONSE_BYTES:
            raise _FetchError("oversize", "response exceeds %d bytes" % MAX_RESPONSE_BYTES)
        chunks.append(chunk)


def _get_json(url, timeout):
    """One bounded read-only GET; returns parsed JSON or raises _FetchError."""
    request = urllib.request.Request(url, method="GET",
                                     headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            data = json.loads(_read_bounded(response).decode("utf-8", "replace"))
    except urllib.error.HTTPError as exc:
        raise _FetchError("http", "HTTP %s for %s" % (exc.code, url)) from None
    except (OSError, TimeoutError) as exc:
        raise _FetchError("connection", "connection to %s failed: %s" % (url, exc)) from None
    except (ValueError, UnicodeError) as exc:
        raise _FetchError("malformed", "malformed JSON from %s: %s" % (url, exc)) from None
    if not isinstance(data, dict):
        raise _FetchError("malformed", "unexpected JSON shape from %s" % url)
    return data


def _as_int(value):
    """Non-negative integer or None; never invents a default."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, float) and value.is_integer() and value >= 0:
        return int(value)
    return None


def _as_str(value):
    return value if isinstance(value, str) and value else None


def _model_name(entry):
    for key in ("name", "model"):
        value = _as_str(entry.get(key)) if isinstance(entry, dict) else None
        if value:
            return value
    return None


def _classify_processor(processor):
    """Classify Ollama's /api/ps `processor` field when present and clear.

    Newer builds emit e.g. {"major": "100% GPU", "minor": ""}; 0.34.x
    omits it entirely. Returns "gpu", "cpu", "mixed" or None (unknown).
    """
    if not isinstance(processor, dict):
        return None
    parts = []
    for key in ("major", "minor"):
        value = _as_str(processor.get(key))
        if value:
            parts.append(value.lower())
    if not parts:
        return None
    gpu = any(token in part for part in parts for token in ("gpu", "cuda"))
    cpu = any("cpu" in part for part in parts)
    if gpu and cpu:
        return "mixed"
    if gpu:
        return "gpu"
    if cpu:
        return "cpu"
    return None


def _parse_expires(value):
    """Parse Ollama's expires_at (ISO 8601) to epoch seconds, or None."""
    text = _as_str(value)
    if not text:
        return None
    try:
        stamp = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return stamp.timestamp()


def _new_entry(installed):
    """Uniform per-model entry; every field present, null when unknown."""
    return {
        "installed": installed, "resident": False,
        "digest": None, "disk_size_bytes": None, "quantization_level": None,
        "capabilities": None, "size_bytes": None, "size_vram_bytes": None,
        "context_length_tokens": None, "processor": None,
        "expires_at": None, "expires_at_epoch": None,
    }


class OllamaReader:
    """Collect installed/resident model state from one Ollama endpoint.

    The endpoint is fixed per node (e.g. DGX02 is http://192.168.0.2:11434,
    not a localhost guess); only GET requests are issued and responses are
    size-bounded. A stopped Ollama service is a normal offline state and
    is reported, not treated as an empty model inventory.
    """

    def __init__(self, base_url=DEFAULT_BASE_URL, timeout=DEFAULT_TIMEOUT_SECONDS):
        self.base_url = validate_base_url(base_url)
        if (isinstance(timeout, bool) or not isinstance(timeout, (int, float))
                or not math.isfinite(timeout) or timeout <= 0):
            raise ValueError("timeout must be a positive number of seconds")
        self.timeout = float(timeout)

    @staticmethod
    def _installed_view(entry):
        view = _new_entry(True)
        view["digest"] = _as_str(entry.get("digest"))
        view["disk_size_bytes"] = _as_int(entry.get("size"))
        details = entry.get("details") if isinstance(entry.get("details"), dict) else {}
        view["quantization_level"] = _as_str(details.get("quantization_level"))
        capabilities = entry.get("capabilities")
        view["capabilities"] = capabilities if isinstance(capabilities, list) and capabilities else None
        return view

    @staticmethod
    def _resident_updates(entry):
        updates = {"resident": True}
        digest = _as_str(entry.get("digest"))
        if digest:
            updates["digest"] = digest
        for key, field in (("size", "size_bytes"), ("size_vram", "size_vram_bytes"),
                           ("context_length", "context_length_tokens")):
            value = _as_int(entry.get(key))
            if value is not None:
                updates[field] = value
        expires = _as_str(entry.get("expires_at"))
        if expires:
            updates["expires_at"] = expires
            updates["expires_at_epoch"] = _parse_expires(expires)
        processor = _classify_processor(entry.get("processor"))
        if processor is not None:
            updates["processor"] = processor
        return updates


def _overall_status(failures):
    failed = {path for path, _ in failures}
    if "/api/tags" not in failed and "/api/ps" not in failed:
        return "ok"
    core = [exc for path, exc in failures if path in ("/api/tags", "/api/ps")]
    if core and all(exc.kind == "connection" for exc in core):
        return "unreachable"
    return "error"


def _sample_metrics(installed_views, resident_views):
    """Chartable cluster metrics; a None input list means that endpoint failed.

    ``resident_views`` carries Ollama's own allocation numbers (size_vram)
    and is summed here only as an attribution breakdown; nothing in this
    module adds them onto a host memory total.
    """
    metrics = {
        "ollama_up": 1 if installed_views is not None or resident_views is not None else 0,
        "ollama_models_installed": len(installed_views) if installed_views is not None else None,
        "ollama_models_resident": len(resident_views) if resident_views is not None else None,
        "ollama_resident_bytes": None,
        "ollama_context_tokens_max": None,
    }
    if resident_views:
        allocations = [entry["size_vram_bytes"] for entry in resident_views
                       if entry["size_vram_bytes"] is not None]
        if allocations:
            metrics["ollama_resident_bytes"] = sum(allocations)
        contexts = [entry["context_length_tokens"] for entry in resident_views
                    if entry["context_length_tokens"] is not None]
        if contexts:
            metrics["ollama_context_tokens_max"] = max(contexts)
    elif resident_views is not None:
        metrics["ollama_resident_bytes"] = 0
    return metrics


def _collect(self):
    """One read-only sample; JSON-serializable; None means unknown."""
    stamp = time.time()
    fetched, failures = {}, []
    for path in ENDPOINTS:
        try:
            fetched[path] = _get_json(self.base_url + path, self.timeout)
        except _FetchError as exc:
            failures.append((path, exc))

    tags, ps = fetched.get("/api/tags"), fetched.get("/api/ps")
    version_entry = fetched.get("/api/version")
    models, order = {}, []
    for entry in (tags.get("models") if isinstance(tags, dict) else []) or []:
        if not isinstance(entry, dict):
            continue
        name = _model_name(entry)
        if name:
            models[name] = self._installed_view(entry)
            order.append(name)
    for entry in (ps.get("models") if isinstance(ps, dict) else []) or []:
        if not isinstance(entry, dict):
            continue
        name = _model_name(entry)
        if not name:
            continue
        if name not in models:
            models[name] = _new_entry(False)
            order.append(name)
        models[name].update(self._resident_updates(entry))

    names = sorted(models)
    installed_names = [n for n in names if models[n]["installed"]] if tags is not None else None
    resident_names = [n for n in names if models[n]["resident"]] if ps is not None else None
    installed_views = ([models[n] for n in installed_names]
                       if installed_names is not None else None)
    resident_views = ([models[n] for n in resident_names]
                      if resident_names is not None else None)
    status = _overall_status(failures)
    return {
        "timestamp": stamp,
        "status": status,
        "error": (" | ".join("[%s] %s" % (path, exc.message) for path, exc in failures)
                  if failures else None),
        "version": _as_str(version_entry.get("version")) if version_entry else None,
        "installed_models": installed_names,
        "resident_models": resident_names,
        "models": {name: models[name] for name in names},
        "metrics": _sample_metrics(installed_views, resident_views),
    }


OllamaReader.collect = _collect


__all__ = ["OllamaReader", "DEFAULT_BASE_URL", "DEFAULT_TIMEOUT_SECONDS", "validate_base_url"]
