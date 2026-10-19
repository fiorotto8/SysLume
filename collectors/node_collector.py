"""Node-level telemetry aggregator for the SysLume cluster.

Combines the readers that are meaningful *locally* on each DGX:

* ``SystemMetrics``   - CPU / RAM / GPU / temperatures / disk / network,
  including the GB10 unified-memory gauge (never a sum of GPU + host).
* ``OllamaReader``    - installed vs resident models from the node's
  *fixed, validated* Ollama endpoint (DGX02/DGX03 are their own
  192.168.x.2 addresses; DGX01 is localhost).
* ``ProcessMetrics``  - local inference process / Docker / tmux counts.

``VllmReader`` (collectors.heavy_metrics) is intentionally NOT part of
this snapshot: the heavy endpoint only exists on DGX02 in heavy mode
and the central aggregator judges it with the authoritative
``cluster-model`` state.

Node identity and endpoint come from the validated ``NODE_CONFIG`` map
(or a loader-provided config file); they are never taken from client
input. Collection is local only - no SSH to other nodes.

Node ``status`` describes *telemetry collection quality only*:
  ok      - every reader completed; any offline endpoints are reported
            inside their component with an informational warning
  partial - one or more readers failed or reported an error state
  error   - all readers failed
It never represents authoritative cluster health; that remains
``cluster-model status --json``.
"""

from __future__ import annotations

import json
import math
import time

from collectors import ollama_metrics as OL
from collectors import process_metrics as PM
from collectors import system_metrics as SM

__all__ = ["NodeCollector", "NODE_CONFIG", "SCHEMA_VERSION", "load_node_config"]

SCHEMA_VERSION = 1

#: Validated per-node identity -> fixed Ollama endpoint. These are the
#: endpoints verified against the live cluster; they are configuration,
#: not input.
NODE_CONFIG = {
    "dgx01": {"ollama_endpoint": "http://127.0.0.1:11434"},
    "dgx02": {"ollama_endpoint": "http://192.168.0.2:11434"},
    "dgx03": {"ollama_endpoint": "http://192.168.2.2:11434"},
}

COMPONENTS = ("system", "ollama", "processes")

#: Fallback key sets used to keep the flattened mapping stable (all-null)
#: when a component fails and contributes no sample of its own.
_FALLBACK_METRIC_KEYS = {
    "system": tuple(SM.METRIC_KEYS),
    "ollama": ("ollama_up", "ollama_models_installed",
               "ollama_models_resident", "ollama_resident_bytes",
               "ollama_context_tokens_max"),
    "processes": ("ollama_daemon_count", "ollama_runner_count",
                  "ray_process_count", "vllm_process_count",
                  "vllm_engine_core_count", "vllm_rank_worker_count",
                  "docker_container_count", "tmux_session_count"),
}


def load_node_config(path):
    """Load {node_id, ollama_endpoint?} from validated local configuration.

    Only the two keys are honored; the node_id must be a known node and
    the endpoint must pass the Ollama reader's URL validation.
    """
    try:
        with open(path, "r") as fh:
            raw = json.load(fh)
    except (OSError, ValueError) as exc:
        raise ValueError("cannot read node config %s: %s" % (path, exc))
    if not isinstance(raw, dict):
        raise ValueError("node config must be a JSON object")
    node_id = raw.get("node_id")
    if node_id not in NODE_CONFIG:
        raise ValueError("config node_id %r is not a known node" % (node_id,))
    endpoint = raw.get("ollama_endpoint",
                       NODE_CONFIG[node_id]["ollama_endpoint"])
    OL.validate_base_url(endpoint)
    return {"node_id": node_id, "ollama_endpoint": endpoint}


class NodeCollector:
    """Compose local readers into one versioned, JSON-safe node snapshot.

    Readers are dependency-injectable for tests:
      NodeCollector("dgx01", system=<obj>, ollama=<obj>, processes=<obj>)
    Any reader left as None is constructed from the validated node
    configuration. Each injected reader only needs a ``collect()``
    method returning a dict with a ``metrics`` mapping.
    """

    def __init__(self, node_id=None, system=None, ollama=None,
                 processes=None, config_path=None, base_url=None,
                 ollama_timeout=OL.DEFAULT_TIMEOUT_SECONDS,
                 process_kwargs=None):
        if node_id is None:
            if not config_path:
                raise ValueError("node_id or config_path is required")
            cfg = load_node_config(config_path)
            node_id = cfg["node_id"]
            if base_url is None:
                base_url = cfg["ollama_endpoint"]
        if node_id not in NODE_CONFIG:
            raise ValueError("unknown node_id: %r" % (node_id,))
        self.node_id = node_id
        if base_url is None:
            base_url = NODE_CONFIG[node_id]["ollama_endpoint"]
        self.base_url = OL.validate_base_url(base_url)
        self._system = system if system is not None else SM.SystemMetrics()
        self._ollama = (ollama if ollama is not None
                        else OL.OllamaReader(self.base_url,
                                             timeout=ollama_timeout))
        self._processes = (processes if processes is not None
                           else PM.ProcessMetrics(**(process_kwargs or {})))
        #: Component readers by snapshot key, in canonical order.
        self._readers = {"system": self._system, "ollama": self._ollama,
                         "processes": self._processes}

    # -- internal helpers -------------------------------------------------

    def _component_state(self, name, sample):
        """Return (failed, degraded, warning) for one completed reader.

        failed   - the reader's data cannot be trusted; its metrics are
                   nulled in the flattened map and it counts toward the
                   node status.
        degraded - the reader completed but a sub-source failed (e.g.
                   nvidia-smi); valid measurements are still merged and
                   only the node status/warnings are affected.
        """
        if not isinstance(sample, dict):
            return True, False, "component %s returned a non-dict sample" % name
        if name == "ollama":
            status = sample.get("status")
            if status == "error":
                detail = sample.get("error") or "endpoint returned errors"
                return (True, False, "ollama component error: %s"
                        % str(detail)[:240])
            if status == "unreachable":
                # A successfully-observed offline endpoint: not a failure.
                return (False, False, "ollama endpoint %s unreachable "
                        "(reported offline)" % self.base_url)
            if status is not None and status != "ok":
                return (True, False,
                        "ollama reported unexpected status %r" % (status,))
            return False, False, None
        if name == "system":
            details = sample.get("details") or {}
            gpu_status = details.get("gpu_status")
            if isinstance(gpu_status, dict) \
                    and gpu_status.get("state") == "error":
                msg = str(gpu_status.get("message") or "gpu telemetry error")
                return (False, True, "gpu telemetry error: %s" % msg[:240])
            return False, False, None
        if name == "processes":
            if sample.get("status") == "error":
                return (True, False, "process telemetry error: %s" % str(
                    sample.get("error") or "")[:240])
            return False, False, None
        return False, False, None

    def collect(self):
        """One bounded, read-only node snapshot (JSON-serializable)."""
        started = time.time()
        warnings = []
        components = {}
        merged = {}
        failed = []
        degraded = []
        for name in COMPONENTS:
            try:
                sample = self._readers[name].collect()
            except Exception as exc:  # per-reader isolation
                failed.append(name)
                warnings.append("component %s raised %s: %s"
                                % (name, type(exc).__name__, str(exc)[:200]))
                continue
            components[name] = sample
            failed_flag, degraded_flag, warn = self._component_state(
                name, sample)
            if warn:
                warnings.append(warn)
            if failed_flag:
                failed.append(name)
            elif degraded_flag:
                degraded.append(name)
            comp_warnings = sample.get("warnings") if isinstance(
                sample, dict) else None
            if isinstance(comp_warnings, list):
                for w in comp_warnings[:10]:
                    warnings.append("%s: %s" % (name, str(w)[:200]))
            if failed_flag:
                # A failed component's partial numbers are misleading at
                # the flattened level; raw values remain in components.
                continue
            metrics = sample.get("metrics") if isinstance(sample, dict) \
                else None
            if not isinstance(metrics, dict):
                if not failed_flag:
                    failed.append(name)
                    warnings.append("component %s returned no metrics map"
                                    % name)
                continue
            for key, value in metrics.items():
                if key in merged:
                    renamed = "%s_%s" % (name, key)
                    if renamed in merged:
                        warnings.append("metric collision unresolvable: %s"
                                        % key)
                        continue
                    warnings.append("metric collision: %s.%s renamed to %s"
                                    % (name, key, renamed))
                    merged[renamed] = value
                else:
                    merged[key] = value
        # Keep the mapping stable: failed components contribute all-null.
        for name in COMPONENTS:
            if name not in components or name in failed:
                for key in _FALLBACK_METRIC_KEYS[name]:
                    merged.setdefault(key, None)
        if not components:
            status = "error"
        elif failed or degraded:
            status = "partial"
        else:
            status = "ok"
        snapshot = {
            "schema_version": SCHEMA_VERSION,
            "node_id": self.node_id,
            "timestamp": started,
            "status": status,
            "components": components,
            "metrics": merged,
            "warnings": warnings,
        }
        return _sanitize(snapshot)


def _sanitize(value):
    """Recursively convert non-finite floats to None (JSON-safe output)."""
    if isinstance(value, dict):
        return {key: _sanitize(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_sanitize(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value
