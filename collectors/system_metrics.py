"""System metrics reader for the SysLume-Cluster node collector.

Reuses the upstream SysLume readers (``LinuxReader`` and ``NvidiaReader``
from ``monitor.py``) and adds the GB10 unified-memory fields. On GB10 the
nvidia-smi memory view and ``/proc/meminfo`` describe the *same* physical
pool, so exactly one of them is exposed as ``mem_unified_*`` and the two
are never added together. Unavailable values stay ``None`` (JSON null);
nothing is zero-filled. No third-party dependencies.
"""
from __future__ import annotations

import importlib.util
import math
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _load_monitor():
    """Import upstream monitor.py whether or not the repo root is on sys.path."""
    try:
        import monitor
        return monitor
    except ImportError:
        spec = importlib.util.spec_from_file_location("_syslume_monitor", ROOT / "monitor.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module


MON = _load_monitor()


def default_config(overrides=None):
    """Reader configuration: upstream defaults, documented keys only."""
    cfg = dict(MON.DEFAULTS)
    for key, value in (overrides or {}).items():
        if key not in cfg:
            raise ValueError("unknown config key: " + key)
        cfg[key] = value
    return cfg


#: Upstream metric set plus the cluster additions. SI base units except
#: percentages, MHz, degrees C, rpm and seconds. None (JSON null) means
#: unavailable; first-sample rate fields are null until the next sample.
METRIC_KEYS = list(MON.METRICS) + ["mem_unified_used_bytes", "mem_unified_total_bytes"]


def _ok_pair(used, total):
    """A usable gauge needs both values present, finite and non-negative."""
    for value in (used, total):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return False
        if not math.isfinite(value) or value < 0:
            return False
    return True


def unified_memory(linux, nvidia):
    """Derive the single GB10 unified memory gauge.

    On GB10 nvidia-smi and ``/proc/meminfo`` describe the same shared
    pool (nvidia-smi total is ~121.5 GiB of the ~128 GiB when the driver
    exposes it; otherwise nvidia-smi reports [N/A]). Prefer the
    nvidia-smi view when present, fall back to meminfo, and never return
    a sum. Returns ``(used_bytes, total_bytes, source)`` where source is
    ``"nvidia-smi"``, ``"meminfo"`` or ``None``.
    """
    used, total = nvidia.get("gpu_vram_used_bytes"), nvidia.get("gpu_vram_total_bytes")
    if _ok_pair(used, total):
        return used, total, "nvidia-smi"
    used, total = linux.get("ram_used_bytes"), linux.get("ram_total_bytes")
    if _ok_pair(used, total):
        return used, total, "meminfo"
    return None, None, None


class SystemMetrics:
    """Collect CPU, RAM, swap, GPU, temperatures, disk and network for one node.

    Thin aggregator over the upstream readers; each unavailable reading
    stays ``None`` (JSON null) in both ``metrics`` and ``details``.
    """

    def __init__(self, cfg=None):
        self.cfg = cfg or default_config()
        self.linux = MON.LinuxReader(self.cfg)
        self.gpu = MON.NvidiaReader(self.cfg)

    def sample(self):
        """Return ``(metrics, details)`` for one instant."""
        metrics = {key: None for key in METRIC_KEYS}
        linux_metrics, linux_details = self.linux.sample()
        for key, value in linux_metrics.items():
            if key in metrics:
                metrics[key] = value
        gpu_metrics, gpu_status = self.gpu.sample()
        for key, value in gpu_metrics.items():
            if key in metrics:
                metrics[key] = value
        used, total, source = unified_memory(linux_metrics, gpu_metrics)
        metrics["mem_unified_used_bytes"] = used
        metrics["mem_unified_total_bytes"] = total
        details = {
            "gpu_status": gpu_status,
            "mem_unified_source": source,
            "system": linux_details,
            "gpu": gpu_metrics,
        }
        return metrics, details

    def collect(self, timestamp=None):
        """Upstream-style sample dict: ``{"timestamp", "metrics", "details"}``."""
        metrics, details = self.sample()
        return {"timestamp": time.time() if timestamp is None else timestamp,
                "metrics": metrics, "details": details}


__all__ = ["METRIC_KEYS", "SystemMetrics", "default_config", "unified_memory"]
