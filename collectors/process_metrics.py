"""Process-level, node-local monitoring for SysLume cluster collectors.

Read-only observations of inference-related processes and services:
Ollama daemon + llama-server runners, Ray processes, vLLM processes,
Docker containers and tmux sessions on a single DGX node.

Semantics
---------
* Process presence is *observational evidence*, NOT authoritative
  cluster health. ``cluster-model status --json`` remains the
  authoritative source for cluster mode.
* Counts can never be higher than what was observable. When part of
  /proc is unreadable, per-class counts are reported as ``None``
  (unknown) instead of a possibly-false zero.
* A stopped Ollama in heavy mode, or absent Ray/vLLM in split mode,
  are normal states: this module reports numbers and lets the
  central aggregator (which knows the mode) judge them.
* Container names and PIDs are NOT stable identities across mode
  transitions; they are reported per snapshot only.
* No secrets, environment variables, or full command lines are
  captured. Only pids, short comm names, matched role tokens and
  uptimes are retained.

Standard library only. Subprocess invocations use fixed argument
arrays, ``shell=False``, short timeouts and bounded captured output:
  docker ps --no-legend --format '{{.Names}}\t{{.Image}}'
  tmux    list-sessions -F #{session_name}
No state-changing commands are ever issued.
"""

from __future__ import annotations

import errno
import json
import os
import re
import subprocess
import time

__all__ = ["ProcessMetrics", "DEFAULT_PROC_ROOT"]

DEFAULT_PROC_ROOT = "/proc"

# Bounded subprocess output (bytes/chars) to keep pathological output
# from bloating a sample.
MAX_OUTPUT_CHARS = 65536

_ALLOWED_COMMANDS = ("docker", "tmux")


def _run(args, timeout, _runner=subprocess.run):
    """Run a fixed argv with shell=False, bounded output, no check.

    Returns (returncode | None, stdout, stderr, kind) where kind is one
    of: None (completed), "timeout", "missing" (no binary), "error".
    """
    if not args or args[0] not in _ALLOWED_COMMANDS:
        raise ValueError("refusing to run: %r" % (args,))
    try:
        proc = _runner(args, shell=False, stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE, text=True, timeout=timeout,
                       check=False)
    except subprocess.TimeoutExpired:
        return None, "", "", "timeout"
    except FileNotFoundError:
        return None, "", "", "missing"
    except (OSError, subprocess.SubprocessError):
        return None, "", "", "error"
    return (proc.returncode,
            (proc.stdout or "")[:MAX_OUTPUT_CHARS],
            (proc.stderr or "")[:MAX_OUTPUT_CHARS],
            None)


# --------------------------------------------------------------------------
# /proc inspection
# --------------------------------------------------------------------------

def _read_file(path, binary_sep=None):
    """Read a small file; return None on any error, list if binary_sep."""
    try:
        with open(path, "rb") as fh:
            raw = fh.read(16384)
    except OSError:
        return None
    if binary_sep is None:
        return raw.decode("utf-8", "replace").strip()
    parts = [p.decode("utf-8", "replace") for p in raw.split(binary_sep)]
    return [p for p in parts if p != ""]


def _cgroup_kind(text):
    """Map a cgroup file to 'container', 'host' or 'unknown'."""
    if not text:
        return "unknown"
    lowered = text.lower()
    if "/docker/" in lowered or "libpod" in lowered or "kubepods" in lowered:
        return "container"
    for line in text.splitlines():
        parts = line.split("::", 1)
        path = parts[1] if len(parts) == 2 else parts[-1]
        first = path.strip("/").split("/")[0] if path.strip("/") else ""
        if path == "/" or (first and (first.endswith(".slice")
                                      or first in ("init.scope", "systemd"))):
            return "host"
    return "unknown"


def _classify(comm, tokens):
    """Classify a process into roles. Never stores raw arguments.

    roles: subset of {ollama_daemon, llama_runner, ray, vllm}
    flags: engine_core / worker tokens observed (sanitized names only)
    """
    comm_l = (comm or "").lower()
    lowered = [t.lower() for t in tokens]
    base = os.path.basename(tokens[0]) if tokens else ""
    roles = set()
    if base == "llama-server" or comm_l == "llama-server":
        roles.add("llama_runner")
    elif base == "ollama" or comm_l == "ollama":
        roles.add("ollama_daemon")
    if (base in ("raylet", "gcs_server") or comm_l.startswith("ray::")
            or any("ray::" in t for t in lowered)
            or any(t == "ray" or "/ray/" in t for t in lowered)):
        roles.add("ray")
    # vLLM: comm title (e.g. "VLLM::EngineCore"), a vllm binary path,
    # or a python module token shaped "vllm.<...>" / "/vllm/" path.
    # Plain argument text containing "vllm" (e.g. earlyoom --prefer
    # regexes) must NOT match.
    vllm_hit = ("vllm" in comm_l) or ("vllm" in base)
    if not vllm_hit:
        for t in lowered:
            if t == "vllm" or t.startswith("vllm.") or "/vllm/" in t:
                vllm_hit = True
                break
    if vllm_hit or comm_l.startswith("enginecore"):
        roles.add("vllm")
    flags = {"engine_core": "enginecore" in comm_l or
             any("enginecore" in t for t in lowered),
             "worker": comm_l in ("worker", "vllm::worker") or
             any(t in ("worker", "vllm::worker") for t in lowered[:2])}
    return roles, flags


def _parse_uptime(proc_root, pid, clock_hz):
    """Uptime seconds for a pid, or None when unavailable."""
    stat = _read_file(os.path.join(proc_root, str(pid), "stat"))
    if stat is None or ")" not in stat:
        return None
    fields = stat.rsplit(")", 1)[1].split()
    if len(fields) < 20:  # starttime is field 22 -> index 19 after ')'
        return None
    try:
        starttime = float(fields[19])
        boot = None
        for line in (_read_file(os.path.join(proc_root, "stat")) or "").splitlines():
            if line.startswith("btime"):
                boot = float(line.split()[1])
                break
        if boot is None:
            return None
        up = time.time() - boot - starttime / clock_hz
        return max(0.0, up)
    except (ValueError, IndexError):
        return None


def _read_comm(pdir):
    """Read comm; return (value | None, vanished: bool)."""
    try:
        with open(os.path.join(pdir, "comm"), "rb") as fh:
            return fh.read(256).decode("utf-8", "replace").strip(), False
    except OSError as exc:
        if exc.errno == errno.ESRCH:
            return None, True
        return None, False


def _scan_proc(proc_root, clock_hz):
    """Scan /proc for inference roles. Returns dict or None if /proc
    itself is unreadable (distinct from per-pid unreadability)."""
    try:
        entries = os.listdir(proc_root)
    except OSError:
        return None
    classes = {"ollama_daemon": [], "llama_runner": [], "ray": [], "vllm": []}
    runners = []          # [{"pid", "uptime_seconds"}]
    split = {"host": 0, "container": 0, "unknown": 0}
    vllm_split = {"host": 0, "container": 0, "unknown": 0}
    engine_core = 0
    rank_workers = 0
    scanned = 0
    unreadable = 0
    vanished = 0
    for name in sorted(entries, key=lambda s: (len(s), s)):
        if not name.isdigit():
            continue
        pid = int(name)
        pdir = os.path.join(proc_root, name)
        comm, gone = _read_comm(pdir)
        if gone:
            vanished += 1
            continue
        if comm is None:
            unreadable += 1
            split["unknown"] += 1
            continue
        scanned += 1
        tokens = _read_file(os.path.join(pdir, "cmdline"), binary_sep=b"\0")
        cgroup = _read_file(os.path.join(pdir, "cgroup"))
        kind = _cgroup_kind(cgroup)
        split[kind] += 1
        roles, flags = _classify(comm, tokens or [])
        for role in roles:
            classes[role].append(pid)
        if "llama_runner" in roles:
            runners.append({"pid": pid,
                            "uptime_seconds": _parse_uptime(proc_root, pid, clock_hz)})
        if "vllm" in roles:
            vllm_split[kind] += 1
            if flags["engine_core"]:
                engine_core += 1
            if flags["worker"]:
                rank_workers += 1
    return {
        "complete": unreadable == 0,
        "scanned": scanned,
        "vanished_mid_sample": vanished,
        "unreadable_pids": unreadable,
        "container_split": split,
        "vllm_container_split": vllm_split,
        "classes": classes,
        "runners": runners,
        "engine_core_count": engine_core,
        "rank_worker_count": rank_workers,
    }


# --------------------------------------------------------------------------
# Docker and tmux (read-only, fixed argv, bounded)
# --------------------------------------------------------------------------

_NODE_CONTAINER_RE = re.compile(r"^node-[A-Za-z0-9._-]+$")


def _scan_docker(run, timeout):
    """Return (running_count | None, details). count None = unknown."""
    details = {"reachable": None, "running_containers": None,
               "node_container_names": None, "problem": None}
    code, out, err, kind = run(
        ["docker", "ps", "--format", "{{.Names}}\t{{.Image}}"], timeout)
    if kind is not None:
        details["reachable"] = (False if kind == "missing" else None)
        details["problem"] = "docker %s" % kind
        return None, details
    if code != 0:
        problem = "permission denied" if "permission" in err.lower() \
            else "daemon unreachable"
        if "permission" in err.lower():
            details["reachable"] = False
        else:
            details["reachable"] = False
        details["problem"] = problem
        return None, details
    containers = []
    for line in out.splitlines():
        if "\t" in line:
            name, image = line.split("\t", 1)
        else:
            name, image = line, ""
        containers.append({"name": name, "image": image})
    details["reachable"] = True
    details["running_containers"] = containers
    details["node_container_names"] = [
        c["name"] for c in containers if _NODE_CONTAINER_RE.match(c["name"])]
    return len(containers), details


def _scan_tmux(run, timeout):
    """Return (session_count | None, details).

    count None = unknown (no binary / permission / timeout).
    0 with server_running False = no server, a normal state.
    """
    details = {"server_running": None, "sessions": None,
               "relevant_sessions": None, "problem": None}
    code, out, err, kind = run(
        ["tmux", "list-sessions", "-F", "#{session_name}"], timeout)
    lowered_err = (err or "").lower()
    if kind == "timeout" or kind == "error":
        details["problem"] = "tmux %s" % kind
        return None, details
    if kind == "missing":
        details["problem"] = "tmux binary not found"
        return None, details
    no_server = ("no server running" in lowered_err
                 or "error connecting" in lowered_err)
    if code != 0 and not no_server:
        details["problem"] = "tmux failed: %s" % err.strip()[:200]
        return None, details
    if no_server:
        details.update({"server_running": False, "sessions": [],
                        "relevant_sessions": []})
        return 0, details
    sessions = [line for line in out.splitlines() if line.strip()]
    relevant = [s for s in sessions
                if re.search(r"ray|vllm|node-\d|heavy|inference", s, re.I)]
    details.update({"server_running": True, "sessions": sessions,
                    "relevant_sessions": relevant})
    return len(sessions), details


class ProcessMetrics:
    """Node-local, read-only process/service snapshot.

    Parameters
    ----------
    proc_root: /proc-equivalent root (injectable for tests).
    runner: callable with the _run() signature (injectable for tests).
    clock_hz: ticks-per-second for /proc stat (defaults to kernel value).
    docker_timeout / tmux_timeout: short, bounded subprocess timeouts.
    """

    def __init__(self, proc_root=DEFAULT_PROC_ROOT, runner=None,
                 clock_hz=None, docker_timeout=5.0, tmux_timeout=3.0):
        self.proc_root = proc_root
        self.runner = runner or _run
        self.clock_hz = clock_hz or _default_clock_hz()
        self.docker_timeout = docker_timeout
        self.tmux_timeout = tmux_timeout

    def collect(self):
        warnings = []
        metrics = {
            "ollama_daemon_count": None,
            "ollama_runner_count": None,
            "ray_process_count": None,
            "vllm_process_count": None,
            "vllm_engine_core_count": None,
            "vllm_rank_worker_count": None,
            "docker_container_count": None,
            "tmux_session_count": None,
        }
        details = {"proc": None, "ollama": None, "ray": None,
                   "vllm": None, "docker": None, "tmux": None}

        proc = _scan_proc(self.proc_root, self.clock_hz)
        if proc is None:
            return _sample("error", "/proc unreadable", warnings,
                           metrics, details)
        details["proc"] = {
            "complete": proc["complete"],
            "scanned": proc["scanned"],
            "vanished_mid_sample": proc["vanished_mid_sample"],
            "unreadable_pids": proc["unreadable_pids"],
            "container_split": proc["container_split"],
        }
        if proc["complete"]:
            c = proc["classes"]
            for key, cls in [("ollama_daemon_count", "ollama_daemon"),
                             ("ollama_runner_count", "llama_runner"),
                             ("ray_process_count", "ray"),
                             ("vllm_process_count", "vllm")]:
                metrics[key] = len(c[cls])
            metrics["vllm_engine_core_count"] = proc["engine_core_count"]
            metrics["vllm_rank_worker_count"] = proc["rank_worker_count"]
            details["ollama"] = {
                "daemon_pids": sorted(c["ollama_daemon"]),
                "runner_pids": sorted(c["llama_runner"]),
                "runners": proc["runners"],
            }
            details["ray"] = {"present": bool(c["ray"]),
                              "pids": sorted(c["ray"])}
            details["vllm"] = {
                "present": bool(c["vllm"]),
                "pids": sorted(c["vllm"]),
                "container_split": proc["vllm_container_split"],
                "engine_core_count": proc["engine_core_count"],
                "rank_worker_count": proc["rank_worker_count"],
            }
        else:
            warnings.append("/proc scan incomplete: %d unreadable pid(s); "
                            "process counts reported as null"
                            % proc["unreadable_pids"])
            details["ollama"] = {"daemon_pids": None, "runner_pids": None,
                                 "runners": None}
            details["ray"] = {"present": None, "pids": None}
            details["vllm"] = {"present": None, "pids": None,
                               "container_split": None,
                               "engine_core_count": None,
                               "rank_worker_count": None}
        if proc["vanished_mid_sample"]:
            warnings.append("%d process(es) vanished mid-sample (normal)"
                            % proc["vanished_mid_sample"])

        dock_n, dock_d = _scan_docker(self.runner, self.docker_timeout)
        tmux_n, tmux_d = _scan_tmux(self.runner, self.tmux_timeout)
        metrics["docker_container_count"] = dock_n
        metrics["tmux_session_count"] = tmux_n
        details["docker"] = dock_d
        details["tmux"] = tmux_d
        for d, label in ((dock_d, "docker"), (tmux_d, "tmux")):
            if d["problem"]:
                warnings.append("%s: %s" % (label, d["problem"]))

        return _sample("ok", None, warnings, metrics, details)


def _default_clock_hz():
    try:
        return os.sysconf("SC_CLK_TCK")
    except (ValueError, OSError):
        return 100


def _sample(status, error, warnings, metrics, details):
    return {"timestamp": time.time(), "status": status, "error": error,
            "warnings": warnings, "metrics": metrics, "details": details}
