"""Read-only central aggregation layer for the SysLume DGX cluster (DGX01).

One :class:`ClusterAggregator.collect()` call produces a stable, JSON-safe
cluster snapshot:

* fetches the three node ``/api/telemetry`` endpoints (bounded timeout and
  bounded body size, each node isolated so one slow/dead node cannot block
  the others),
* validates each node's ``schema_version`` and ``node_id``,
* records per-node reachability, freshness (central receipt time) and the
  node's own collection quality,
* reads the authoritative ``cluster-model status --json`` **once**,
* combines everything into one snapshot.

Design invariants (Phase 18-19):

* Standard library only. No storage, no HTTP serving, no services.
* Never embed or log real tokens. Node credentials are read from protected
  (mode 0600) files; the Bearer token travels only in the HTTP
  ``Authorization`` header.
* Cluster mode is never inferred from process counts or port status. It is
  taken verbatim from ``cluster-model``. When the controller is unavailable
  the mode is ``unknown`` -- it is never guessed (e.g. as ``split``).
* Node telemetry quality (``state``/``fresh``) is kept strictly separate
  from authoritative cluster health (``cluster.healthy`` from the controller).
* A node that becomes unreachable retains its last successful sample for
  diagnostic display, but that retained sample is always flagged stale.
  Unavailable measurements are never zero-filled.
* Both the central receipt time and the remote sample timestamp are recorded
  so freshness never depends exclusively on synchronized node clocks.
"""

from __future__ import annotations

import json
import math
import os
import stat
import subprocess
import threading
import time
import urllib.error
import urllib.request

__all__ = [
    "ClusterAggregator",
    "ClusterModelController",
    "ControllerError",
    "NodeSource",
    "load_aggregator_config",
    "read_node_token",
    "AGGREGATOR_SCHEMA_VERSION",
    "NODE_SCHEMAS",
    "ControllerConfig",
]

#: Version of the aggregator's own cluster-snapshot schema (top level).
AGGREGATOR_SCHEMA_VERSION = 1

#: Node snapshot schema versions this aggregator accepts.  NodeCollector
#: currently emits version 1 (collectors.NODE_CONFIG / node_collector.py).
NODE_SCHEMAS = (1,)

#: Canonical node identity -> telemetry URL (matches the collector bind table
#: in collectors.collector_server.BIND_BY_NODE).  These are configuration, not
#: client input.
DEFAULT_NODE_SOURCES = {
    "dgx01": "http://127.0.0.1:8165/api/telemetry",
    "dgx02": "http://192.168.0.2:8165/api/telemetry",
    "dgx03": "http://192.168.2.2:8165/api/telemetry",
}

#: Nodes whose Ollama / Ray / vLLM activity is judged against the mode
#: expectation.  DGX01 (control node) is never judged against a split/heavy
#: worker expectation.
WORKER_NODES = ("dgx02", "dgx03")

# --- bounded budgets (defaults, all injectable) ----------------------------
# Per-request network timeout (Phase 18-19 design: "HTTP timeout 2 s").
DEFAULT_REQUEST_TIMEOUT_SECONDS = 2.0
# Global wall-clock budget for the node-fetch phase; keeps collect() bounded
# even if one node request_fn ignores its socket timeout.
DEFAULT_COLLECTION_DEADLINE_SECONDS = 8.0
# A node is "fresh"/online while within this window of its last successful
# central receipt (matches the design's "~<= 3x 5 s interval" online window).
DEFAULT_FRESH_THRESHOLD_SECONDS = 15.0
# Bounded body size for a single telemetry response.
DEFAULT_MAX_RESPONSE_BYTES = 4 * 1024 * 1024
# Bounded time / output for the cluster-model controller subprocess.
DEFAULT_CONTROLLER_TIMEOUT_SECONDS = 45.0
DEFAULT_CONTROLLER_MAX_BYTES = 1024 * 1024
DEFAULT_CONTROLLER_EXECUTABLE = "~/bin/cluster-model"
#: The controller command is a fixed, read-only argument list -- never a mode
#: verb, never a shell string.
CONTROLLER_STATUS_COMMAND = ("status", "--json")


class ControllerError(ValueError):
    """Raised when the cluster-model controller cannot be read or validated."""


class NodeSource:
    """Telemetry source for one node: its URL and (secret) Bearer token.

    ``token`` is the raw credential, read from a protected file; it is never
    logged or placed in any warning/error string.
    """

    __slots__ = ("node_id", "url", "token")

    def __init__(self, node_id, url, token=None):
        self.node_id = node_id
        self.url = url
        self.token = token

    def __repr__(self):
        # Never repr the token (avoid accidental log capture).
        tok = "set" if self.token is not None else "none"
        return "NodeSource(%s, %s, token=%s)" % (self.node_id, self.url, tok)


# --- controlled configuration / credentials ---------------------------------

class ControllerConfig:
    """Validated, fixed specification for the read-only controller command."""

    __slots__ = ("executable", "arguments", "timeout_seconds", "max_bytes")

    def __init__(self, executable, arguments=CONTROLLER_STATUS_COMMAND,
                 timeout_seconds=DEFAULT_CONTROLLER_TIMEOUT_SECONDS,
                 max_bytes=DEFAULT_CONTROLLER_MAX_BYTES):
        if not isinstance(executable, str) or not executable:
            raise ControllerError("controller executable must be a non-empty string")
        if "~" in executable:  # resolve a leading ~ to $HOME
            executable = os.path.expanduser(executable)
        if not isinstance(arguments, (list, tuple)) or not arguments:
            raise ControllerError("controller arguments must be a non-empty list")
        args = [str(a) for a in arguments]
        # The aggregator must ONLY ever read the status snapshot: the head
        # verb has to be 'status', and any known mutating cluster-model verb is
        # refused outright so a misconfiguration can never switch, stop, or
        # restart the cluster.
        if args[0] != "status":
            raise ControllerError(
                "controller command must read cluster state via 'status', got %r"
                % (args,))
        _MUTATING_VERBS = ("split", "heavy-qwen", "heavy-deepseek", "stop",
                           "up", "down", "restart", "reset", "reload", "reboot")
        for token in args:
            if token in _MUTATING_VERBS:
                raise ControllerError(
                    "refusing to run a mutating cluster-model command: %r" % (args,))
        if not (isinstance(timeout_seconds, (int, float)) and not isinstance(
                timeout_seconds, bool) and timeout_seconds > 0):
            raise ControllerError("controller timeout_seconds must be positive")
        if not (isinstance(max_bytes, int) and max_bytes > 0):
            raise ControllerError("controller max_bytes must be a positive int")
        self.executable = executable
        self.arguments = args
        self.timeout_seconds = float(timeout_seconds)
        self.max_bytes = int(max_bytes)

    @property
    def command(self):
        return [self.executable] + list(self.arguments)


def read_node_token(path):
    """Load a single Bearer token from a protected (mode 0600) file.

    Raises :class:`ControllerError` (a :class:`ValueError` subclass) if the
    path is missing, unreadable, not a regular file, grants read access to
    "other" (defeating the protection), or is empty.  Mirrors the fail-closed
    credential policy of the node collector server.
    """
    if not isinstance(path, str) or not path:
        raise ControllerError("token path must be a non-empty string")
    try:
        st = os.stat(path)
    except OSError:
        raise ControllerError("token file not readable: %s" % os.path.basename(path)) from None
    if not stat.S_ISREG(st.st_mode):
        raise ControllerError("token path is not a regular file: %s" % os.path.basename(path))
    if st.st_mode & 0o0044:  # group- or world-readable
        raise ControllerError("token file must not be group/world-readable (use 0600)")
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = fh.read()
    except OSError:
        raise ControllerError("token file could not be read: %s" % os.path.basename(path)) from None
    token = data.strip()
    if not token:
        raise ControllerError("token file is empty")
    return token


def _resolve_nodes(nodes):
    """Normalize a user-supplied node spec into an ordered NodeSource map.

    Accepts a mapping ``node_id -> (NodeSource | url-str | {"url":...,
    "token":... | "token_file":...})``.  When ``nodes`` is falsy the three
    validated default sources (no tokens) are used.  Token *files* are read
    here so the aggregator holds in-memory credentials, never files.
    """
    if nodes is None or len(nodes) == 0:
        nodes = {nid: url for nid, url in DEFAULT_NODE_SOURCES.items()}
    sources = {}
    order = list(nodes.keys())
    for node_id in order:
        if node_id not in DEFAULT_NODE_SOURCES:
            raise ValueError("unknown node_id: %r" % (node_id,))
        spec = nodes[node_id]
        if isinstance(spec, NodeSource):
            if spec.node_id != node_id:
                raise ValueError("node source identity mismatch for %r" % (node_id,))
            sources[node_id] = spec
        elif isinstance(spec, str):
            sources[node_id] = NodeSource(node_id, spec)
        elif isinstance(spec, dict):
            url = spec.get("url") or DEFAULT_NODE_SOURCES[node_id]
            token = spec.get("token")
            token_file = spec.get("token_file")
            if token is None and token_file is not None:
                token = read_node_token(token_file)
            if token is not None and not isinstance(token, str):
                raise ValueError("node token must be a string")
            sources[node_id] = NodeSource(node_id, url, token)
        else:
            raise ValueError("invalid node spec for %r" % (node_id,))
    return sources


_CONTROLLER_REQUIRED_KEYS = ("mode", "healthy", "transitioning", "schema")


def _validate_controller_payload(data):
    """Validate a parsed controller JSON document is a usable status object.

    Only *presence* and basic shape are checked (never invent members).  The
    schema key is the controller's own (``schema``), distinct from the node
    snapshot's ``schema_version``.
    """
    if not isinstance(data, dict):
        raise ControllerError("controller output is not a JSON object")
    for key in _CONTROLLER_REQUIRED_KEYS:
        if key not in data:
            raise ControllerError("controller output missing %r" % (key,))
    schema = data["schema"]
    if not isinstance(schema, int) or isinstance(schema, bool) or schema < 1:
        raise ControllerError("unsupported controller schema %r" % (schema,))


class ClusterModelController:
    """Read a single ``cluster-model status --json`` snapshot.

    The command is a **fixed executable + argument list**:

    * launched with ``shell=False`` (a literal argv, no shell expansion),
    * bounded by ``timeout_seconds`` (a hard wall-clock ceiling),
    * with a bounded ``max_bytes`` read from stdout (oversized output is
      rejected, not streamed unbounded), and
    * the stdout must parse to a validated JSON status object.

    ``status`` returns the parsed JSON dict.  A non-zero exit is *not* an
    error by itself: ``cluster-model status`` exits non-zero when the observed
    mode is unhealthy, yet still emits valid JSON, so stdout is parsed before
    the exit code is consulted.  No mutating verb can be expressed here.
    """

    def __init__(self, executable=DEFAULT_CONTROLLER_EXECUTABLE,
                 arguments=CONTROLLER_STATUS_COMMAND,
                 timeout_seconds=DEFAULT_CONTROLLER_TIMEOUT_SECONDS,
                 max_bytes=DEFAULT_CONTROLLER_MAX_BYTES,
                 runner=None):
        self._spec = ControllerConfig(executable, arguments=arguments,
                                      timeout_seconds=timeout_seconds,
                                      max_bytes=max_bytes)
        # ``runner`` (cmd, timeout, max_bytes) -> (returncode, stdout_bytes).
        self._runner = runner if runner is not None else _subprocess_run_status

    @property
    def command(self):
        return self._spec.command

    def status(self):
        """Run the read-only status command and return its validated JSON."""
        cmd = self._spec.command
        try:
            _returncode, stdout = self._runner(
                cmd, self._spec.timeout_seconds, self._spec.max_bytes)
        except subprocess.TimeoutExpired:
            raise ControllerError(
                "controller timed out after %.1fs" % self._spec.timeout_seconds) from None
        except FileNotFoundError:
            raise ControllerError(
                "controller executable not found: %s"
                % os.path.basename(self._spec.executable)) from None
        except OSError as exc:
            raise ControllerError(
                "controller could not be started: %s" % type(exc).__name__) from None
        if not isinstance(stdout, (bytes, bytearray)):
            raise ControllerError("controller returned non-bytes stdout")
        if len(stdout) > self._spec.max_bytes:
            raise ControllerError(
                "controller output too large (%d > %d bytes)"
                % (len(stdout), self._spec.max_bytes))
        try:
            data = json.loads(stdout.decode("utf-8", "replace"))
        except ValueError:
            raise ControllerError("controller output is not valid JSON") from None
        _validate_controller_payload(data)
        # returncode is intentionally not treated as fatal once JSON validated:
        # an unhealthy-but-readable status is still observable state.
        return data


def _subprocess_run_status(cmd, timeout, max_bytes):
    """Default runner: a bounded, non-shell subprocess with bounded stdout.

    Returns ``(returncode, stdout_bytes)``.  Enforces a hard ``timeout`` and a
    bounded ``max_bytes`` read from the stdout pipe (kills the child if it
    overflows) so a misbehaving controller can neither hang nor flood memory.
    stderr is discarded (the status JSON is on stdout only).
    """
    return _run_bounded(cmd, timeout, max_bytes)


def _run_bounded(cmd, timeout, max_bytes):
    """Run ``cmd`` with a wall-clock ``timeout`` and a bounded stdout read.

    The stdout is drained on a helper thread in small chunks, stopping once
    ``max_bytes + 1`` bytes have been seen (so the caller can *detect* an
    overflow without allocating it all) or on EOF.  The global ``timeout`` is
    enforced by joining that thread; on expiry the child is killed.  Returns
    ``(returncode, stdout_bytes)`` and raises :class:`subprocess.TimeoutExpired`
    if the process outlives the budget.
    """
    proc = subprocess.Popen(
        cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL)
    cap = int(max_bytes) + 1
    box = {"data": b"", "overflow": False}

    def drain():
        buf = box["data"]
        while True:
            chunk = proc.stdout.read(4096)
            if not chunk:
                break
            buf += chunk
            if len(buf) > cap:  # enough to prove overflow; stop reading
                box["overflow"] = True
                break
        box["data"] = buf

    reader = threading.Thread(target=drain, name="agg-ctl-reader", daemon=True)
    reader.start()
    reader.join(timeout)
    if reader.is_alive():
        proc.kill()
        reader.join(2.0)
        raise subprocess.TimeoutExpired(cmd, timeout)
    try:
        returncode = proc.wait(timeout=2.0)
    except subprocess.TimeoutExpired:
        proc.kill()
        try:
            returncode = proc.wait(timeout=2.0)
        except subprocess.TimeoutExpired:
            raise subprocess.TimeoutExpired(cmd, timeout)
    proc.stdout.close()
    return returncode, box["data"]


class NodeFetchError(RuntimeError):
    """A telemetry fetch failed at the transport/credential layer."""


def _http_get(url, token, timeout, max_bytes):
    """Default request_fn: a bounded GET returning the parsed JSON payload.

    Applies a socket ``timeout`` and a bounded ``max_bytes`` body read, sending
    a Bearer ``Authorization`` header only when a token is present.  Raises
    :class:`NodeFetchError` with a sanitized (secret-free) message on any
    transport/HTTP/size/parse problem.
    """
    req = urllib.request.Request(url, method="GET")
    if token is not None:
        req.add_header("Authorization", "Bearer " + token)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read(max_bytes + 1)
            code = resp.getcode()
    except urllib.error.HTTPError as exc:
        raise NodeFetchError("HTTP %s from %s" % (exc.code, _host(url))) from None
    except urllib.error.URLError as exc:
        reason = _safe_reason(exc)
        raise NodeFetchError("connection error to %s: %s" % (_host(url), reason)) from None
    except (OSError, ValueError) as exc:  # socket read errors / bad status
        raise NodeFetchError("%s from %s" % (_safe_type(exc), _host(url))) from None
    if len(body) > max_bytes:
        raise NodeFetchError("response too large from %s" % _host(url))
    if code is not None and code != 200:
        raise NodeFetchError("HTTP %s from %s" % (code, _host(url)))
    try:
        return json.loads(body.decode("utf-8", "replace"))
    except ValueError:
        raise NodeFetchError("non-JSON response from %s" % _host(url)) from None


def _host(url):
    """Best-effort host:port for sanitized (port-free-of-secrets) messages."""
    try:
        return urllib.request.urlparse(url).netloc or url
    except (ValueError, AttributeError):
        return url


def _safe_reason(exc):
    reason = getattr(exc, "reason", exc)
    text = str(reason) if getattr(reason, "reason", None) is None else str(reason)
    return _sanitize_text(text)


def _safe_type(exc):
    return type(exc).__name__


def _sanitize_text(text):
    """Strip anything that looks like a Bearer token out of free-form text."""
    if not isinstance(text, str):
        return str(text)[:200]
    cleaned = " ".join(text.split())
    return cleaned[:200]


#: Per-node expectation for each authoritative cluster mode.  Values are
#: "active" | "inactive" | "any".  The control node (dgx01) is exempt.
MODE_EXPECTATIONS = {
    "split":           {"ollama": "active",   "heavy": "inactive"},
    "heavy-qwen":      {"ollama": "inactive", "heavy": "active"},
    "heavy-deepseek":  {"ollama": "inactive", "heavy": "active"},
    "stop":            {"ollama": "inactive", "heavy": "inactive"},
    "transitioning":   {"ollama": "any",      "heavy": "any"},
}
_NEUTRAL_EXPECTATION = {"ollama": "any", "heavy": "any"}


class _NewNodeState:
    """Rolling per-node observation state (mutable, held by the aggregator)."""

    __slots__ = ("last_success_at", "last_fresh_at", "last_sample",
                 "last_error", "remote_stale", "attempts", "failures")

    def __init__(self):
        # Central receipt tracking (local clock; never the remote wall clock).
        self.last_success_at = None   # last accepted response (any staleness)
        self.last_fresh_at = None     # last accepted response with stale != True
        self.last_sample = None
        self.last_error = None
        self.remote_stale = None
        self.attempts = 0
        self.failures = 0


class ClusterAggregator:
    """Combine three node telemetry endpoints + the controller into one snapshot.

    Parameters
    ----------
    nodes:
        Optional mapping ``node_id -> NodeSource | url | {"url", "token",
        "token_file"}``.  Omitting it uses the three validated defaults.
    controller:
        Any object exposing ``status() -> dict`` (typically a
        :class:`ClusterModelController`).  ``None`` (the default) means "use the
        standard cluster-model controller" (``~/bin/cluster-model status --json``
        by default), so an out-of-the-box aggregator reports the cluster mode.
        Pass ``False`` to disable the controller entirely (cluster mode is then
        reported as ``unknown``).  A missing/unreadable controller degrades
        gracefully to ``unknown`` -- never guessed, never fatal.
    clock:
        Zero-argument callable returning the current epoch time (for
        deterministic staleness tests).
    request_fn:
        ``request_fn(url, token, timeout) -> payload_dict``.  Defaults to
        :func:`_http_get`.  Injected in tests.
    fresh_threshold_seconds / request_timeout_seconds /
    collection_deadline_seconds / max_response_bytes:
        Bounded budgets (see module defaults).
    """

    def __init__(self, nodes=None, controller=None, clock=None, request_fn=None,
                 fresh_threshold_seconds=DEFAULT_FRESH_THRESHOLD_SECONDS,
                 request_timeout_seconds=DEFAULT_REQUEST_TIMEOUT_SECONDS,
                 collection_deadline_seconds=DEFAULT_COLLECTION_DEADLINE_SECONDS,
                 max_response_bytes=DEFAULT_MAX_RESPONSE_BYTES,
                 supported_schema_versions=NODE_SCHEMAS):
        self._nodes = _resolve_nodes(nodes)
        if controller is False:                       # explicitly disabled
            self._controller = None
        elif controller is None:                      # -> standard controller
            self._controller = ClusterModelController()
        else:
            self._controller = controller
        self._clock = clock if clock is not None else time.time
        if request_fn is None:
            self._request_fn = lambda url, token, timeout: _http_get(
                url, token, timeout, self._max_response_bytes)
        else:
            self._request_fn = request_fn
        if not supported_schema_versions:
            raise ValueError("supported_schema_versions must be non-empty")
        self._schemas = tuple(supported_schema_versions)
        if not (isinstance(fresh_threshold_seconds, (int, float)) and fresh_threshold_seconds > 0):
            raise ValueError("fresh_threshold_seconds must be positive")
        if not (isinstance(request_timeout_seconds, (int, float)) and request_timeout_seconds > 0):
            raise ValueError("request_timeout_seconds must be positive")
        if not (isinstance(collection_deadline_seconds, (int, float)) and collection_deadline_seconds > 0):
            raise ValueError("collection_deadline_seconds must be positive")
        if not (isinstance(max_response_bytes, int) and max_response_bytes > 0):
            raise ValueError("max_response_bytes must be a positive int")
        self._fresh_threshold = float(fresh_threshold_seconds)
        self._request_timeout = float(request_timeout_seconds)
        self._deadline = float(collection_deadline_seconds)
        self._max_response_bytes = int(max_response_bytes)
        # Per-node state, keyed by node_id in canonical order.
        self._state = {nid: _NewNodeState() for nid in self._nodes}
        # In-flight fetch per node: node_id -> (thread, result-holder).  A node
        # whose previous fetch is still running is never re-launched (bounded
        # thread lifetime: at most one outstanding fetch per node).
        self._inflight = {}

    # -- node fetch phase (bounded, per-node isolated) ----------------------
    def _fetch_node(self, source):
        """Fetch one node's telemetry payload.

        Returns ``("ok", payload)`` (transport succeeded; ``payload`` may be any
        JSON value) or ``("error", message)`` (transport/credential failure).
        Never raises: a failure is captured so one node can never take down the
        others.  Content validity is judged separately by :meth:`_process_node`.
        """
        try:
            payload = self._request_fn(source.url, source.token,
                                       self._request_timeout)
        except NodeFetchError as exc:
            return ("error", str(exc))
        except Exception as exc:  # a request_fn bug must not break the phase
            return ("error", "%s: %s" % (_safe_type(exc), _sanitize_text(str(exc))))
        return ("ok", payload)

    def _fetch_nodes(self):
        """Fetch all nodes concurrently, bounded by a global wall-clock deadline.

        A slow/dead/hanging node is isolated to its own daemon thread; joining
        each thread against a shared deadline guarantees a single slow node
        (e.g. DGX02) can never block collection of the others past the budget.

        Bounded thread lifetime: a node whose previous fetch is still in
        flight reuses that fetch instead of being re-launched, so repeated
        timeouts can never accumulate abandoned threads, overlap unbounded
        fetches, or grow memory (at most one outstanding fetch per node,
        each a daemon thread that cannot delay process shutdown).  A late
        result is consumed on the next cycle that waits for it.
        Returns ``{node_id: ("ok"|"error", payload_or_message)}``.
        """
        threads = {}
        holders = {}
        for node_id, source in self._nodes.items():
            inflight = self._inflight.get(node_id)
            if inflight is not None and (inflight[0].is_alive()
                                         or "result" in inflight[1]):
                # Reuse the previous fetch: still running, or finished but
                # not yet consumed.  Never re-launch a node that already has
                # an outstanding fetch (bounded threads, no duplicates).
                threads[node_id], holders[node_id] = inflight
                continue
            holder = {}

            def worker(_src=source, _h=holder):
                _h["result"] = self._fetch_node(_src)

            thread = threading.Thread(target=worker, name="agg-%s" % node_id,
                                      daemon=True)
            thread.start()
            self._inflight[node_id] = (thread, holder)
            threads[node_id], holders[node_id] = thread, holder
        deadline = self._clock() + self._deadline
        results = {}
        for node_id, t in threads.items():
            remaining = max(0.0, deadline - self._clock())
            t.join(remaining)
            h = holders[node_id]
            if "result" in h:
                results[node_id] = h["result"]
                self._inflight.pop(node_id, None)
            else:  # the fetch is still running past the shared deadline
                results[node_id] = ("error", "timed out after %.1fs bounded wait"
                                    % self._deadline)
                # keep the in-flight entry; the late result is consumed later
        return results

    # -- controller read (once per collect) --------------------------------
    def _read_controller(self):
        """Read cluster-model status once. Returns a dict: ok/data/error.

        Any exception (missing binary, timeout, non-JSON, invalid schema) is
        captured as ok=False with a sanitized error.  Never raises.
        """
        if self._controller is None:
            return {"ok": False, "data": None, "error": "no controller configured"}
        try:
            data = self._controller.status()
        except ControllerError as exc:
            return {"ok": False, "data": None, "error": str(exc)}
        except Exception as exc:  # a controller bug must not break collection
            return {"ok": False, "data": None,
                    "error": "%s: %s" % (_safe_type(exc), _sanitize_text(str(exc)))}
        if not isinstance(data, dict):
            return {"ok": False, "data": None,
                    "error": "controller returned a non-object payload"}
        return {"ok": True, "data": data, "error": None}

    def _controller_view(self, controller):
        """Build the authoritative ``cluster`` block + controller warnings.

        mode / healthy / transitioning are taken *verbatim* from the
        controller.  When the controller is unavailable the mode is ``unknown``
        and healthy/transitioning are ``None`` -- never guessed.
        """
        warnings = []
        source = "cluster-model status --json"
        if controller["ok"]:
            data = controller["data"]
            mode = data.get("mode")
            healthy = data.get("healthy")
            transitioning = data.get("transitioning")
            if not isinstance(mode, str):
                mode = "unknown"
            if not isinstance(healthy, bool):
                healthy = None
            if not isinstance(transitioning, bool):
                transitioning = None
            block = {
                "mode": mode,
                "healthy": healthy,
                "transitioning": transitioning,
                "controller": "ok",
                "source": source,
                "raw": data,
                "error": None,
            }
        else:
            warnings.append(
                "cluster controller unavailable (%s); cluster mode reported as "
                "unknown" % _sanitize_text(controller["error"]))
            block = {
                "mode": "unknown",
                "healthy": None,
                "transitioning": None,
                "controller": "error",
                "source": source,
                "raw": None,
                "error": controller["error"],
            }
        return block, warnings

    def _process_node(self, node_id, result, now, mode):
        """Combine one node's fetch result + rolling state into a node entry."""
        state = self._state[node_id]
        state.attempts += 1
        reachable = result is not None and result[0] == "ok"
        schema_valid = None
        identity_valid = None
        error = None
        accepted = False
        self_stale = None
        if reachable:
            payload = result[1]
            if not isinstance(payload, dict):
                schema_valid = False          # content invalid at the top level
                error = "telemetry payload is not a JSON object"
                state.failures += 1
            else:
                node = payload.get("node")
                if not isinstance(node, dict):
                    schema_valid = False
                    error = "telemetry payload missing 'node' snapshot"
                    state.failures += 1
                else:
                    sv = node.get("schema_version")
                    nid = node.get("node_id")
                    schema_valid = sv in self._schemas
                    identity_valid = nid == node_id
                    self_stale = payload.get("stale") if isinstance(
                        payload.get("stale"), bool) else None
                    if not schema_valid:
                        error = "unsupported schema_version %r" % (sv,)
                        state.failures += 1
                    elif not identity_valid:
                        error = "node_id mismatch: expected %s got %r" % (node_id, nid)
                        state.failures += 1
                    else:
                        accepted, sample = True, node
        else:
            error = (result[1] if (result is not None and result[0] == "error")
                     else "no result")
            state.failures += 1

        if accepted:
            state.last_success_at = now          # central receipt of the response
            state.last_sample = sample
            state.last_error = None
            if self_stale is not True:
                state.last_fresh_at = now        # central receipt of fresh data
            state.remote_stale = self_stale
            # The four freshness distinctions:
            #   reachable           - the endpoint answered this poll (above);
            #   response recent     - ``age_seconds`` ~ 0 (central receipt);
            #   sample fresh/stale  - the remote ``stale`` indicator is
            #                         authoritative for the underlying sample.
            # A node that keeps serving its cached snapshot (HTTP 200 with
            # stale=true) therefore stays stale across repeated successful
            # polls; only a response whose remote stale indicator is not set
            # refreshes the sample.  Recency is measured by central receipt
            # times, never by remote wall-clock synchronization alone.
            sample_stale = self_stale is True
            cur = "stale" if sample_stale else "online"
            fresh = not sample_stale
        else:
            state.last_error = error
            fresh = False
            if state.last_sample is None:
                cur, sample_stale = "never", False   # nothing to display
            else:
                age = now - state.last_success_at
                cur = "stale" if age <= self._fresh_threshold else "error"
                sample_stale = True                  # retained, never current

        last_sample = state.last_sample
        sample_ts = last_sample.get("timestamp") if isinstance(last_sample, dict) else None
        entry = {
            "state": cur,
            "reachable": reachable,
            # fresh == response received recently AND the sample is fresh
            "fresh": fresh,
            # the underlying sample's freshness (distinct from the response's)
            "sample_fresh": cur == "online",
            "sample_stale": sample_stale,
            # collection quality == the node's own reported status (kept
            # separate from authoritative cluster health)
            "quality": (last_sample.get("status")
                        if isinstance(last_sample, dict) else None),
            "schema_valid": schema_valid,
            "identity_valid": identity_valid,
            "last_success_at": state.last_success_at,
            "age_seconds": ((now - state.last_success_at)
                            if state.last_success_at is not None else None),
            # central age of the underlying sample: seconds since the last
            # accepted response whose remote stale indicator was not set
            "sample_age_seconds": ((now - state.last_fresh_at)
                                   if state.last_fresh_at is not None else None),
            "error": None if accepted else state.last_error,
            # the sample shown is the last real one (never zero-filled); when
            # a node is down it is the retained, explicitly-stale sample
            "sample": last_sample,
            "sample_timestamp": sample_ts,
            "remote_reported_stale": state.remote_stale,
            "mode_expected": _mode_expected(node_id, mode),
            "as_expected": (_judged_as_expected(node_id, last_sample, mode)
                            if (accepted and cur == "online") else None),
        }
        return entry

    # -- public API ---------------------------------------------------------
    def collect(self):
        """One bounded, read-only cluster snapshot (JSON-serializable).

        Fetches the nodes (bounded, in parallel), reads the controller once,
        then combines them into a stable snapshot.  Never raises for a node
        failure or a controller failure; problems are reported in per-node
        state and top-level warnings.
        """
        now = self._clock()
        node_results = self._fetch_nodes()
        nodes_done = self._clock()
        controller = self._read_controller()
        ctl_done = self._clock()

        cluster_block, warnings = self._controller_view(controller)
        mode = cluster_block["mode"]

        nodes_out = {}
        for node_id in self._nodes:
            entry = self._process_node(node_id, node_results.get(node_id), now, mode)
            nodes_out[node_id] = entry

        for node_id, entry in nodes_out.items():
            if (entry["state"] == "stale" and entry["reachable"]
                    and entry["remote_reported_stale"] is True):
                detail = ("endpoint is responding but serving a cached sample "
                          "(remote stale indicator set)")
                age = entry["sample_age_seconds"]
                if age is not None:
                    detail += "; last fresh central receipt %.0fs ago" % age
                else:
                    detail += "; no fresh sample received yet"
                warnings.append(
                    "%s: telemetry stale (reachable=True): %s"
                    % (node_id, detail))
            elif entry["state"] != "online":
                if entry["error"]:
                    detail = _sanitize_text(entry["error"])
                else:
                    detail = "no sample yet"
                warnings.append(
                    "%s: telemetry %s (reachable=%s): %s"
                    % (node_id, entry["state"], entry["reachable"], detail))
            elif entry["as_expected"] is False:
                warnings.append(
                    "%s: observed activity deviates from %s-mode expectation "
                    "(fresh sample)" % (node_id, mode))

        snapshot = {
            "schema_version": AGGREGATOR_SCHEMA_VERSION,
            "timestamp": now,
            "cluster": cluster_block,
            "nodes": nodes_out,
            "warnings": warnings,
            "meta": {
                "nodes_ms": int((nodes_done - now) * 1000),
                "controller_ms": int((ctl_done - nodes_done) * 1000),
                "total_ms": int((ctl_done - now) * 1000),
                "deadline_seconds": self._deadline,
                "fresh_threshold_seconds": self._fresh_threshold,
            },
        }
        return _jsonify(snapshot)


# --- module-level helpers --------------------------------------------------

def _jsonify(value):
    """Recursively make a value JSON-safe (non-finite floats become None)."""
    if isinstance(value, dict):
        return {key: _jsonify(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonify(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _numeric_at_least(metrics, key, threshold=1):
    value = metrics.get(key)
    return (isinstance(value, (int, float)) and not isinstance(value, bool)
            and value >= threshold)


def _obs_ollama_active(metrics):
    if not isinstance(metrics, dict):
        return False
    return (_numeric_at_least(metrics, "ollama_up")
            or _numeric_at_least(metrics, "ollama_daemon_count")
            or _numeric_at_least(metrics, "ollama_runner_count"))


def _obs_heavy_active(metrics):
    if not isinstance(metrics, dict):
        return False
    return (any(_numeric_at_least(metrics, key)
                for key in ("vllm_process_count", "ray_process_count",
                            "vllm_engine_core_count", "vllm_rank_worker_count")))


def _matches(observed_active, expectation):
    if expectation == "active":
        return observed_active
    if expectation == "inactive":
        return not observed_active
    return True  # "any"


def _mode_expected(node_id, mode):
    """Mode-aware expectation for a node, or neutral when not applicable."""
    if node_id in WORKER_NODES and mode in MODE_EXPECTATIONS:
        return dict(MODE_EXPECTATIONS[mode])
    return dict(_NEUTRAL_EXPECTATION)


def _judged_as_expected(node_id, sample, mode):
    """True/False when a *fresh* worker sample is judged against the mode.

    Returns None when not judged: the control node, an unknown mode, or a
    ``transitioning`` mode (where temporary service changes are expected).
    Judging uses only the node's own process/activity metrics -- it never
    infers the mode itself (that comes solely from the controller).
    """
    if node_id not in WORKER_NODES or mode not in MODE_EXPECTATIONS:
        return None
    if mode == "transitioning":
        return None
    if not isinstance(sample, dict) or not isinstance(sample.get("metrics"), dict):
        return None
    exp = MODE_EXPECTATIONS[mode]
    return bool(_matches(_obs_ollama_active(sample["metrics"]), exp["ollama"])
                and _matches(_obs_heavy_active(sample["metrics"]), exp["heavy"]))


def load_aggregator_config(path):
    """Load a central-aggregator config and return a constructed aggregator.

    Config JSON shape (all optional except where noted):
      {
        "nodes": {                      # omit to use the three defaults
          "dgx01": {"url": "http://127.0.0.1:8165/api/telemetry",
                    "token_file": "/home/.../token.dgx01"},
          "dgx02": {"token_file": "/home/.../token.dgx02"},
          "dgx03": {"url": "...", "token_file": "..."}
        },
        "controller": {                 # omit to disable the controller
          "executable": "~/bin/cluster-model",
          "arguments": ["status", "--json"],
          "timeout_seconds": 45,
          "max_bytes": 1048576
        },
        "fresh_threshold_seconds": 15,
        "request_timeout_seconds": 2,
        "collection_deadline_seconds": 8,
        "max_response_bytes": 4194304
      }

    Credentials are read here from protected (0600) token files and held in
    memory only -- never embedded in source, argv, or logs.
    """
    try:
        with open(path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
    except (OSError, ValueError) as exc:
        raise ControllerError("could not read aggregator config %s: %s"
                              % (os.path.basename(path), type(exc).__name__))
    if not isinstance(raw, dict):
        raise ControllerError("aggregator config must be a JSON object")

    controller = False   # absent  -> disabled (the config is authoritative)
    if raw.get("controller") is not None:
        ccfg = raw["controller"]
        if not isinstance(ccfg, dict):
            raise ControllerError("controller config must be a JSON object")
        controller = ClusterModelController(
            executable=ccfg.get("executable", DEFAULT_CONTROLLER_EXECUTABLE),
            arguments=ccfg.get("arguments", list(CONTROLLER_STATUS_COMMAND)),
            timeout_seconds=ccfg.get("timeout_seconds",
                                       DEFAULT_CONTROLLER_TIMEOUT_SECONDS),
            max_bytes=ccfg.get("max_bytes", DEFAULT_CONTROLLER_MAX_BYTES))
    return ClusterAggregator(
        nodes=raw.get("nodes"),
        controller=controller,
        fresh_threshold_seconds=raw.get("fresh_threshold_seconds",
                                         DEFAULT_FRESH_THRESHOLD_SECONDS),
        request_timeout_seconds=raw.get("request_timeout_seconds",
                                         DEFAULT_REQUEST_TIMEOUT_SECONDS),
        collection_deadline_seconds=raw.get("collection_deadline_seconds",
                                             DEFAULT_COLLECTION_DEADLINE_SECONDS),
        max_response_bytes=raw.get("max_response_bytes",
                                    DEFAULT_MAX_RESPONSE_BYTES))
