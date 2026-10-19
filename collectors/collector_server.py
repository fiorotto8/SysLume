"""Authenticated read-only node telemetry HTTP server for SysLume Cluster.

This is the per-node collector service described in the Phase 18-19 design
document.  One instance runs on each DGX and serves a single cached
:class:`~collectors.node_collector.NodeCollector` snapshot over a small
GET-only HTTP API:

* ``GET /api/telemetry`` -- latest cached snapshot plus transport metadata.
  Requires a Bearer token and a permitted TCP peer.
* ``GET /healthz``       -- service/sampling/sample health, sample age and
  staleness.  Requires a permitted TCP peer but no token.

Design invariants enforced here:

* **One long-lived :class:`NodeCollector` per server.**  A single background
  sampling thread refreshes the cache; HTTP handlers only read already-collected
  data and never block on a collection or re-enter ``collect()``.
* **Never bind to ``0.0.0.0``, the management LAN, or a Tailscale IP.**  The
  bind address is allow-listed per node identity.
* **Fail closed on credentials / configuration.**  A missing, unreadable, or
  world-readable token file aborts startup.  Token comparison is constant-time
  via :func:`hmac.compare_digest`.
* **No secrets in logs or responses.**  The handler logs nothing; error bodies
  are fixed, sanitized strings.

Standard library only.  No state-changing endpoints, no shell, no proxying.
"""

from __future__ import annotations

import hmac
import json
import os
import stat
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

__all__ = [
    "CollectorServerConfigError",
    "CollectorServer",
    "load_collector_config",
    "read_token_file",
    "BIND_BY_NODE",
    "PEER_BY_NODE",
    "DEFAULT_PORT",
    "DEFAULT_INTERVAL_SECONDS",
    "DEFAULT_STALE_THRESHOLD_SECONDS",
    "STALE_THRESHOLD_NOTE",
]

# Fixed per-node bind addresses and the DGX01-facing peer that may reach it.
# Only these binds are permitted; anything else (0.0.0.0, management LAN,
# Tailscale/CGNAT) is rejected at config-validation time.
BIND_BY_NODE = {
    "dgx01": "127.0.0.1",
    "dgx02": "192.168.0.2",
    "dgx03": "192.168.2.2",
}
PEER_BY_NODE = {
    "dgx01": "127.0.0.1",
    "dgx02": "192.168.0.1",
    "dgx03": "192.168.2.1",
}
DEFAULT_PORT = 8165
DEFAULT_INTERVAL_SECONDS = 15.0
# Worst-case single collection is ~25 s (Ollama 3x4s + docker 5s + tmux 3s +
# nvidia-smi 5s).  A healthy loop lands a success at most one interval plus one
# collection apart (~40 s), so 60 s is a safe "stale if no success yet" line.
DEFAULT_STALE_THRESHOLD_SECONDS = 60.0
STALE_THRESHOLD_NOTE = (
    "stale_threshold must exceed interval_seconds plus the ~25 s worst-case "
    "collection budget; default is 2x that (~60 s)"
)

_MAX_RESPONSE_BYTES = 8 * 1024 * 1024
_MAX_AUTH_HEADER = 512



class CollectorServerConfigError(ValueError):
    """Raised for invalid collector server configuration or credentials."""


def read_token_file(path):
    """Read a single Bearer token from a protected local file.

    Returns the token string (whitespace-stripped, single line).  Raises
    :class:`CollectorServerConfigError` if the path does not exist, is not a
    regular file, is empty, or grants read access to "other" (world-readable)
    which would defeat the 0600 protection requirement.
    """
    if not isinstance(path, str) or not path:
        raise CollectorServerConfigError("token_file path must be a non-empty string")
    try:
        st = os.stat(path)
    except OSError as exc:
        raise CollectorServerConfigError(
            "token_file not readable: %s" % exc.__class__.__name__
        ) from None
    if not stat.S_ISREG(st.st_mode):
        raise CollectorServerConfigError("token_file is not a regular file")
    if st.st_mode & 0o0044:  # group- or world-readable
        raise CollectorServerConfigError(
            "token_file must not be group/world-readable (use mode 0600)"
        )
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = fh.read()
    except OSError as exc:
        raise CollectorServerConfigError(
            "token_file could not be read: %s" % exc.__class__.__name__
        ) from None
    token = data.strip()
    if not token:
        raise CollectorServerConfigError("token_file is empty")
    return token


def _is_valid_ipv4(value):
    if not isinstance(value, str):
        return False
    parts = value.split(".")
    if len(parts) != 4:
        return False
    for part in parts:
        if not part.isdigit() or len(part) > 3:
            return False
        if int(part) > 255:
            return False
    return True


def _is_safe_bind(node_id, bind):
    expected = BIND_BY_NODE.get(node_id)
    return expected is not None and bind == expected


def _validate_config(raw):
    if not isinstance(raw, dict):
        raise CollectorServerConfigError("config must be a JSON object")
    node_id = raw.get("node_id")
    if node_id not in BIND_BY_NODE:
        raise CollectorServerConfigError("unknown or missing node_id: %r" % (node_id,))
    bind = raw.get("bind")
    if not _is_valid_ipv4(bind) or not _is_safe_bind(node_id, bind):
        raise CollectorServerConfigError(
            "bind %r is not permitted for node %r (must be %r)"
            % (bind, node_id, BIND_BY_NODE[node_id]))
    port = raw.get("port", DEFAULT_PORT)
    if not isinstance(port, int) or isinstance(port, bool) or not (0 <= port <= 65535):
        raise CollectorServerConfigError("port must be an integer 0..65535 (0 = ephemeral)")
    interval = raw.get("interval_seconds", DEFAULT_INTERVAL_SECONDS)
    if not isinstance(interval, (int, float)) or isinstance(interval, bool) \
            or not (0.05 <= float(interval) <= 86400.0):
        raise CollectorServerConfigError("interval_seconds must be a number in [0.05, 86400]")
    stale = raw.get("stale_threshold_seconds", DEFAULT_STALE_THRESHOLD_SECONDS)
    if not isinstance(stale, (int, float)) or isinstance(stale, bool) \
            or float(stale) <= 0:
        raise CollectorServerConfigError("stale_threshold_seconds must be a positive number")
    peers = raw.get("allowed_peers")
    if peers is None:
        peers = [PEER_BY_NODE[node_id]]
    if not isinstance(peers, (list, tuple)) or not peers:
        raise CollectorServerConfigError("allowed_peers must be a non-empty list")
    for peer in peers:
        if not _is_valid_ipv4(peer) or peer == "0.0.0.0":
            raise CollectorServerConfigError("invalid allowed peer: %r" % (peer,))
    token_file = raw.get("token_file")
    if not isinstance(token_file, str) or not token_file:
        raise CollectorServerConfigError("token_file must be a non-empty string")
    return {
        "node_id": node_id,
        "bind": bind,
        "port": int(port),
        "interval_seconds": float(interval),
        "stale_threshold_seconds": float(stale),
        "allowed_peers": [str(p) for p in peers],
        "token_file": token_file,
    }


def load_collector_config(path):
    """Load and validate a collector server config from a JSON file.

    Returns the validated config dict.  The token is NOT read here; the token
    file is read separately by :class:`CollectorServer` so a bad token does not
    invalidate the rest of the config in error messages.
    """
    if not isinstance(path, str) or not path:
        raise CollectorServerConfigError("config path must be a non-empty string")
    try:
        with open(path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
    except (OSError, ValueError) as exc:
        raise CollectorServerConfigError(
            "could not read config %r: %s" % (os.path.basename(path),
                                              exc.__class__.__name__)
        ) from None
    return _validate_config(raw)


class _Sampler:
    """Single-threaded sampler refreshing one NodeCollector's cache."""

    def __init__(self, node_collector, interval, time_func, on_result):
        self._nc = node_collector
        self._interval = interval
        self._time = time_func
        self._on_result = on_result
        self._stop = threading.Event()
        self._thread = None
        self._collect_lock = threading.Lock()

    def started(self):
        return self._thread is not None

    def start(self):
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="nc-sampler",
                                        daemon=True)
        self._thread.start()

    def stop(self, timeout):
        if self._thread is None:
            return True
        self._stop.set()
        self._thread.join(timeout)
        alive = self._thread.is_alive()
        self._thread = None
        return not alive

    def _do_collection(self):
        """Perform one collection attempt and report it via on_result.

        The attempt is serialized by ``_collect_lock`` so no two collections
        ever overlap, whether from the loop or an external ``run_once()``.
        A failing collection is reported, never raised.
        """
        attempt_ts = self._time()
        snapshot = None
        error = None
        try:
            with self._collect_lock:
                begin = self._time()
                try:
                    snapshot = self._nc.collect()
                finally:
                    duration = self._time() - begin
        except Exception as exc:  # a failed sample must never kill the sampler
            error = "%s: %s" % (exc.__class__.__name__, str(exc)[:200])
        elapsed = self._time() - attempt_ts
        self._on_result(snapshot, error, attempt_ts, elapsed)
        return elapsed

    def run_once(self):
        """Blocking single collection (serialized with the loop)."""
        self._do_collection()

    def _loop(self):
        while not self._stop.is_set():
            elapsed = self._do_collection()
            if self._stop.is_set():
                break
            # Sleep only the remaining budget so a slow collection delays the
            # next sample instead of overlapping samples (period = max(interval,
            # collection duration)).
            self._stop.wait(max(0.0, self._interval - elapsed))



# Request handling + JSON transport (methods on CollectorServer).
class _HttpMixin:
    def _peer_allowed(self, h):
        try:
            peer = h.client_address[0]
        except (AttributeError, IndexError, TypeError):
            return False
        return peer in self._peers

    def _token_allowed(self, h):
        auth = h.headers.get("Authorization") if h.headers is not None else None
        if not isinstance(auth, str) or len(auth) > _MAX_AUTH_HEADER:
            return False
        parts = auth.split(" ", 1)
        if len(parts) != 2 or parts[0].lower() != "bearer" or not parts[1]:
            return False
        return hmac.compare_digest(parts[1].encode("utf-8", "replace"),
                                   self._token.encode("utf-8", "replace"))

    def _send_json(self, h, code, obj):
        try:
            body = json.dumps(obj, separators=(",", ":"),
                              allow_nan=False).encode("utf-8")
        except (TypeError, ValueError):
            return self._raw(h, 500, b'{"error":"internal error"}')
        if len(body) > _MAX_RESPONSE_BYTES:
            return self._raw(h, 500, b'{"error":"response too large"}')
        return self._raw(h, code, body)

    def _raw(self, h, code, body):
        try:
            h.send_response(code)
            h.send_header("Content-Type", "application/json")
            h.send_header("Content-Length", str(len(body)))
            h.send_header("Cache-Control", "no-store")
            h.end_headers()
            h.wfile.write(body)
        except OSError:
            h.close_connection = True

    def _handle_get(self, h):
        path = (h.path or "").split("?", 1)[0]
        if path not in ("/api/telemetry", "/healthz"):
            return self._send_json(h, 404, {"error": "not found"})
        if not self._peer_allowed(h):
            return self._send_json(h, 403, {"error": "forbidden"})
        if path == "/api/telemetry" and not self._token_allowed(h):
            return self._send_json(h, 401, {"error": "unauthorized"})
        if path == "/api/telemetry":
            return self._send_json(h, 200, self.telemetry_payload())
        return self._send_json(h, 200, self.health_payload())

class CollectorServer(_HttpMixin):
    """Bounded, authenticated, read-only node telemetry server.

    Parameters
    ----------
    config:
        A validated config dict as produced by :func:`load_collector_config`
        (or an equivalent dict).  A file path string is accepted for
        convenience and validated the same way.
    collector:
        An object exposing ``collect()`` and a ``node_id`` attribute.  When
        omitted, a real :class:`~collectors.node_collector.NodeCollector` is
        constructed for ``config["node_id"]``.  Inject a fake in tests.
    clock:
        A zero-argument callable returning the current epoch time.  Defaults
        to :func:`time.time`.  Injectable for deterministic staleness tests.
    """

    def __init__(self, config, collector=None, clock=None):
        if clock is None:
            clock = time.time
        if isinstance(config, (str, os.PathLike)):
            config = load_collector_config(str(config))
        self.config = _validate_config(config)
        self._token = read_token_file(self.config["token_file"])
        self._peers = frozenset(self.config["allowed_peers"])
        self._clock = clock
        if collector is None:
            from .node_collector import NodeCollector
            collector = NodeCollector(node_id=self.config["node_id"])
        self._collector = collector
        # Snapshot cache state (guarded by self._cache_lock).
        self._cache_lock = threading.Lock()
        self._sample = None
        self._success_at = None
        self._attempt_at = None
        self._last_duration = None
        self._current_error = None
        self._attempts = 0
        self._failures = 0
        self._sampler = _Sampler(collector, self.config["interval_seconds"],
                                 self._clock, self._record)
        self._httpd = None
        self._http_thread = None

    # -- sampling state -----------------------------------------------------
    def _record(self, snapshot, error, attempt_ts, duration):
        with self._cache_lock:
            self._attempt_at = attempt_ts
            self._last_duration = duration
            self._attempts += 1
            if error is not None:
                self._failures += 1
                self._current_error = error
            else:
                self._sample = snapshot
                self._success_at = self._clock()
                self._current_error = None

    def run_once(self):
        """Blocking single collection, serialized with the background loop."""
        self._sampler.run_once()

    # -- read-only views over the cache (never trigger collection) ----------
    def telemetry_payload(self):
        """Build the /api/telemetry response body from cached state.

        Returns the wrapper dict described in the task.  The node snapshot is
        preserved verbatim; transport metadata is added around it.
        """
        stale_threshold = self.config["stale_threshold_seconds"]
        with self._cache_lock:
            sample = self._sample
            success_at = self._success_at
            error = self._current_error
        now = self._clock()
        age = None if success_at is None else now - success_at
        stale = age is None or age > stale_threshold
        return {
            "node": sample,
            "server_time": now,
            "age_seconds": age,
            "stale": stale,
            "collector_error": error,
        }

    def health_payload(self):
        """Build the /healthz response body from cached state."""
        stale_threshold = self.config["stale_threshold_seconds"]
        with self._cache_lock:
            sample = self._sample
            success_at = self._success_at
            attempt_at = self._attempt_at
            last_duration = self._last_duration
            error = self._current_error
            attempts = self._attempts
            failures = self._failures
        now = self._clock()
        age = None if success_at is None else now - success_at
        stale = age is None or age > stale_threshold
        if sample is None:
            sample_state = "missing"
        elif stale:
            sample_state = "stale"
        else:
            sample_state = "fresh"
        node_status = sample.get("status") if isinstance(sample, dict) else None
        return {
            "service": "up",
            "sampling": "active",
            "sample": sample_state,
            "node_status": node_status,
            "sample_age_seconds": age,
            "stale": stale,
            "last_attempt_at": attempt_at,
            "last_success_at": success_at,
            "last_duration_seconds": last_duration,
            "interval_seconds": self.config["interval_seconds"],
            "stale_threshold_seconds": stale_threshold,
            "collector_error": error,
            "samples_total": attempts,
            "failures_total": failures,
        }

    # -- lifecycle ----------------------------------------------------------
    @property
    def collector(self):
        """The single long-lived NodeCollector instance."""
        return self._collector

    def start(self):
        """Start the sampler thread and the HTTP server, return the bound port.

        The port is the actually-bound port (useful when configured to ``0``
        for an ephemeral loopback binding in tests or scratch runs).
        """
        self.start_sampler()
        return self.start_http()

    def start_sampler(self):
        """Start the background sampling loop only."""
        self._sampler.start()
        return self

    def start_http(self, port=None):
        """Start the HTTP listener only (no sampling), return the bound port."""
        if port is None:
            port = self.config["port"]
        return self._start_http(port)

    def bound_port(self):
        """The actually-bound HTTP port after :meth:`start`, else ``None``."""
        if self._httpd is None:
            return None
        return self._httpd.server_address[1]

    def _start_http(self, port):
        if self._httpd is not None:
            raise RuntimeError("HTTP server already started")
        handler = _make_handler(self)
        httpd = ThreadingHTTPServer((self.config["bind"], int(port)), handler)
        httpd.daemon_threads = True
        self._httpd = httpd
        self._http_thread = threading.Thread(
            target=httpd.serve_forever, kwargs={"poll_interval": 0.2},
            name="nc-http", daemon=True)
        self._http_thread.start()
        return httpd.server_address[1]

    def stop(self, timeout=10.0):
        """Bounded shutdown: stop the sampler, then the HTTP listener.

        The sampler thread may be mid-collection; ``timeout`` (seconds) is a
        hard ceiling on how long we wait for it to finish.  The thread is a
        daemon, so it can never hang the process past this deadline.
        """
        self._sampler.stop(timeout)
        if self._httpd is not None:
            self._httpd.shutdown()
            self._httpd.server_close()
            self._httpd = None
        if self._http_thread is not None:
            self._http_thread.join(timeout)
            self._http_thread = None




def _make_handler(svc):
    """Build the request-handler class bound to one CollectorServer."""

    class _Handler(BaseHTTPRequestHandler):
        server_version = "SysLumeNodeCollector/1"
        protocol_version = "HTTP/1.1"
        timeout = 15  # idle connections close; bounded request handling

        def log_message(self, format, *args):  # no logs, no header leakage
            pass

        def do_GET(self):
            svc._handle_get(self)

        def _not_allowed(self):
            svc._send_json(self, 405, {"error": "method not allowed",
                                       "allowed": ["GET"]})

        do_POST = _not_allowed
        do_PUT = _not_allowed
        do_DELETE = _not_allowed
        do_PATCH = _not_allowed
        do_HEAD = _not_allowed
        do_OPTIONS = _not_allowed

    return _Handler
