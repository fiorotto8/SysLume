"""Central read-only HTTP API for the SysLume DGX cluster (Phase 19B.1).

A thin, loopback-only HTTP server that exposes the *already-collected* cluster
state for the dashboard.  It composes three existing, trusted read surfaces and
re-implements none of their logic:

* :class:`cluster.runtime.ClusterRuntime`
      -- :meth:`~ClusterRuntime.latest_snapshot` / :meth:`~ClusterRuntime.latest`
         (the last *successfully collected* snapshot + publish time) and
         :meth:`~ClusterRuntime.status` (bounded runtime summary).
* :class:`cluster.history.ClusterHistoryStore`
      -- :meth:`~ClusterHistoryStore.node_history` /
         :meth:`~ClusterHistoryStore.service_history` /
         :meth:`~ClusterHistoryStore.cluster_history` (bounded, bucketed,
         min/mean/max series with gaps preserved) and
         :meth:`~ClusterHistoryStore.status`.

Design guarantees (Phase 19B.1):

* **Read-only, no second sampling loop.**  Handlers only call the runtime's
  cached views and the history store's query methods.  They never call
  ``ClusterAggregator.collect()``, ``ClusterRuntime._run_cycle()`` or the
  ``cluster-model`` controller, and they never write to the database.
* **Loopback-only.**  Binds to ``127.0.0.1``; ``Host`` and ``Origin`` headers
  are validated against an allowlist (defaults to loopback names).  No
  permissive CORS is enabled -- no ``Access-Control-Allow-Origin`` is ever
  sent.  A future Tailscale Serve hostname can be added to the allowlist
  without code changes; Serve itself is not configured here.
* **Bounded and honest.**  Query ranges and result sizes are bounded by the
  history store.  Fresh vs stale vs never-seen samples are carried verbatim in
  the snapshot / history.  No NaN/Infinity escapes into JSON.  Errors are
  structured JSON (never an HTML traceback).
* **Concurrent and safe.**  Uses :class:`http.server.ThreadingHTTPServer` (one
  thread per connection); the store's single-writer lock and short-lived
  read-only connections already make concurrent reads safe.
* **No credentials / tokens / environment variables in responses.**

Routes
------
* ``GET /healthz``
* ``GET /api/cluster/current``
* ``GET /api/cluster/status``
* ``GET /api/history/node``
* ``GET /api/history/service``
* ``GET /api/history/cluster``

Any other method on a known route is rejected with ``405``; unknown paths get
``404``.  All JSON responses are ``application/json`` with ``Cache-Control:
no-store``.

Optional static frontend (``static_root=<directory>``)
-----------------------------------------------------
When constructed with a ``static_root`` directory, two *explicit* additional
GET routes are served from it (JSON API routes always win):

* ``GET /``          -> ``<static_root>/cluster.html``
* ``GET /static/x``  -> ``<static_root>/x`` where ``x`` is a single (non-hidden)
  file name with an allowed extension (html/css/js/svg/png/json).

No sub-directories, no path traversal, no directory listing; anything else
still gets the JSON ``404``.  Static files are served with ``Cache-Control:
no-store`` so a restart can never leave a stale dashboard.
"""

from __future__ import annotations

import json
import math
import sqlite3
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from cluster.history import (
    HistoryError,
    NODE_IDS,
    NODE_METRICS,
    SERVICE_METRICS,
)

#: Content types for the explicit static-file routes (opt-in via ``static_root``).
_STATIC_CONTENT_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
    ".svg": "image/svg+xml",
    ".png": "image/png",
    ".json": "application/json; charset=utf-8",
}

#: API version advertised in responses (independent of the aggregator schema).
API_VERSION = 1
#: Default loopback bind host (never a public / Tailscale interface).
DEFAULT_BIND_HOST = "127.0.0.1"
#: Default allowlist of Host/Origin hostnames (loopback).  A future Tailscale
#: Serve hostname can be added here by the operator; Serve is not configured.
DEFAULT_ALLOW_HOSTNAMES = frozenset({"127.0.0.1", "::1", "localhost"})
#: Central-snapshot freshness window expressed as a multiple of the runtime
#: interval (default 3x -> ~45 s at the standard 15 s cadence).
FRESH_MULTIPLE = 3.0
#: Upper bound on ``points`` we accept at the HTTP layer (the history store
#: itself clamps to 5000; this keeps a huge parse from exercising odd paths).
MAX_POINTS = 5000


class ApiError(Exception):
    """A structured, HTTP-level error to be rendered as JSON.

    ``code`` is a short stable identifier (e.g. ``"unknown_metric"``),
    ``status`` the HTTP status code, ``message`` a bounded human string.
    Optional ``details`` is an extra JSON-safe object.
    """

    def __init__(self, status, code, message, details=None):
        super().__init__(message)
        self.status = int(status)
        self.code = code
        self.message = message
        self.details = details

    def to_body(self):
        body = {"status": "error", "code": self.code, "message": self.message}
        if self.details is not None:
            body["details"] = self.details
        return body


# --- request helpers -------------------------------------------------------

def _host_of(value):
    """Lowercase hostname from a ``Host`` / origin value, port/brackets stripped."""
    if not isinstance(value, str):
        return None
    v = value.strip().lower()
    if not v:
        return None
    if v.startswith("["):  # IPv6 [::1]:port
        end = v.find("]")
        if end != -1:
            v = v[1:end]
        else:
            v = v[1:]
    else:
        v = v.split(":", 1)[0]
    return v or None


def _parse_float_param(query, name):
    """Parse an optional finite float query param; returns None or raises."""
    values = query.get(name)
    if not values:
        return None
    raw = values[-1].strip()
    try:
        value = float(raw)
    except (TypeError, ValueError):
        raise ApiError(400, "bad_parameter",
                       "parameter %r must be a number, got %r" % (name, raw))
    if not math.isfinite(value):
        raise ApiError(400, "bad_parameter",
                       "parameter %r must be finite" % name)
    return value


def _parse_points_param(query):
    values = query.get("points")
    if not values:
        return None
    raw = values[-1].strip()
    try:
        value = int(raw)
    except (TypeError, ValueError):
        raise ApiError(400, "bad_parameter",
                       "parameter 'points' must be an integer, got %r" % raw)
    if value < 1:
        raise ApiError(400, "bad_parameter",
                       "parameter 'points' must be >= 1")
    if value > MAX_POINTS:
        # Bounded result sizes: clamp instead of erroring so dashboards degrade
        # gracefully (the history store re-clamps to its own ceiling).
        value = MAX_POINTS
    return value


def _split_csv(value):
    """Split a comma-separated (and/or repeated) query value into items."""
    out = []
    for part in value.split(","):
        item = part.strip()
        if item:
            out.append(item)
    return out


def _check_allowlist(items, allowlist, label):
    """Return an ordered, de-duplicated *items* list confined to *allowlist*.

    Membership validation lives here (the HTTP layer) so the client gets a
    precise ``unknown_metric`` / ``unknown_node`` error; the history store
    re-validates the same names as defence-in-depth (no SQL injection).
    """
    out = []
    for item in items:
        if item not in allowlist:
            raise ApiError(400, "unknown_%s" % label.replace(" ", "_"),
                           "unknown %s %r" % (label, item),
                           details={"value": item})
        if item not in out:
            out.append(item)
    if not out:
        raise ApiError(400, "missing_parameter",
                       "%s selection must not be empty" % label)
    return out


def _pick_csv(query, name):
    """Gather all occurrences of *name* (repeated + comma-separated) as items."""
    items = []
    for value in query.get(name, []):
        items.extend(_split_csv(value))
    return items


def _db_uri(path):
    """Absolute ``file://`` URI for a SQLite path (or ``None`` if unusable)."""
    try:
        return Path(path).resolve().as_uri()
    except (TypeError, ValueError, OSError):
        return None


def _json_bytes(obj):
    """Serialize *obj* to UTF-8 JSON, force-replacing non-finite floats with
    ``None`` (never emit NaN/Infinity) without mutating the input."""
    return (
        json.dumps(_sanitize(obj), allow_nan=False, separators=(",", ":"),
                   sort_keys=False).encode("utf-8")
    )


def _sanitize(value):
    if isinstance(value, dict):
        return {k: _sanitize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_sanitize(v) for v in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, set) or isinstance(value, frozenset):
        return sorted(str(x) for x in value)
    return value


#: Fallback interval used only when a runtime omits ``interval_seconds``.
_FALLBACK_INTERVAL_SECONDS = 15.0

#: Non-allowlist (label) columns stored on the ``service_samples`` row.
_SERVICE_EXTRA_COLUMNS = (
    "vllm_status", "vllm_served_model", "gw_ts", "gw_age_seconds")


class ClusterHttpApi:
    """Loopback-only, read-only HTTP API over a :class:`ClusterRuntime`.

    Parameters
    ----------
    runtime:
        A :class:`cluster.runtime.ClusterRuntime` (or any object exposing
        :meth:`latest`, :meth:`status` and a ``store`` attribute).
    store:
        A :class:`cluster.history.ClusterHistoryStore` to query.  Defaults to
        ``runtime.store``.  Injecting a different store (e.g. a temporary
        SQLite database or a broken stub) makes the API trivially testable.
    bind_host / port:
        Loopback bind address and port (``0`` = ephemeral).  ``bind_host``
        must be a loopback address (anything else is refused at construction).
    allowed_hostnames:
        Allowlist of Host/Origin hostnames (default: loopback names).
    static_root:
        Optional directory of dashboard files.  When given (an existing
        directory) the explicit static routes ``/`` and ``/static/<file>``
        are served from it; ``None`` (default) keeps the API JSON-only.
    clock:
        Zero-argument callable returning epoch seconds (inject for tests).
    """

    def __init__(self, runtime, store=None, bind_host=DEFAULT_BIND_HOST,
                 port=0, allowed_hostnames=None, static_root=None, clock=None):
        if bind_host not in ("127.0.0.1", "::1"):
            raise ValueError("bind_host must be a loopback address")
        self._runtime = runtime
        resolved = store if store is not None else getattr(runtime, "store", None)
        if resolved is None:
            raise ValueError(
                "a history store is required (runtime.store or store=)")
        self._store = resolved
        self._static_root = None
        if static_root is not None:
            root = Path(static_root)
            if not root.is_dir():
                raise ValueError("static_root must be an existing directory")
            self._static_root = root.resolve()
        self._bind_host = bind_host
        self._port = int(port)
        self._allowed_hostnames = frozenset(
            allowed_hostnames if allowed_hostnames is not None
            else DEFAULT_ALLOW_HOSTNAMES)
        self._clock = clock if clock is not None else time.time
        self._httpd = None
        self._thread = None

    # -- dependency accessors (read at call time so tests can swap stores) --
    def _runtime_ref(self):
        return self._runtime

    def _store_ref(self):
        return self._store

    # -- read-only views (never collect, never write, never mutate) --------
    def health(self):
        """Liveness + running state + last cycle + errors + cache freshness."""
        now = self._clock()
        status = self._runtime_ref().status()
        latest = self._runtime_ref().latest()
        interval = status.get("interval_seconds")
        if not (isinstance(interval, (int, float))
                and not isinstance(interval, bool) and interval > 0):
            interval = _FALLBACK_INTERVAL_SECONDS
        window = FRESH_MULTIPLE * float(interval)
        has = latest is not None
        published_at = latest.get("published_at") if has else None
        age = (now - published_at) if (has and published_at is not None) else None
        fresh = bool(has and age is not None and age <= window)
        snapshot = (latest.get("snapshot") or {}) if has else {}
        return {
            "status": "ok",
            "api_version": API_VERSION,
            "service": "up",
            "time": now,
            "running": bool(status.get("running")),
            "interval_seconds": interval,
            "cycles": status.get("cycles"),
            "last_cycle": None if not has else {
                "cycle": latest.get("cycle"),
                "published_at": published_at,
                "snapshot_timestamp": snapshot.get("timestamp"),
                "age_seconds": age,
            },
            "cache": {
                "has_snapshot": has,
                "fresh": fresh,
                "fresh_window_seconds": window,
                "age_seconds": age,
            },
            "collection_errors": status.get("last_errors"),
            "history": status.get("history"),
        }

    def current(self):
        """Latest cached snapshot (controller + nodes) + latest service sample."""
        now = self._clock()
        latest = self._runtime_ref().latest()
        if latest is None:
            return {
                "status": "ok",
                "api_version": API_VERSION,
                "available": False,
                "reason": "no_sample_yet",
                "snapshot": None,
                "service": None,
                "published_at": None,
                "cycle": None,
                "snapshot_timestamp": None,
                "snapshot_age_seconds": None,
                "time": now,
            }
        snapshot = latest.get("snapshot")
        published_at = latest.get("published_at")
        age = (now - published_at) if published_at is not None else None
        service, service_error = self._latest_service_sample()
        result = {
            "status": "ok",
            "api_version": API_VERSION,
            "available": True,
            "reason": None,
            "snapshot": snapshot,
            "service": service,
            "published_at": published_at,
            "cycle": latest.get("cycle"),
            "snapshot_timestamp": (snapshot or {}).get("timestamp"),
            "snapshot_age_seconds": age,
            "time": now,
        }
        if service_error is not None:
            result["service_error"] = service_error
        return result

    def status(self):
        """Bounded runtime summary (``ClusterRuntime.status()``), sanitized."""
        result = {"status": "ok", "api_version": API_VERSION,
                  "time": self._clock()}
        result.update(self._runtime_ref().status())
        return result

    # -- history views (bounded; the store does the real work) -------------
    def node_history(self, query):
        start = _parse_float_param(query, "start")
        end = _parse_float_param(query, "end")
        points = _parse_points_param(query)
        metric_names = _pick_csv(query, "metrics") or _pick_csv(query, "metric")
        if not metric_names:
            raise ApiError(400, "missing_parameter",
                           "node history requires a 'metrics' parameter")
        names = _check_allowlist(metric_names, NODE_METRICS, "node metric")
        nodes_param = _pick_csv(query, "nodes") or _pick_csv(query, "node")
        node_ids = (_check_allowlist(nodes_param, NODE_IDS, "node")
                    if nodes_param else None)
        return self._store_ref().node_history(
            names, start=start, end=end,
            points=points if points is not None else 300, nodes=node_ids)

    def service_history(self, query):
        start = _parse_float_param(query, "start")
        end = _parse_float_param(query, "end")
        points = _parse_points_param(query)
        metric_names = _pick_csv(query, "metrics") or _pick_csv(query, "metric")
        if not metric_names:
            raise ApiError(400, "missing_parameter",
                           "service history requires a 'metrics' parameter")
        names = _check_allowlist(metric_names, SERVICE_METRICS, "service metric")
        return self._store_ref().service_history(
            names, start=start, end=end,
            points=points if points is not None else 300)

    def cluster_history(self, query):
        start = _parse_float_param(query, "start")
        end = _parse_float_param(query, "end")
        points = _parse_points_param(query)
        return self._store_ref().cluster_history(
            start=start, end=end,
            points=points if points is not None else 300)

    # -- internal ----------------------------------------------------------
    def _latest_service_sample(self):
        """Most recent ``service_samples`` row, straight from the history DB.

        Returns ``(row_dict_or_None, error_or_None)``.  A DB failure yields a
        benign ``None`` + error string so the cached snapshot still serves.
        """
        store = self._store_ref()
        path = getattr(store, "path", None)
        uri = _db_uri(path) if path is not None else None
        if uri is None:
            return None, "no history database"
        columns = list(_SERVICE_EXTRA_COLUMNS) + list(SERVICE_METRICS)
        try:
            conn = sqlite3.connect(uri + "?mode=ro", uri=True, timeout=10)
            conn.row_factory = sqlite3.Row
            try:
                row = conn.execute(
                    "SELECT ts, %s FROM service_samples "
                    "ORDER BY ts DESC LIMIT 1" % ", ".join(
                        '"%s"' % c for c in columns)).fetchone()
            finally:
                conn.close()
        except sqlite3.Error as exc:
            return None, "history database unavailable: %s" % type(exc).__name__
        except Exception as exc:
            return None, "unreadable history database: %s" % type(exc).__name__
        if row is None:
            return None, None
        out = {"ts": row[0]}
        for i, col in enumerate(columns, start=1):
            out[col] = row[i]
        return out, None

    # -- server lifecycle --------------------------------------------------
    def start(self):
        """Bind the loopback listener and serve in a daemon thread.

        Returns the actually-bound port (useful with ``port=0``).
        """
        if self._httpd is not None:
            raise RuntimeError("HTTP API already started")
        handler = _make_handler(self)
        self._httpd = ThreadingHTTPServer((self._bind_host, self._port), handler)
        self._httpd.daemon_threads = True
        self._thread = threading.Thread(
            target=self._httpd.serve_forever, kwargs={"poll_interval": 0.2},
            name="syslume-cluster-api", daemon=True)
        self._thread.start()
        return self.bound_port()

    def bound_port(self):
        return None if self._httpd is None else self._httpd.server_address[1]

    def stop(self, timeout=10.0):
        """Bounded shutdown (daemon thread cannot hold the process hostage)."""
        if self._httpd is not None:
            self._httpd.shutdown()
            self._httpd.server_close()
            self._httpd = None
        if self._thread is not None:
            self._thread.join(timeout)
            self._thread = None


def _make_handler(api):
    """Build the request-handler class bound to one :class:`ClusterHttpApi`."""

    class _Handler(BaseHTTPRequestHandler):
        server_version = "SysLumeClusterApi/1"
        protocol_version = "HTTP/1.1"
        timeout = 15  # idle connections close; bounded request handling

        def log_message(self, *args):  # no logs, no header leakage
            pass

        # -- route table (read-only views only) ----------------------------
        def _routes(self):
            return {
                "/healthz": lambda q: api.health(),
                "/api/cluster/current": lambda q: api.current(),
                "/api/cluster/status": lambda q: api.status(),
                "/api/history/node": lambda q: api.node_history(q),
                "/api/history/service": lambda q: api.service_history(q),
                "/api/history/cluster": lambda q: api.cluster_history(q),
            }

        def do_GET(self):
            self._dispatch(send_body=True)

        def do_HEAD(self):
            self._dispatch(send_body=False)

        def _dispatch(self, send_body):
            parsed = urlparse(self.path)
            path = parsed.path
            if len(path) > 1 and path.endswith("/"):
                path = path.rstrip("/")
            query = parse_qs(parsed.query)
            rejected = self._validate_origin()
            if rejected is not None:
                self._send_json(rejected[0], rejected[1], send_body=send_body)
                return
            static_reply = self._static_route(path)
            if static_reply is not None:
                self._send_bytes(static_reply[0], static_reply[1],
                                 static_reply[2], send_body=send_body)
                return
            route = self._routes().get(path)
            if route is None:
                self._send_json(
                    404, {"status": "error", "code": "not_found",
                          "message": "unknown path %r" % path},
                    send_body=send_body)
                return
            try:
                body = route(query)
                self._send_json(200, body, send_body=send_body)
            except ApiError as exc:
                self._send_json(exc.status, exc.to_body(), send_body=send_body)
            except HistoryError as exc:
                self._send_json(400, {"status": "error", "code": "invalid_range",
                                       "message": str(exc)}, send_body=send_body)
            except sqlite3.Error as exc:
                self._send_json(
                    503, {"status": "error", "code": "history_unavailable",
                          "message": "history store temporarily unavailable: %s"
                                     % type(exc).__name__},
                    send_body=send_body)
            except Exception:  # noqa: BLE001 - never leak an HTML traceback
                self._send_json(500, {"status": "error",
                                       "code": "internal_error",
                                       "message": "internal error"},
                                send_body=send_body)

        # -- Host / Origin validation (loopback-only; no permissive CORS) --
        def _validate_origin(self):
            host = _host_of(self.headers.get("Host"))
            if host is None:
                return (400, {"status": "error", "code": "bad_request",
                              "message": "missing or invalid Host header"})
            if host not in api._allowed_hostnames:
                return (403, {"status": "error", "code": "host_not_allowed",
                              "message": "request Host is not allowed"})
            origin = self.headers.get("Origin")
            if origin is not None:
                ohost = _host_of(origin)
                if not ohost or ohost not in api._allowed_hostnames:
                    return (403, {"status": "error",
                                  "code": "origin_not_allowed",
                                  "message": "request Origin is not allowed"})
            return None

        def _static_route(self, path):
            """Explicit static routes only.  Returns ``(code, ctype, bytes)``
            or ``None`` (no static match; the JSON 404 below applies).

            ``GET /`` maps to ``cluster.html``; ``GET /static/<name>`` maps to
            a single non-hidden file directly in the root with an allowed
            extension.  Sub-paths and traversal simply miss.
            """
            root = api._static_root
            if root is None:
                return None
            try:
                if path == "/":
                    target = root / "cluster.html"
                elif path.startswith("/static/") and len(path) > len("/static/"):
                    name = path[len("/static/"):]
                    if ("/" in name or name.startswith(".") or ".." in name):
                        return None
                    target = root / name
                else:
                    return None
                resolved = target.resolve()
                if resolved.parent != root or not resolved.is_file():
                    return None  # missing file or escape attempt
                ctype = _STATIC_CONTENT_TYPES.get(resolved.suffix.lower())
                if ctype is None:
                    return None
                return (200, ctype, resolved.read_bytes())
            except OSError:
                return None

        def _send_bytes(self, code, ctype, data, send_body=True):
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            if send_body:
                self.wfile.write(data)

        def _send_json(self, code, obj, send_body=True):
            data = _json_bytes(obj)
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            if code == 405:
                self.send_header("Allow", "GET, HEAD")
            self.end_headers()
            if send_body:
                self.wfile.write(data)

        # -- reject any method other than GET / HEAD -----------------------
        def _method_not_allowed(self, send_body=True):
            data = _json_bytes({"status": "error", "code": "method_not_allowed",
                                "message": "method not allowed",
                                "allowed": ["GET", "HEAD"]})
            self.send_response(405)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("Allow", "GET, HEAD")
            self.end_headers()
            if send_body:
                self.wfile.write(data)

        do_POST = _method_not_allowed
        do_PUT = _method_not_allowed
        do_DELETE = _method_not_allowed
        do_PATCH = _method_not_allowed
        do_OPTIONS = _method_not_allowed

    return _Handler
