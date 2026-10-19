"""Cluster application: one central runtime + one HTTP API + static UI.

``build_app(config)`` wires the *already implemented* pieces together and
nothing else:

* one :class:`cluster.aggregator.ClusterAggregator` (three node collectors +
  ``cluster-model`` controller),
* one :class:`cluster.history.ClusterHistoryStore` (single SQLite writer),
* one :class:`cluster.runtime.ClusterRuntime` (the ONLY 15-second sampling
  loop; it collects, records and publishes -- HTTP handlers never collect),
* one :class:`cluster.http_api.ClusterHttpApi` (read-only JSON API + the
  explicit static dashboard routes).

Run as ``python3 -m cluster.app --config central.json`` (systemd user unit)
or ``build_app`` from Python (tests).

Config JSON (paths relative to the config file's directory; ``~`` expanded):

.. code-block:: json

    {
      "bind": "127.0.0.1",                  "port": 8765,
      "database": "data/monitor.db",        "retention_days": 30.0,
      "interval_seconds": 15.0,
      "static_dir": "static",               "allowed_hostnames": ["localhost"],
      "nodes": { "dgx01": {"url": "http://127.0.0.1:8165/api/telemetry",
                           "token_file": "tokens/dgx01"}, ... },
      "controller": {"executable": "~/bin/cluster-model"}
    }

``nodes`` / ``controller`` are optional (defaults = live cluster).  Secrets
live only in the 0600 ``token_file`` files -- never in this config's values.
"""

from __future__ import annotations

import json
import os
import signal
import sys
import threading
from pathlib import Path

from cluster.aggregator import (
    ClusterAggregator,
    ClusterModelController,
    ControllerError,
)
from cluster.history import ClusterHistoryStore
from cluster.http_api import ClusterHttpApi
from cluster.runtime import ClusterRuntime

__all__ = ["ClusterApp", "build_app", "load_config", "main"]


class ConfigError(ValueError):
    """Invalid central configuration."""


def _resolve(base_dir, value):
    path = Path(os.path.expanduser(str(value)))
    if not path.is_absolute():
        path = base_dir / path
    return path


def load_config(path):
    """Load + validate the central config file.

    Returns a plain dict with absolute paths for ``database`` and
    ``static_dir`` (resolved relative to the config file's directory).
    Secrets are never values in this config -- only paths to 0600 files.
    """
    path = Path(os.path.expanduser(str(path)))
    try:
        with open(path, "r", encoding="utf-8") as fh:
            cfg = json.load(fh)
    except (OSError, TypeError, ValueError) as exc:
        raise ConfigError("cannot read config %r: %s"
                          % (path, type(exc).__name__)) from None
    if not isinstance(cfg, dict):
        raise ConfigError("config must be a JSON object")
    bind = cfg.get("bind", "127.0.0.1")
    if bind not in ("127.0.0.1", "::1"):
        raise ConfigError("bind must be a loopback address")
    port = cfg.get("port", 8765)
    if not isinstance(port, int) or isinstance(port, bool) \
            or not (1 <= port <= 65535):
        raise ConfigError("port must be an integer 1..65535")
    interval = cfg.get("interval_seconds", 15.0)
    if not isinstance(interval, (int, float)) or isinstance(interval, bool) \
            or not (1.0 <= float(interval) <= 600.0):
        raise ConfigError("interval_seconds must be a number in [1, 600]")
    retention = cfg.get("retention_days", 30.0)
    if not isinstance(retention, (int, float)) or isinstance(retention, bool) \
            or not (1.0 <= float(retention) <= 3650.0):
        raise ConfigError("retention_days must be a number in [1, 3650]")
    hostnames = cfg.get("allowed_hostnames")
    if hostnames is not None:
        if not isinstance(hostnames, (list, tuple)) \
                or not all(isinstance(h, str) and h for h in hostnames):
            raise ConfigError("allowed_hostnames must be a list of strings")
    static_dir = cfg.get("static_dir", "static")
    return {
        "bind": bind,
        "port": int(port),
        "database": str(
            _resolve(path.parent, cfg.get("database", "data/monitor.db"))),
        "retention_days": float(retention),
        "interval_seconds": float(interval),
        "static_dir": (None if static_dir in (None, "")
                       else str(_resolve(path.parent, static_dir))),
        "allowed_hostnames": list(hostnames) if hostnames else None,
        "nodes": cfg.get("nodes"),
        "controller": cfg.get("controller"),
    }


def _build_aggregator(cfg):
    """Aggregator from the central config (same rules as
    :func:`cluster.aggregator.load_aggregator_config`'s ``controller`` key).
    """
    controller = False  # absent -> disabled (config is authoritative)
    if cfg.get("controller") is not None:
        ccfg = cfg["controller"]
        if not isinstance(ccfg, dict):
            raise ConfigError("controller must be an object")
        # Pass only the keys the config provides; the controller's own
        # defaults (executable / arguments / timeout / max_bytes) fill the rest
        # so a minimal {"executable": ...} config is valid.
        kwargs = {}
        for opt in ("executable", "arguments", "timeout_seconds", "max_bytes"):
            if opt in ccfg:
                kwargs[opt] = ccfg[opt]
        controller = ClusterModelController(**kwargs)
    return ClusterAggregator(nodes=cfg.get("nodes"), controller=controller)


class ClusterApp:
    """Owns the four long-lived pieces; start() / stop() only."""

    def __init__(self, cfg, runtime=None, api=None, store=None):
        self.cfg = cfg
        self.store = store if store is not None else ClusterHistoryStore(
            cfg["database"], retention_days=cfg["retention_days"],
            sample_interval_seconds=cfg["interval_seconds"])
        self.aggregator = _build_aggregator(cfg)
        self.runtime = runtime if runtime is not None else ClusterRuntime(
            interval_seconds=cfg["interval_seconds"], store=self.store,
            aggregator=self.aggregator)
        self.api = api if api is not None else ClusterHttpApi(
            self.runtime, store=self.store, bind_host=cfg["bind"],
            port=cfg["port"],
            allowed_hostnames=cfg.get("allowed_hostnames"),
            static_root=cfg.get("static_dir"))
        self.port = None

    def start(self):
        self.port = self.api.start()
        self.runtime.start()
        return self.port

    def stop(self, timeout=None):
        stopped_api = False
        try:
            self.runtime.stop(timeout=timeout)
        finally:
            self.api.stop()
        return stopped_api


def build_app(config):
    """Build (do not start) the app from a config dict (see :func:`load_config`).

    Accepts either a validated dict or a path to the config file.
    """
    if isinstance(config, (str, Path)):
        config = load_config(config)
    return ClusterApp(config)


def main(argv=None):
    """Run the app under systemd (or a shell) until signaled."""
    import argparse

    parser = argparse.ArgumentParser(prog="syslume-cluster")
    parser.add_argument("--config", required=True, help="central config JSON")
    args = parser.parse_args(argv)
    try:
        app = build_app(args.config)
        port = app.start()
    except (ConfigError, ValueError, OSError) as exc:
        print("syslume-cluster: cannot start: %s" % exc, file=sys.stderr)
        return 1
    print("syslume-cluster: listening on http://%s:%d "
          "(interval %.0fs, database %s)"
          % (app.cfg["bind"], port, app.cfg["interval_seconds"],
             app.cfg["database"]), flush=True)
    stop = threading.Event()

    def _signal(signum, frame):
        stop.set()

    signal.signal(signal.SIGTERM, _signal)
    signal.signal(signal.SIGINT, _signal)
    while not stop.is_set():
        stop.wait(0.5)
    app.stop()
    print("syslume-cluster: stopped", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
