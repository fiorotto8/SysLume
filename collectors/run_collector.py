"""Per-node telemetry collector entry point for SysLume Cluster.

Runs one :class:`~collectors.collector_server.CollectorServer` for a single
node until signaled.  Launched by a systemd *user* unit on every DGX (DGX01
runs the central app *and* its own collector; DGX02/DGX03 run only this).

Run as ``python3 -m collectors.run_collector --config collector.json``.

The config file is the validated collector config (see
:func:`collectors.collector_server.load_collector_config`):

.. code-block:: json

    {
      "node_id": "dgx02",
      "bind": "192.168.0.2",
      "port": 8165,
      "interval_seconds": 15.0,
      "stale_threshold_seconds": 60.0,
      "allowed_peers": ["192.168.0.1"],
      "token_file": "~/.config/syslume/tokens/dgx02"
    }

The Bearer token lives only in the 0600 ``token_file`` (never in this config
or on the command line).  ``--check`` validates config + token without
binding, for deployment verification.
"""

from __future__ import annotations

import argparse
import os
import signal
import sys
import threading

from collectors.collector_server import CollectorServer, CollectorServerConfigError


def _signal_args():
    parser = argparse.ArgumentParser(
        prog="syslume-node-collector",
        description="Run one SysLume per-node telemetry collector.")
    parser.add_argument("--config", required=True,
                        help="collector config JSON file")
    parser.add_argument("--check", action="store_true",
                        help="validate config + token and exit (no binding)")
    return parser


def main(argv=None):
    args = _signal_args().parse_args(argv)
    config_path = os.path.expanduser(args.config)

    try:
        svc = CollectorServer(config_path)
    except (CollectorServerConfigError, OSError, ValueError) as exc:
        print("syslume-node-collector: cannot start: %s" % exc,
              file=sys.stderr)
        return 1

    cfg = svc.config
    if args.check:
        print("syslume-node-collector: config ok: node=%s bind=%s:%d "
              "peers=%s interval=%.1fs token=ok"
              % (cfg["node_id"], cfg["bind"], cfg["port"],
                 ",".join(cfg["allowed_peers"]),
                 cfg["interval_seconds"]), flush=True)
        svc.stop()
        return 0

    try:
        port = svc.start()
    except OSError as exc:
        print("syslume-node-collector: cannot start: %s" % exc,
              file=sys.stderr)
        return 1

    print("syslume-node-collector: node %s serving on %s:%d "
          "(interval %.0fs, stale after %.0fs)"
          % (cfg["node_id"], cfg["bind"], port,
             cfg["interval_seconds"],
             cfg["stale_threshold_seconds"]), flush=True)

    stop = threading.Event()

    def _handler(signum, frame):
        stop.set()

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)
    while not stop.is_set():
        stop.wait(0.5)

    svc.stop()
    print("syslume-node-collector: stopped", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
