"""SysLume deployment config generator (pure, no side effects).

Emits the exact JSON config files the central app and the per-node collectors
read.  Values are validated at load time by the apps themselves
(:func:`cluster.app.load_config`,
:func:`collectors.collector_server.load_collector_config`); this module only
produces the file contents from a node identity, home directory and the
Bearer tokens so the *same* generation logic is used on every machine.

The three Bearer tokens are generated once (see ``secrets.token_hex``) and must
match between a node's collector and the central app's node source for that
node.  They are written to 0600 files; nothing here embeds a secret in the
config values.
"""

from __future__ import annotations

import json

#: Per-node collector bind address (matches collectors.collector_server.BIND_BY_NODE).
BIND = {"dgx01": "127.0.0.1", "dgx02": "192.168.0.2", "dgx03": "192.168.2.2"}
#: The DGX01-facing peer that may reach each collector
#: (matches collectors.collector_server.PEER_BY_NODE).
PEER = {"dgx01": "127.0.0.1", "dgx02": "192.168.0.1", "dgx03": "192.168.2.1"}
#: Telemetry URL the central aggregator polls for each node.
URL = {"dgx01": "http://127.0.0.1:8165/api/telemetry",
       "dgx02": "http://192.168.0.2:8165/api/telemetry",
       "dgx03": "http://192.168.2.2:8165/api/telemetry"}

NODE_IDS = ("dgx01", "dgx02", "dgx03")
COLLECTOR_PORT = 8165
APP_PORT = 8765
ALLOWED_HOSTNAMES = [
    "127.0.0.1", "::1", "localhost", "cygno-dgx01",
    "cygno-dgx01.taildccbbd.ts.net",  # Tailscale Serve (additive, tailnet only)
]


def paths(home):
    """Standard per-node on-disk locations under ``home``."""
    h = home.rstrip("/")
    return {
        "collector_json": "%s/.config/syslume/collector.json" % h,
        "app_json": "%s/.config/syslume/app.json" % h,
        "db": "%s/.local/state/syslume/monitor.db" % h,
        "log": "%s/.local/state/syslume" % h,
        "state": "%s/.local/state/syslume" % h,
        "token": lambda nid: "%s/.config/syslume/tokens/%s" % (h, nid),
        "static_dir": "%s/pi-agent/syslume-cluster-repo/static" % h,
        "repo": "%s/pi-agent/syslume-cluster-repo" % h,
        "controller": "%s/bin/cluster-model" % h,
    }


def collector_config(node_id, home):
    """JSON text for one node's collector config file."""
    p = paths(home)
    cfg = {
        "node_id": node_id,
        "bind": BIND[node_id],
        "port": COLLECTOR_PORT,
        "interval_seconds": 15.0,
        "stale_threshold_seconds": 60.0,
        "allowed_peers": [PEER[node_id]],
        "token_file": p["token"](node_id),
    }
    return json.dumps(cfg, indent=2) + "\n"


def app_config(home):
    """JSON text for the central app config (DGX01 only)."""
    p = paths(home)
    nodes = {}
    for nid in NODE_IDS:
        nodes[nid] = {"url": URL[nid], "token_file": p["token"](nid)}
    cfg = {
        "bind": "127.0.0.1",
        "port": APP_PORT,
        "database": p["db"],
        "retention_days": 30.0,
        "interval_seconds": 15.0,
        "static_dir": p["static_dir"],
        "allowed_hostnames": list(ALLOWED_HOSTNAMES),
        "nodes": nodes,
        "controller": {"executable": p["controller"]},
    }
    return json.dumps(cfg, indent=2) + "\n"


def token_files_for(node_id, home, tokens):
    """Map of node_id -> absolute token-file path this machine must provision.

    ``tokens`` is a mapping node_id -> bearer token.  A node's collector only
    needs its own token; the central app (dgx01) needs all three.
    """
    p = paths(home)
    if node_id == "dgx01":
        wanted = list(NODE_IDS)
    else:
        wanted = [node_id]
    return {nid: (p["token"](nid), tokens[nid]) for nid in wanted
            if nid in tokens}


if __name__ == "__main__":
    import sys

    nid, home = sys.argv[1], sys.argv[2]
    if nid == "dgx01":
        print(app_config(home))
    print(collector_config(nid, home))
