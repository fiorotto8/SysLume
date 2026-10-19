#!/usr/bin/env bash
# SysLume: expose the cluster dashboard at https://<host>/ (HTTPS 443) via
# Tailscale Serve, TAILNET ONLY (no Funnel), while preserving the existing LLM
# gateway at https://<host>:8443.
#
#   Desired final serve state:
#     https://cygno-dgx01.taildccbbd.ts.net/        -> http://127.0.0.1:8765  (new: dashboard)
#     https://cygno-dgx01.taildccbbd.ts.net:8443/   -> http://127.0.0.1:8101  (keep: LLM gateway)
#
# WHY A SCRIPT / RUN AS ROOT:
#   The serve config is a control-plane attribute. tailscaled only lets the
#   root UID (or a tailscale "operator") modify it, so this must run as root:
#       sudo bash deploy/tailscale-serve.sh
#
#   It uses the high-level `tailscale serve` subcommand (incremental by design:
#   it only ADDS the 443 -> 8765 mapping and does NOT touch the 8443 mapping).
#   It never calls `serve reset`/`serve clear`, so the existing LLM gateway is
#   not wiped.  It backs up the current config first, applies, and verifies.
set -euo pipefail

HOST="cygno-dgx01.taildccbbd.ts.net"
DASH="127.0.0.1:8765"   # cluster app: dashboard + history + API
GW="127.0.0.1:8101"     # existing LLM gateway (must keep working)

if [ "$(id -u)" -ne 0 ]; then
    echo "error: run as root (the serve config is root/operator-gated)." >&2
    echo "       -> sudo bash $0" >&2
    echo "       (or, to let this user manage serve without root later:" >&2
    echo "           sudo tailscale set --operator=\$(whoami))" >&2
    exit 2
fi

bak="$(mktemp)"
trap 'rm -f "$bak"' EXIT
tailscale serve status --json > "$bak"
echo "backed up current serve config -> (kept in $bak for this run)"

echo "applying: tailscale serve --bg $DASH  (HTTPS 443 -> $DASH)"
tailscale serve --bg "$DASH"

echo "verifying ..."
tailscale serve status --json | DASHD="$DASH" GWD="$GW" python3 -c "
import os, sys, json
d = json.load(sys.stdin)
web = d.get('Web', {})
def proxy_of(hostport):
    h = web.get(hostport, {})
    return (h.get('Handlers') or {}).get('/', {}).get('Proxy')
dash = os.environ['DASHD']; gw = os.environ['GWD']
proxies = [proxy_of(hp) for hp in web]
dash_ok = ('http://' + dash) in proxies
gw_ok   = ('http://' + gw) in proxies
print('  443  -> %s : %s' % (dash, 'OK' if dash_ok else 'MISSING'))
print('  8443 -> %s : %s' % (gw, 'OK (preserved)' if gw_ok else 'MISSING!'))
sys.exit(0 if (dash_ok and gw_ok) else 1)
" && {
    echo "SUCCESS. Dashboard: https://$HOST/   (LLM gateway unchanged at :8443)"
    exit 0
} || {
    echo "!! 443 added but the :8443 LLM gateway mapping was dropped." >&2
    echo "   Re-add it via the Tailscale admin console (this node -> Serve ->" >&2
    echo "   add port 8443 (HTTPS) -> proxy http://$GW), or restore from the" >&2
    echo "   backup captured at the top of this run." >&2
    exit 1
}
