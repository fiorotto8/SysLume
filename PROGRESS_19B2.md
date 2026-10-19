# SysLume Cluster — completion progress (Phase 19B.2+)

Branch: feature/dgx-cluster-monitoring · Repo: ~/pi-agent/syslume-cluster-repo
Goal: permanent dashboard at https://cygno-dgx01.taildccbbd.ts.net/ (port 443 →
127.0.0.1:8765) covering DGX01/02/03; keep :8443/v1 (gateway) untouched.

Legend: [x] done  [~] in progress  [ ] todo  [!] needs operator privilege
RECOVERY NOTE (this session): previous session died mid-`cluster.js`. On disk we
actually had: cluster/http_api.py (NO static serving), runtime, aggregator,
history, service_metrics, collectors (no launcher), static/{cluster.html,
cluster.css,charts.js}, all cluster tests. Missing: cluster.js, cluster/app.py,
collectors/run_collector.py, units. Schemas were re-verified from source:
  snapshot: {schema_version,timestamp,cluster{mode,healthy,transitioning,
    controller:"ok"|"error",raw,error},nodes:{<id>:{state:online|stale|error|never,
    reachable,fresh,sample_fresh,sample_stale,quality,age_seconds,
    sample_age_seconds,error,sample:<node sample>,sample_timestamp,
    remote_reported_stale,mode_expected,as_expected}},warnings,meta}
  node sample: {schema_version,node_id,timestamp,status:ok|partial|error,
    components:{system:{timestamp,metrics,details{gpu_status,mem_unified_source,
    system,gpu}},ollama:{timestamp,status,error,version,installed_models,
    resident_models,models:{name:{installed,resident,size_bytes,size_vram_bytes,
    context_length_tokens,processor,expires_at,...}},metrics},processes:{...}}
    metrics:<flattened>,warnings}
  /api/cluster/current: {status,api_version,available,reason,snapshot,
    service:<service sample|none>,published_at,cycle,snapshot_timestamp,
    snapshot_age_seconds,time[,service_error]}
  service sample: {schema_version,timestamp,cluster_mode,model_state_source,
    vllm{available,stale,status,up,healthy,error,served_model,model_identity,
    metrics{...}},gateway{available,stale,ts,age_seconds,source_path,metrics
    {requests_total,errors_total,in_flight,since_start_seconds,requests_by_model,
    errors_by_model,status_by_code,latency_ms{p50,p95,max},tokens_in/out_by_model,
    counters},error},metrics:{vllm:{vllm_available,vllm_stale,vllm_up,
    vllm_healthy,vllm_running_reqs,vllm_waiting_reqs,vllm_kv_cache_usage,
    vllm_prompt_tokens_total,vllm_generation_tokens_total},gateway:{gw_*}},
    availability,staleness,warnings}
  /api/history/node?metrics=..&nodes=.. → {start,end,bucket_seconds,nodes,
    metrics,units,per_node:{<id>:{timestamps,sample_counts,
    series:<m>:{mean,min,max,count}}},sample_counts,sample_count}
  /api/history/service → same minus per_node (top-level timestamps/series)
  /api/history/cluster → {start,end,bucket_seconds,timestamps,
    series:{mode,healthy,transitioning,controller_ok}}
  /healthz → {status,api_version,service,time,running,interval_seconds,cycles,
    last_cycle{cycle,published_at,snapshot_timestamp,age_seconds},
    cache{has_snapshot,fresh,fresh_window_seconds,age_seconds},
    collection_errors{collect,service,history,prune},history}

## Status (COMPLETE — all code + deployment done; one operator step left)
- [x] Baseline: full pytest suite green.
- [x] S1 cluster/http_api.py: opt-in static_root serving (explicit routes only).
- [x] S2 cluster/app.py: load_config + ClusterApp + build_app + main
      (`python3 -m cluster.app --config`); allowed hostnames incl ts.net.
- [x] S3 tests/test_cluster_app.py (wiring cycle + static + /healthz + CLI).
- [x] S4 collectors/run_collector.py launcher (--config; user-service body).
- [x] S5 deploy/systemd units + deploy/daemon.sh (no-root supervisor) +
      deploy/configs.py (pure config generator).
- [x] F1 static/charts.js · F2 cluster.html · F3 cluster.js (56-line
      orchestrator) · F3b cluster-render.js · F3c cluster-history.js ·
      cluster.css.
- [x] F4 node DOM-stub smoke: split + heavy scenarios pass (tests/frontend/).
- [x] D1 DGX01 deployed: 3 token files (0600) + central.json + collector.json
      in ~/.config/syslume/; data/log in ~/.local/state/syslume/; BOTH user
      services (syslume-collector, syslume-cluster) running AND enabled.
- [x] D2 DGX02 deployed: full stdlib repo (excl .git/.venv/pycache) + config +
      token via ssh; daemon.sh supervisor (pid 604823), running now, auto-
      restarts on crash. Reboot-persistence = D4 (enable-linger).
- [x] D3 DGX03 deployed: same (pid 613287).
- [x] V1 pytest: 324 passed, 29 subtests. V2 node smoke: split+heavy pass.
- [x] V3 live 3-node telemetry: dgx01/02/03 online, reachable, sample_fresh,
      49 metrics each; mode "split", controller ok, 30s cadence observed.
- [x] V4 history accumulates: /history/node (per_node) + /history/service
      (series with mean/min/max/count) + /history/cluster (mode timeline) all
      return real data; SQLite rows confirmed (3 per_node).
- [x] V6 service persistence: services Restart=always + enabled for boot.
- [x] G1 git checkpoint: commit 17f9097 (43 code files; no tokens/configs/
      db/logs — verified).  Working tree clean.
- [!] T1 V5 tailscale 443 + D4 worker reboot-persistence — the ONLY remaining
      steps; BOTH need root (no passwordless sudo):
        sudo bash deploy/tailscale-serve.sh            # 443 -> 8765, :8443 kept
        (on dgx02 AND dgx03) sudo loginctl enable-linger cygno
      Then verify https://cygno-dgx01.taildccbbd.ts.net/ + :8443/v1 unchanged.

## Deployment & verification (completed this session)
- Tokens: 3x secrets.token_hex(16); DGX01 stash ~/.local/state/syslume/
  .deploy_tokens (0600); per-node tokens in ~/.config/syslume/tokens/.
- DGX01 user services: syslume-collector (127.0.0.1:8165) + syslume-cluster
  (127.0.0.1:8765), both `systemctl --user` running + enabled.
- DGX02/03: no linger, no system unit, no root -> deploy/daemon.sh setsid-
  detached loop (5s restart) writing a pgroup PID file; stop via
  `bash -c 'kill -- -$(cat pid)'`. Survives logout; auto-restarts on crash;
  after a reboot it runs again on next login (or always, once D4 enable-linger
  is set).
- Collector bind/peer (verified): dgx01 127.0.0.1<-127.0.0.1; dgx02
  192.168.0.2<-192.168.0.1; dgx03 192.168.2.2<-192.168.2.1.
- Auth verified: DGX02/03 /api/telemetry 200 with token, 401 without.
- Frontend: cluster.html + modules reuse SysCharts canvas charts + the shared
  history tab pattern (current/history). vllm_kv_cache_usage is a 0-1 fraction
  (formatted as %).
- History response shapes (verified): node -> per_node; service -> series dict
  {<m>:{mean,min,max,count}}; cluster -> timestamps + series{mode,healthy,
  transitioning,controller_ok}. Gateway preserved: :8443 -> 127.0.0.1:8101.

## Invariants (do not break)
- Ollama daemons, OpenClaw, Pi, Ray/vLLM, cluster-model controller untouched.
- Serve 8443 → 127.0.0.1:8101 stays. No Funnel. No 0.0.0.0 binds.
- Cluster stays in split mode; heavy-mode UI tested with fixtures only.
- No secrets/configs/SQLite/logs/backup in git.

## Key facts (verified this session)
- pytest 9.1.1 now at ~/.local (pip --user --break-system-packages).
- node v22.23.0 at ~/.nvm/... (for JS syntax + smoke tests).
- ssh aliases: dgx02-admin-cx (192.168.0.2, user cygno), dgx03-admin-cx
  (192.168.2.2, user cygno); hostnames spark-aeae / spark-ab2a.
- DGX01 user = cygno-dgx01 (Linger=yes); workers user = cygno (Linger=no).
- tailscale serve: only 8443 tailnet-only → 127.0.0.1:8101 currently.
- GW metrics file: ~/.local/state/cygno-api-gateway/metrics.json (exists).
- Controller: /home/cygno-dgx01/bin/cluster-model (exists).
- Collector bind table (fixed): dgx01→127.0.0.1, dgx02→192.168.0.2,
  dgx03→192.168.2.2; port 8165; allowed_peers default 192.168.X.1;
  token_file must be 0600 (read_token_file rejects group/world-readable).
- Aggregator defaults: node URLs 127.0.0.1:8165 / 192.168.0.2:8165 /
  192.168.2.2:8165 + /api/telemetry; cluster-model status --json.
- http_api Host/Origin allowlist: must add cygno-dgx01.taildccbbd.ts.net.
- Serve 443 will forward requests with Host = ts.net name.
