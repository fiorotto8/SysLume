"use strict";
// Fixtures for the cluster dashboard UI smoke test: full /api/cluster/current
// payloads for a SPLIT-mode cluster and a HEAVY (vLLM) cluster, plus matching
// history payloads. Shapes match the backend verbatim (see cluster/app tests).
function systemMetrics() {
  return {cpu_percent: 12, cpu_iowait_percent: 0.4, gpu_percent: 44, gpu_memory_percent: 51,
    ram_used_bytes: 48e9, ram_available_bytes: 80e9, ram_total_bytes: 128e9,
    swap_used_bytes: 0.2e9, swap_total_bytes: 2e9, cpu_freq_mhz: 2200, gpu_freq_mhz: 1800,
    gpu_memory_freq_mhz: 2600, cpu_temp_c: 55, gpu_temp_c: 61, gpu_power_w: 240,
    gpu_power_limit_w: 275, gpu_vram_used_bytes: 21e9, gpu_vram_total_bytes: 48e9,
    gpu_fan_percent: 33, fan_rpm: 900, disk_used_bytes: 400e9, disk_total_bytes: 1536e9,
    disk_read_bps: 12e6, disk_write_bps: 8e6, net_rx_bps: 4e6, net_tx_bps: 2e6,
    tailscale_rx_bps: 1e5, tailscale_tx_bps: 0.4e5, load_1: 2.1, load_5: 2.4, load_15: 2.2,
    uptime_seconds: 3.4e6, mem_unified_used_bytes: 52e9, mem_unified_total_bytes: 128e9};
}
function ollamaModels(residentNames) {
  const defs = {
    "qwen3-coder:30b": {size: 18e9, ctx: 32768, q: "Q4_K_M"},
    "deepseek-r1:14b": {size: 9e9, ctx: 16384, q: "Q5_K_M"},
    "llama3.1:8b": {size: 4.9e9, ctx: 8192, q: "Q4_K_M"},
  };
  const models = {};
  Object.keys(defs).forEach(n => {
    const d = defs[n];
    models[n] = {installed: true, resident: residentNames.includes(n), digest: "sha256:abc",
      disk_size_bytes: d.size, quantization_level: d.q, capabilities: ["completion"],
      size_bytes: null, size_vram_bytes: null, context_length_tokens: d.ctx,
      processor: null, expires_at: null, expires_at_epoch: null};
  });
  const resident = residentNames.length;
  return {status: "ok", models,
    metrics: {ollama_up: 1, ollama_models_installed: Object.keys(defs).length,
      ollama_models_resident: resident,
      ollama_resident_bytes: residentNames.reduce((t, n) => t + defs[n].size, 0),
      ollama_context_tokens_max: residentNames.length ? Math.max(...residentNames.map(n => defs[n].ctx)) : null}};
}
function procMetrics(opts) {
  opts = opts || {};
  return {ollama_daemon_count: 1, ollama_runner_count: opts.runners || 0,
    ray_process_count: opts.ray || 0, vllm_process_count: opts.vllm || 0,
    vllm_engine_core_count: opts.vllm || 0, vllm_rank_worker_count: (opts.vllm || 0) * 4,
    docker_container_count: opts.docker != null ? opts.docker : 4, tmux_session_count: opts.tmux != null ? opts.tmux : 2};
}
function nodeSample(nodeId, opts) {
  opts = opts || {};
  const resident = opts.resident || [];
  const sys = systemMetrics();
  const olm = ollamaModels(resident);
  const procs = procMetrics(opts);
  const merged = Object.assign({}, sys, olm.metrics, procs);
  return {schema_version: 1, node_id: nodeId, timestamp: opts.ts, status: "ok",
    components: {system: {status: "ok", metrics: sys}, ollama: olm, processes: {status: "ok", metrics: procs}},
    metrics: merged, warnings: []};
}
function nodeEntry(nodeId, opts) {
  opts = opts || {};
  const state = opts.state || "online";
  const sample = opts.sample === null ? null : nodeSample(nodeId, Object.assign({ts: opts.ts}, opts));
  return {state, reachable: true, fresh: state === "online",
    sample_fresh: state === "online", sample_stale: state !== "online",
    quality: sample ? sample.status : null, schema_valid: true, identity_valid: true,
    last_success_at: opts.ts - 1, age_seconds: 1, sample_age_seconds: 1, error: null,
    sample, sample_timestamp: sample ? sample.timestamp : null,
    remote_reported_stale: false, mode_expected: true, as_expected: null};
}
function splitCurrent(ts) {
  return {status: "ok", api_version: 1, available: true, reason: null,
    snapshot: {schema_version: 1, timestamp: ts,
      cluster: {mode: "split", healthy: true, transitioning: false, controller: "ok",
        source: "cluster-model", raw: "split", error: null},
      nodes: {dgx01: nodeEntry("dgx01", {ts, resident: ["llama3.1:8b"]}),
              dgx02: nodeEntry("dgx02", {ts, resident: ["qwen3-coder:30b"]}),
              dgx03: nodeEntry("dgx03", {ts, resident: ["deepseek-r1:14b"]})},
      warnings: [], meta: {total_ms: 40, deadline_seconds: 10, fresh_threshold_seconds: 45}},
    service: {ts: ts, gw_available: 1, gw_stale: 0, gw_requests_total: 1240, gw_errors_total: 3,
      gw_in_flight: 0, gw_since_start_seconds: 5e5, gw_latency_p50_ms: 42, gw_latency_p95_ms: 90,
      gw_latency_max_ms: 410, gw_age_seconds: 2, vllm_available: 0, vllm_stale: 0, vllm_up: 0,
      vllm_healthy: null, vllm_status: "inactive", vllm_served_model: null,
      vllm_running_reqs: null, vllm_waiting_reqs: null, vllm_kv_cache_usage: null,
      vllm_prompt_tokens_total: null, vllm_generation_tokens_total: null},
    published_at: ts, cycle: 100, snapshot_timestamp: ts, snapshot_age_seconds: 2, time: ts + 2};
}
function heavyCurrent(ts) {
  const cur = splitCurrent(ts);
  cur.snapshot.cluster = {mode: "heavy:qwen", healthy: true, transitioning: false,
    controller: "ok", source: "cluster-model", raw: "heavy:qwen", error: null};
  cur.snapshot.nodes.dgx03 = nodeEntry("dgx03", {ts, resident: [], vllm: 1, ray: 6, runners: 0});
  cur.snapshot.warnings = ["dgx02: telemetry stale (reachable=True): endpoint is responding but serving a cached sample"];
  cur.service = Object.assign({}, cur.service, {vllm_available: 1, vllm_stale: 0, vllm_up: 1,
    vllm_healthy: 1, vllm_status: "ok", vllm_served_model: "Qwen/Qwen3-30B-A3B",
    vllm_running_reqs: 3, vllm_waiting_reqs: 1, vllm_kv_cache_usage: 0.62,
    vllm_prompt_tokens_total: 5e6, vllm_generation_tokens_total: 2e6});
  return cur;
}
function clusterHistorySeries() {
  // 288 five-minute buckets over 24h: split -> heavy -> split, one gap.
  const n = 288, mode = [], healthy = [], transitioning = [], controller_ok = [], ts0 = 1_760_000_000, step = 300;
  for (let i = 0; i < n; i++) {
    const m = i < 60 ? "split" : i < 100 ? "heavy:qwen" : i < 120 ? null : "split";
    mode.push(m); healthy.push(m ? 1 : null); transitioning.push(i === 60 ? 1 : 0);
    controller_ok.push(1);
  }
  return {mode, healthy, transitioning, controller_ok};
}
module.exports = {splitCurrent, heavyCurrent, clusterHistorySeries, nodeSample,
  serviceRow: () => ({}), timelineRange: (end) => ({start: end - 86400, end, bucket_seconds: 300})};
