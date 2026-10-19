"use strict";
// SysLume Cluster — render module.
// Pure view layer: takes data from the live snapshot/service sample and
// populates the fixed DOM. No fetching, no timers; cluster.js owns those.
window.SysClusterRender = (() => {
  const {byId, has, number, unit, gib, rate, text, date} = M;

  const NODE_IDS = ["dgx01", "dgx02", "dgx03"];
  const NODE_LABEL = {dgx01: "DGX01", dgx02: "DGX02", dgx03: "DGX03"};
  const NODE_ROLE = {
    dgx01: "controller · dashboard",
    dgx02: "ollama · heavy backend",
    dgx03: "ollama · heavy backend",
  };
  const MODE_LABEL = {
    split: "Split", "heavy:qwen": "Heavy · Qwen", "heavy:deepseek": "Heavy · DeepSeek",
    stop: "Stopped", unknown: "Unknown",
  };

  // --- small formatters (all null-safe, "—" for unknown) ---------------
  function pct(v) { return has(v) ? number(v, 0) + "%" : "—"; }
  function frac(v) { return has(v) ? number(v * 100, 0) + "%" : "—"; }
  function secs(v) { return has(v) ? number(v, 0) + "s" : "—"; }
  function tokens(v) { return has(v) ? number(v / 1000, 0) + "k" : "—"; }
  function clock(ts) { return ts ? new Date(ts * 1000).toLocaleTimeString() : "—"; }
  function uptime(v) {
    if (!has(v)) return "—";
    const d = Math.floor(v / 86400), h = Math.floor((v % 86400) / 3600), m = Math.floor((v % 3600) / 60);
    return d ? `${d}d ${h}h` : h ? `${h}h ${m}m` : `${m}m ${Math.floor(v % 60)}s`;
  }
  function modeClass(mode) { return (["split", "stop", "unknown", "transitioning", "degraded", "unreachable"].includes(mode) ? mode : (mode === "heavy:qwen" || mode === "heavy:deepseek") ? mode.replace("heavy:", "heavy-") : "unknown"); }

  function dot(kind) {
    const d = document.createElement("span"); d.className = "dot " + kind; return d;
  }
  function line(label, value, kind) {
    const row = document.createElement("div"); row.className = "status-line";
    row.append(dot(kind || "off"));
    const lab = document.createElement("span"); lab.textContent = label;
    const val = document.createElement("span");
    val.style.marginLeft = "auto"; val.style.fontVariantNumeric = "tabular-nums";
    val.textContent = value === null || value === undefined ? "—" : value;
    row.append(lab, val);
    return row;
  }
  function panel(title, rows) {
    const p = document.createElement("div"); p.className = "subpanel";
    const h = document.createElement("h3"); h.textContent = title;
    p.append(h); rows.forEach(r => p.append(r));
    return p;
  }

  // --- cluster banner (mode block + 3 subpanels) -------------------------
  function renderBanner(cluster, service, snapshot) {
    const banner = byId("cluster-banner");
    if (!banner) return;
    if (!cluster) { banner.innerHTML = '<p class="empty">Waiting for a cluster reading…</p>'; return; }
    banner.innerHTML = "";
    const mode = cluster.mode || "unknown";
    const transitioning = cluster.transitioning === true;
    const grid = document.createElement("div"); grid.className = "banner-grid";

    const block = document.createElement("div"); block.className = "mode-block";
    const chip = document.createElement("span");
    chip.className = "mode-chip " + modeClass(transitioning ? "transitioning" : mode);
    chip.textContent = transitioning ? "Transitioning…" : (MODE_LABEL[mode] || mode);
    const meta = document.createElement("div"); meta.className = "mode-meta";
    const metaItem = (label, ok, val) => {
      const s = document.createElement("span");
      s.className = ok === true ? "ok" : ok === false ? "bad" : "warn";
      s.textContent = `${label}: ${val}`; meta.append(s);
    };
    metaItem("healthy", cluster.healthy === true, cluster.healthy === true ? "yes" : cluster.healthy === false ? "no" : "n/a");
    metaItem("transitioning", transitioning === true ? true : transitioning === false ? false : null, transitioning ? "yes" : "no");
    metaItem("controller", cluster.controller === "ok", cluster.controller === "ok" ? "ok" : cluster.controller === "error" ? "error" : "absent");
    if (cluster.error) metaItem("detail", false, String(cluster.error).slice(0, 90));
    block.append(chip, meta);
    grid.append(block);

    const sub = document.createElement("div"); sub.className = "subpanels";
    sub.append(vllmPanel(service));
    sub.append(gatewayPanel(service));
    sub.append(rayOllamaPanel(snapshot));
    grid.append(sub);
    banner.append(grid);
  }

  function vllmPanel(s) {
    const g = k => s ? s[k] : undefined;
    const st = g("vllm_status");
    const stKind = st === "ok" ? "good" : st === "inactive" ? "off" : st === "starting" ? "warn" : "bad";
    const rows = [
      line("status", st || "—", stKind),
      line("served model", g("vllm_served_model") || "—", g("vllm_served_model") ? "good" : "off"),
      line("healthy", g("vllm_healthy") === true ? "yes" : g("vllm_healthy") === false ? "no" : "—", g("vllm_healthy") === true ? "good" : g("vllm_healthy") === false ? "bad" : "off"),
      line("running / queue", [g("vllm_running_reqs"), g("vllm_waiting_reqs")].map(v => has(v) ? number(v, 0) : "—").join(" / "), "off"),
      line("KV cache", frac(g("vllm_kv_cache_usage")), "off"),
      line("tokens in/out", [g("vllm_prompt_tokens_total"), g("vllm_generation_tokens_total")].map(tokens).join(" / "), "off"),
    ];
    return panel("vLLM · heavy", rows);
  }

  function gatewayPanel(s) {
    const g = k => s ? s[k] : undefined;
    const avail = g("gw_available");
    const kind = avail === true ? "good" : avail === false ? "bad" : "off";
    const rows = [
      line("status", avail === true ? (g("gw_stale") ? "stale" : "up") : avail === false ? "down" : "—", g("gw_stale") ? "warn" : kind),
      line("uptime", uptime(g("gw_since_start_seconds")), "off"),
      line("requests (total)", has(g("gw_requests_total")) ? number(g("gw_requests_total"), 0) : "—", "off"),
      line("errors (total)", has(g("gw_errors_total")) ? number(g("gw_errors_total"), 0) : "—", has(g("gw_errors_total")) && g("gw_errors_total") > 0 ? "warn" : "off"),
      line("in flight", has(g("gw_in_flight")) ? number(g("gw_in_flight"), 0) : "—", "off"),
      line("latency p50/p95/max", [g("gw_latency_p50_ms"), g("gw_latency_p95_ms"), g("gw_latency_max_ms")].map(v => has(v) ? number(v, 1) : "—").join(" / "), "off"),
    ];
    return panel("API gateway", rows);
  }

  function rayOllamaPanel(snapshot) {
    const nodes = (snapshot && snapshot.nodes) || {};
    const sum = key => NODE_IDS.reduce((t, id) => {
      const m = nodes[id] && nodes[id].sample && nodes[id].sample.metrics;
      return t + (m && has(m[key]) ? m[key] : 0);
    }, 0);
    const anyOllamaUp = NODE_IDS.some(id => {
      const m = nodes[id] && nodes[id].sample && nodes[id].sample.metrics;
      return m && m.ollama_up === 1;
    });
    const rows = [
      line("ray processes", number(sum("ray_process_count"), 0), sum("ray_process_count") > 0 ? "good" : "off"),
      line("ollama up", anyOllamaUp ? "yes" : "no", anyOllamaUp ? "good" : "warn"),
      line("ollama daemon/runners", `${number(sum("ollama_daemon_count"), 0)} / ${number(sum("ollama_runner_count"), 0)}`, "off"),
      line("ollama models inst/res", `${number(sum("ollama_models_installed"), 0)} / ${number(sum("ollama_models_resident"), 0)}`, "off"),
      line("vllm engine/worker", `${number(sum("vllm_engine_core_count"), 0)} / ${number(sum("vllm_rank_worker_count"), 0)}`, "off"),
      line("containers / tmux", `${number(sum("docker_container_count"), 0)} / ${number(sum("tmux_session_count"), 0)}`, "off"),
    ];
    return panel("ray · ollama", rows);
  }

  // --- three-node overview ----------------------------------------------
  const STATE_BADGE = {online: "online", stale: "stale", error: "error", never: "never", unknown: "unknown"};
  const STATE_TEXT = {online: "online", stale: "stale", error: "error", never: "no data", unknown: "—"};
  function quick(label, value) {
    const q = document.createElement("div"); q.className = "quick";
    const lab = document.createElement("div"); lab.className = "quick-label";
    const k = document.createElement("span"); k.textContent = label;
    const b = document.createElement("b"); b.textContent = value === null || value === undefined ? "—" : value;
    lab.append(k, b); q.append(lab);
    return q;
  }
  function detailRow(label, value) {
    const r = document.createElement("div"); r.className = "status-line";
    const lab = document.createElement("span"); lab.textContent = label;
    const val = document.createElement("span"); val.style.marginLeft = "auto";
    val.style.fontVariantNumeric = "tabular-nums";
    val.textContent = value === null || value === undefined ? "—" : value;
    r.append(lab, val);
    return r;
  }

  function renderNodes(nodes) {
    const grid = byId("node-grid");
    if (!grid) return;
    if (!nodes) { grid.innerHTML = '<p class="empty">Waiting for node telemetry…</p>'; return; }
    grid.innerHTML = "";
    NODE_IDS.forEach(id => grid.append(nodeCard(id, (nodes && nodes[id]) || null)));
  }

  function nodeCard(id, entry) {
    const card = document.createElement("article"); card.className = "node-card";
    const state = entry ? entry.state : "unknown";
    card.dataset.state = STATE_BADGE[state] || "unknown";

    const head = document.createElement("div"); head.className = "node-head";
    const namebox = document.createElement("div");
    const name = document.createElement("div"); name.className = "node-name"; name.textContent = NODE_LABEL[id] || id;
    const role = document.createElement("div"); role.className = "node-role"; role.textContent = NODE_ROLE[id] || "";
    namebox.append(name, role);
    const badge = document.createElement("span");
    badge.className = "state " + (STATE_BADGE[state] || "unknown");
    badge.textContent = STATE_TEXT[state] || "—";
    head.append(namebox, badge);
    card.append(head);

    const sample = entry && entry.sample;
    const m = sample && sample.metrics;
    if (!sample || !m) {
      const empty = document.createElement("p"); empty.className = "empty";
      empty.textContent = entry && entry.state === "never" ? "No telemetry received yet"
        : "Telemetry unavailable" + (entry && entry.error ? " — " + String(entry.error).slice(0, 80) : "");
      card.append(empty);
      return card;
    }

    const ramTotal = m.mem_unified_total_bytes, ramUsed = m.mem_unified_used_bytes;
    const ramPct = has(ramTotal) && has(ramUsed) ? (ramUsed / ramTotal * 100) : null;
    const quicks = document.createElement("div"); quicks.className = "node-quick";
    quicks.append(
      quick("cpu", pct(m.cpu_percent)),
      quick("gpu", pct(m.gpu_percent)),
      quick("ram", has(ramPct) ? number(ramPct, 0) + "%" : "—"),
      quick("gpu mem", pct(m.gpu_memory_percent)),
      quick("ollama models", has(m.ollama_models_resident) ? number(m.ollama_models_resident, 0) + " resident" : (m.ollama_up === 1 ? "up" : "down")),
      quick("load (1m)", has(m.load_1) ? number(m.load_1, 1) : "—"),
    );
    card.append(quicks);
    card.append(detailsSection(card, id, sample, entry, m));
    return card;
  }

  function detailsSection(card, id, sample, entry, m) {
    const wrap = document.createElement("div"); wrap.className = "node-details";
    const toggle = document.createElement("button"); toggle.type = "button";
    toggle.className = "toggle-details"; toggle.textContent = "Details";
    wrap.append(toggle);

    const rows = document.createElement("div"); rows.className = "rows";
    rows.append(
      detailRow("cpu iowait", pct(m.cpu_iowait_percent)),
      detailRow("cpu freq", has(m.cpu_freq_mhz) ? number(m.cpu_freq_mhz, 0) + " MHz" : "—"),
      detailRow("gpu temp / power", `${has(m.gpu_temp_c) ? number(m.gpu_temp_c, 0) + "°C" : "—"} / ${has(m.gpu_power_w) ? number(m.gpu_power_w, 0) + " W" : "—"}`),
      detailRow("gpu vram", `${m.gpu_vram_used_bytes ? gib(m.gpu_vram_used_bytes) : "—"} / ${m.gpu_vram_total_bytes ? gib(m.gpu_vram_total_bytes) : "—"}`),
      detailRow("ram used / total", `${m.mem_unified_used_bytes ? gib(m.mem_unified_used_bytes) : "—"} / ${m.mem_unified_total_bytes ? gib(m.mem_unified_total_bytes) : "—"}`),
      detailRow("disk used / total", `${m.disk_used_bytes ? gib(m.disk_used_bytes) : "—"} / ${m.disk_total_bytes ? gib(m.disk_total_bytes) : "—"} ${m.disk_total_bytes && m.disk_used_bytes ? "(" + number(m.disk_used_bytes / m.disk_total_bytes * 100, 0) + "%)" : ""}`),
      detailRow("net rx / tx", `${rate(m.net_rx_bps)} / ${rate(m.net_tx_bps)}`),
      detailRow("tailscale rx / tx", `${rate(m.tailscale_rx_bps)} / ${rate(m.tailscale_tx_bps)}`),
      detailRow("swap in / out", `${rate(m.swap_in_bps)} / ${rate(m.swap_out_bps)}`),
      detailRow("load 1/5/15", [m.load_1, m.load_5, m.load_15].map(v => has(v) ? number(v, 1) : "—").join(" / ")),
      detailRow("uptime", uptime(m.uptime_seconds)),
      detailRow("ollama resident (GiB)", m.ollama_resident_bytes ? gib(m.ollama_resident_bytes) : "—"),
      detailRow("ollama ctx max (tokens)", has(m.ollama_context_tokens_max) ? number(m.ollama_context_tokens_max / 1000, 0) + "k" : "—"),
      detailRow("ray procs", has(m.ray_process_count) ? number(m.ray_process_count, 0) : "0"),
      detailRow("vllm proc / eng / worker", `${has(m.vllm_process_count) ? number(m.vllm_process_count, 0) : 0} / ${has(m.vllm_engine_core_count) ? number(m.vllm_engine_core_count, 0) : 0} / ${has(m.vllm_rank_worker_count) ? number(m.vllm_rank_worker_count, 0) : 0}`),
    );
    wrap.append(rows);

    const models = sample.components && sample.components.ollama && sample.components.ollama.models;
    if (models && Object.keys(models).length) wrap.append(modelsTable(models));
    if (entry && entry.sample_age_seconds !== null && entry.sample_age_seconds !== undefined)
      wrap.append(detailRow("last fresh sample", entry.sample_age_seconds < 1 ? "just now" : Math.round(entry.sample_age_seconds) + "s ago"));
    if (sample.warnings && sample.warnings.length)
      rows.append(detailRow("warnings", sample.warnings.length + " (see history)"));
    toggle.onclick = () => {
      const open = card.classList.toggle("open");
      toggle.textContent = open ? "Hide details" : "Details";
    };
    return wrap;
  }


  function sizeText(e) {
    const b = e.size_bytes || e.disk_size_bytes;
    return b ? gib(b) : "—";
  }
  function modelsTable(models) {
    const det = document.createElement("details"); det.className = "models";
    const summary = document.createElement("summary");
    summary.textContent = `ollama models (${Object.keys(models).length})`;
    det.append(summary);
    const table = document.createElement("table"); table.className = "model-table";
    const thead = document.createElement("thead");
    const th = document.createElement("tr");
    ["model", "state", "size", "ctx"].forEach((t, i) => {
      const c = document.createElement("th"); c.textContent = t;
      if (i >= 2) c.className = "num";
      th.append(c);
    });
    thead.append(th);
    const tbody = document.createElement("tbody");
    Object.keys(models).sort().forEach(nm => {
      const e = models[nm];
      const tr = document.createElement("tr");
      const nameTd = document.createElement("td"); nameTd.textContent = nm;
      const stateTd = document.createElement("td");
      const tags = [];
      if (e.resident) tags.push("resident");
      else if (e.installed) tags.push("installed");
      if (e.quantization_level) tags.push(e.quantization_level);
      tags.forEach(t => {
        const span = document.createElement("span");
        span.className = "tag " + (t === "resident" ? "resident" : t === "installed" ? "installed" : "");
        span.textContent = t;
        stateTd.append(span, " ");
      });
      const sizeTd = document.createElement("td"); sizeTd.className = "num"; sizeTd.textContent = sizeText(e);
      const ctxTd = document.createElement("td"); ctxTd.className = "num";
      ctxTd.textContent = e.context_length_tokens ? number(e.context_length_tokens / 1000, 0) + "k" : "—";
      tr.append(nameTd, stateTd, sizeTd, ctxTd);
      tbody.append(tr);
    });
    table.append(thead, tbody); det.append(table);
    return det;
  }

  // --- warnings panel -----------------------------------------------------
  function renderWarnings(warnings) {
    const list = (Array.isArray(warnings) ? warnings : []);
    const panel = byId("warn-panel");
    if (!panel) return;
    if (!list.length) { panel.hidden = true; return; }
    panel.hidden = false;
    const ul = document.createElement("ul"); ul.className = "warn-list";
    list.slice(0, 12).forEach(w => { const li = document.createElement("li"); li.textContent = String(w); ul.append(li); });
    panel.innerHTML = "";
    panel.append(Object.assign(document.createElement("h3"), {textContent: "Warnings"}), ul);
  }

  return {
    renderBanner, renderNodes, renderWarnings,
    NODE_IDS, NODE_LABEL, NODE_ROLE, MODE_LABEL,
  };
})();
