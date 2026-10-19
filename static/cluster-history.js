"use strict";
// SysLume Cluster — history + mode-timeline module.
// Reuses the shared SysCharts.Chart engine (same rendering/zoom/tooltips as
// the single-host History page). Owns the shared time-range object and loads
// /api/history/node + /api/history/cluster. No live-sample fetching.
window.SysClusterHistory = (() => {
  const {byId, has, number, text, date, get} = M;
  const {Chart} = window.SysCharts;

  const RETENTION = 30 * 86400;     // 30-day retention
  const SAMPLE = 15;                // central 15s sampling (seconds)
  const MIN_SPAN = SAMPLE * 2;

  // Same chart groups as the single-host History page (identical metric keys).
  const GROUPS = [
    {title: "Utilization", unit: "%", max: 100, wide: true, keys: [["cpu_percent", "CPU"], ["gpu_percent", "GPU"], ["gpu_memory_percent", "GPU memory controller"]]},
    {title: "Host memory", unit: "GiB", factor: 1073741824, keys: [["ram_used_bytes", "RAM used"], ["ram_available_bytes", "RAM available"], ["swap_used_bytes", "Swap used"]]},
    {title: "GPU VRAM", unit: "GiB", factor: 1073741824, keys: [["gpu_vram_used_bytes", "VRAM used"], ["gpu_vram_total_bytes", "VRAM total"]]},
    {title: "Temperatures", unit: "°C", keys: [["cpu_temp_c", "CPU"], ["gpu_temp_c", "GPU"]]},
    {title: "GPU power", unit: "W", keys: [["gpu_power_w", "Draw"], ["gpu_power_limit_w", "Limit"]]},
    {title: "Clocks", unit: "MHz", keys: [["cpu_freq_mhz", "CPU mean"], ["gpu_freq_mhz", "GPU graphics"], ["gpu_memory_freq_mhz", "GPU memory"]]},
    {title: "Storage I/O", unit: "MiB/s", factor: 1048576, keys: [["disk_read_bps", "Read"], ["disk_write_bps", "Write"]]},
    {title: "Network", unit: "MiB/s", factor: 1048576, keys: [["net_rx_bps", "Receive"], ["net_tx_bps", "Transmit"]]},
    {title: "Tailscale traffic", unit: "MiB/s", factor: 1048576, keys: [["tailscale_rx_bps", "Receive"], ["tailscale_tx_bps", "Transmit"]]},
    {title: "Swap I/O", unit: "MiB/s", factor: 1048576, keys: [["swap_in_bps", "Swap in"], ["swap_out_bps", "Swap out"]]},
    {title: "Storage capacity", unit: "GiB", factor: 1073741824, keys: [["disk_used_bytes", "Used"]]},
    {title: "Load average", unit: "runnable / waiting", keys: [["load_1", "1 minute"], ["load_5", "5 minutes"], ["load_15", "15 minutes"]]},
  ];
  const METRICS = Array.from(new Set(GROUPS.reduce((a, g) => a.concat(g.keys.map(k => k[0])), []))).join(",");

  let range = null, charts = [];
  let histNode = "dgx01", baseDuration = 21600;
  let nodeEl, presetEl, liveEl, controller = null, debounce = null;
  const now = () => Date.now() / 1000;

  function setWindow(start, end) {
    let span = Math.min(RETENTION, Math.max(MIN_SPAN, end - start));
    if (end > now()) { end = now(); start = end - span; }
    range.from = Math.max(0, start); range.to = range.from + span;
    text("range-label", `${date(range.from)} → ${date(range.to)}`);
    charts.forEach(c => c.draw());
  }

  async function loadHistory() {
    if (controller) controller.abort();
    controller = new AbortController();
    const my = controller;
    try {
      const data = await get(`/api/history/node?nodes=${histNode}&metrics=${METRICS}&start=${range.from}&end=${range.to}&points=1000`, my.signal);
      if (my !== controller) return;
      const nodeData = data.per_node && data.per_node[histNode];
      charts.forEach(c => c.setData(nodeData || {timestamps: [], sample_counts: [], series: {}}));
      const n = nodeData && nodeData.sample_count ? nodeData.sample_count : 0;
      const grouped = nodeData && nodeData.sample_counts && nodeData.sample_counts.some(x => x > 1);
      text("hist-summary", n ? `${n.toLocaleString()} samples · ${grouped ? `${number(nodeData.bucket_seconds, 0)}s buckets` : "recorded resolution"} · 30-day retention` : "no stored samples in this range · 30-day retention");
      M.notice("");
    } catch (error) {
      if (error.name === "AbortError") return;
      text("hist-summary", "history unavailable");
      M.notice(`Cannot load history: ${error.message}`);
    }
  }
  function queueHistory() { clearTimeout(debounce); debounce = setTimeout(loadHistory, 150); }

  // --- mode timeline (fixed 24h window; authoritative cluster-model mode)
  const TL_HOURS = 24;
  const MODE_SHORT = {"split": "split", "heavy:qwen": "heavy·qwen", "heavy:deepseek": "heavy·deepseek", "stop": "stop", "unknown": "unknown"};
  function modeCls(mode) {
    if (mode === "heavy:qwen" || mode === "heavy:deepseek") return mode.replace("heavy:", "heavy-");
    return ["split", "stop", "unknown", "degraded", "unreachable", "transitioning"].includes(mode) ? mode : "unknown";
  }
  async function loadTimeline() {
    const el = byId("timeline");
    if (!el) return;
    const end = now(), start = end - TL_HOURS * 3600;
    try {
      const data = await get(`/api/history/cluster?start=${start}&end=${end}&points=288`);
      renderTimeline(el, data);
    } catch (error) {
      el.innerHTML = '<p class="empty">Timeline unavailable</p>';
    }
  }
  function renderTimeline(el, data) {
    el.innerHTML = "";
    const modes = data.series.mode || [];
    const times = data.timestamps || [];
    const wrap = document.createElement("div"); wrap.className = "tl-wrap";
    const strip = document.createElement("div"); strip.className = "tl-strip";
    const segs = [];
    for (let i = 0; i < modes.length; i++) {
      const m = modes[i];
      const last = segs.length ? segs[segs.length - 1] : null;
      if (last && last.mode === m) last.end = i;
      else segs.push({mode: m, start: i, end: i});
    }
    const span = times.length ? (times[times.length - 1] - times[0]) || 1 : 1;
    segs.forEach(s => {
      const seg = document.createElement("div");
      seg.className = "tl-seg " + modeCls(s.mode);
      seg.textContent = MODE_SHORT[s.mode] || s.mode || "—";
      seg.style.flexGrow = (s.end - s.start + 1);
      seg.title = `${MODE_SHORT[s.mode] || s.mode || "no data"} · ${date(times[s.start])} → ${date(times[s.end])}`;
      strip.append(seg);
    });
    wrap.append(strip);
    const axis = document.createElement("div"); axis.className = "tl-axis";
    axis.append(...[0, 0.25, 0.5, 0.75, 1].map(f => {
      const s = document.createElement("span");
      s.textContent = new Date(times[0] * 1000 + f * span * 1000).toLocaleString([], {hour: "2-digit", minute: "2-digit", weekday: f === 0 ? "short" : undefined});
      return s;
    }));
    wrap.append(axis);
    const list = document.createElement("ul"); list.className = "tl-list";
    let prev = null;
    for (let i = 0; i < modes.length; i++) {
      const m = modes[i] || "no data";
      if (m !== prev) {
        const li = document.createElement("li");
        const t = document.createElement("span"); t.className = "tl-time"; t.textContent = date(times[i]);
        const from = document.createElement("span"); from.className = "tl-from"; from.textContent = prev || "—";
        const to = document.createElement("span"); to.className = "tl-to"; to.textContent = m;
        li.append(t, from, document.createTextNode(" → "), to);
        list.append(li); prev = m;
      }
    }
    if (!list.children.length) list.innerHTML = '<li><span class="tl-time">no transitions</span></li>';
    wrap.append(list);
    el.append(wrap);
  }

  function init() {
    nodeEl = byId("hist-node"); presetEl = byId("preset"); liveEl = byId("live");
    if (presetEl) baseDuration = Math.min(RETENTION, Number(presetEl.value) || baseDuration);
    histNode = nodeEl ? nodeEl.value : "dgx01";
    range = {
      from: now() - baseDuration, to: now(), interval: SAMPLE,
      onChange: (start, end) => { if (liveEl) liveEl.checked = false; setWindow(start, end); queueHistory(); },
      onReset: () => { if (liveEl) liveEl.checked = true; setWindow(now() - baseDuration, now()); queueHistory(); },
      onZoom: (factor, fraction) => {
        const span = range.to - range.from;
        const newSpan = Math.min(RETENTION, Math.max(MIN_SPAN, span * factor));
        const anchor = range.from + span * fraction;
        if (liveEl) liveEl.checked = false;
        setWindow(anchor - newSpan * fraction, anchor + newSpan * (1 - fraction));
        queueHistory();
      },
    };
    const container = byId("charts");
    container.innerHTML = "";
    charts = GROUPS.map(def => new Chart(def, range, container));
    window.addEventListener("themechange", () => charts.forEach(c => c.draw()));

    if (nodeEl) nodeEl.addEventListener("change", () => { histNode = nodeEl.value; queueHistory(); });
    if (presetEl) presetEl.addEventListener("change", () => {
      baseDuration = Math.min(RETENTION, Number(presetEl.value) || baseDuration);
      if (liveEl) liveEl.checked = true;
      setWindow(now() - baseDuration, now());
      queueHistory();
    });
    const resetBtn = byId("reset");
    if (resetBtn) resetBtn.addEventListener("click", () => {
      if (liveEl) liveEl.checked = true;
      setWindow(now() - baseDuration, now());
      queueHistory();
    });
    if (liveEl) liveEl.addEventListener("change", () => {
      if (liveEl.checked) { const span = range.to - range.from; setWindow(now() - span, now()); queueHistory(); }
    });

    setWindow(now() - baseDuration, now());
    loadHistory();
    loadTimeline();
    let lastTick = now();
    setInterval(() => {
      if (document.hidden) return;
      const t = now();
      if (liveEl && liveEl.checked && t - lastTick >= 15) {
        setWindow(now() - (range.to - range.from), now());
        queueHistory();
        loadTimeline();
        lastTick = t;
      } else if (t - lastTick >= 30) {
        loadTimeline();
        lastTick = t;
      }
    }, 15000);
    return {range};
  }
  return {init, GROUPS};
})();
