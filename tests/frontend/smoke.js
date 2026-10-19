"use strict";
// DOM smoke test for the SysLume cluster dashboard.
// Evaluates the four browser modules (common, charts, cluster-render,
// cluster-history) + cluster.js in Node against a DOM stub with fixtures,
// for both a SPLIT-mode and a HEAVY-mode cluster, and asserts the tree.
const fs = require("fs");
const path = require("path");
const {Element, TextNode, subtreeText, makeDocument} = require("./dom_stub");
const fx = require("./fixtures");

const STATIC = path.join(__dirname, "..", "..", "static");
const FILES = ["common.js", "charts.js", "cluster-render.js", "cluster-history.js", "cluster.js"];
const IDS = ["connection-status", "last-read", "notice", "cluster-eyebrow", "cluster-subtitle",
  "cluster-banner", "warn-panel", "node-grid", "hist-summary", "hist-node", "preset",
  "live", "reset", "range-label", "charts", "timeline", "theme-toggle", "theme-label",
  "host-name", "machine-model"];

function nodeHistoryFixture(end, bucket) {
  const METRIC_KEYS = ["cpu_percent","cpu_iowait_percent","gpu_percent","gpu_memory_percent",
    "ram_used_bytes","ram_available_bytes","swap_used_bytes","gpu_vram_used_bytes","gpu_vram_total_bytes",
    "cpu_temp_c","gpu_temp_c","gpu_power_w","gpu_power_limit_w","cpu_freq_mhz","gpu_freq_mhz",
    "gpu_memory_freq_mhz","disk_read_bps","disk_write_bps","net_rx_bps","net_tx_bps",
    "tailscale_rx_bps","tailscale_tx_bps","swap_in_bps","swap_out_bps","disk_used_bytes",
    "load_1","load_5","load_15"];
  const n = 30, start = end - n * bucket, ts = [];
  const series = {};
  for (let i = 0; i < n; i++) ts.push(start + i * bucket);
  METRIC_KEYS.forEach(k => {
    const mean = [], min = [], max = [], count = [];
    for (let i = 0; i < n; i++) {
      const base = k.includes("bytes") ? 1e9 : k.includes("bps") ? 1e6 : 50;
      mean.push(base * (1 + 0.1 * Math.sin(i / 3)));
      min.push(base * 0.95); max.push(base * 1.05); count.push(2);
    }
    series[k] = {mean, min, max, count};
  });
  return {start, end, bucket_seconds: bucket,
    per_node: {dgx01: {timestamps: ts, sample_counts: ts.map(() => 2), sample_count: n * 2,
      bucket_seconds: bucket, series},
      dgx02: {timestamps: ts, sample_counts: ts.map(() => 2), sample_count: n * 2, bucket_seconds: bucket, series},
      dgx03: {timestamps: ts, sample_counts: ts.map(() => 2), sample_count: n * 2, bucket_seconds: bucket, series}}};
}
function clusterHistoryFixture(end) {
  const bucket = 300, n = 288, start = end - n * bucket, s = fx.clusterHistorySeries();
  const timestamps = []; for (let i = 0; i < n; i++) timestamps.push(start + i * bucket);
  return {start, end, bucket_seconds: bucket, timestamps,
    series: {mode: s.mode, healthy: s.healthy, transitioning: s.transitioning, controller_ok: s.controller_ok}};
}

let _combined = null;
function combinedSource() {
  if (!_combined) _combined = FILES.map(f => fs.readFileSync(path.join(STATIC, f), "utf8")).join("\n;\n");
  return _combined;
}
function boot(currentFixture, endTs) {
  const root = new Element("html");
  const doc = makeDocument(root);
  const elements = {};
  IDS.forEach(id => { const e = new Element("div"); e.id = id; elements[id] = e; root.appendChild(e); });
  elements["hist-node"].value = "dgx01";
  elements["preset"].value = "21600";
  elements["live"].checked = true;
  elements["node-grid"].className = "node-grid";

  const nodeHist = nodeHistoryFixture(endTs, 300);
  const clusterHist = clusterHistoryFixture(endTs);
  const fetchHandler = (url) => {
    const u = String(url);
    if (u.startsWith("/api/cluster/current")) return {ok: true, status: 200, json: async () => currentFixture};
    if (u.startsWith("/api/history/node")) return {ok: true, status: 200, json: async () => nodeHist};
    if (u.startsWith("/api/history/cluster")) return {ok: true, status: 200, json: async () => clusterHist};
    return {ok: false, status: 404, json: async () => ({error: "not found", path: u})};
  };

  // --- install globals ---
  const win = global.window = global.window || {};
  global.window = global;
  global.document = doc;
  global.fetch = (u) => Promise.resolve(fetchHandler(u));
  global.ResizeObserver = class { constructor(cb) { this.cb = cb; } observe() {} unobserve() {} disconnect() {} };
  global.getComputedStyle = () => ({getPropertyValue: () => "#333333"});
  global.Event = class { constructor(t) { this.type = t; } };
  global.matchMedia = () => ({matches: false, addEventListener() {}});
  global.localStorage = {getItem: () => null, setItem() {}, removeItem() {}};
  global.devicePixelRatio = 1;
  global.dispatchEvent = (ev) => { (global._winListeners && global._winListeners[ev.type] || []).forEach(f => f(ev)); };
  global.addEventListener = (t, f) => { (global._winListeners = global._winListeners || {})[t] = (global._winListeners[t] || []).concat([f]); };
  global.removeEventListener = () => {};
  global.location = {origin: "http://127.0.0.1:8765", protocol: "http:", host: "127.0.0.1:8765", href: "http://127.0.0.1:8765/"};
  if (typeof global.AbortController === "undefined") global.AbortController = class { constructor() { this.signal = {aborted: false}; } abort() { this.signal.aborted = true; } };

  (0, eval)(combinedSource());
  return doc;
}
function el(doc, id) { return doc.getElementById(id); }
function hasClass(node, cls) { return node && (node.classList ? node.classList.contains(cls) : false); }
function findDescendants(root, pred, out) {
  out = out || [];
  const walk = n => { if (pred(n)) out.push(n); (n.children || []).forEach(walk); };
  (root.children || []).forEach(walk);
  return out;
}
const sleep = ms => new Promise(r => setTimeout(r, ms));

function assert(cond, msg) { if (!cond) throw new Error("ASSERT FAILED: " + msg); }

async function runScenario(tag, current, endTs) {
  const doc = boot(current, endTs);
  await sleep(150);           // let init + first refresh/history load settle
  return {doc, endTs};
}

async function testSplit() {
  const ts = 1_760_000_000, {doc} = await runScenario("split", fx.splitCurrent(ts), ts + 2);
  const banner = el(doc, "cluster-banner");
  assert(banner, "banner exists");
  assert(subtreeText(banner).includes("Split"), "split banner shows 'Split' mode");
  const chips = findDescendants(banner, n => n.nodeType === 1 && hasClass(n, "mode-chip"));
  assert(chips.length === 1 && hasClass(chips[0], "split"), "mode chip is split-styled");
  assert(subtreeText(banner).includes("inactive"), "vLLM shows inactive in split mode");
  assert(subtreeText(banner).includes("1,240"), "gateway shows 1,240 requests");
  assert(subtreeText(banner).includes("up"), "gateway status up");

  const grid = el(doc, "node-grid");
  assert(grid, "node-grid exists");
  const cards = findDescendants(grid, n => n.nodeType === 1 && hasClass(n, "node-card"));
  assert(cards.length === 3, "three node cards, got " + cards.length);
  const names = subtreeText(grid).match(/DGX0[123]/g) || [];
  assert(names.includes("DGX01") && names.includes("DGX02") && names.includes("DGX03"),
    "all three node names present: " + names.join(","));

  const warn = el(doc, "warn-panel");
  assert(warn.hidden, "warnings panel hidden in clean split mode");

  const chartsEl = el(doc, "charts");
  const chartPanels = findDescendants(chartsEl, n => n.nodeType === 1 && hasClass(n, "chart-panel"));
  assert(chartPanels.length === 12, "all 12 chart panels rendered, got " + chartPanels.length);

  const tl = el(doc, "timeline");
  const segs = findDescendants(tl, n => n.nodeType === 1 && hasClass(n, "tl-seg"));
  assert(segs.length >= 3 && segs.length <= 8, "mode timeline segments, got " + segs.length);

  const status = el(doc, "connection-status");
  assert(/good/.test(status.className), "connection status good, got '" + status.className + "'");
  assert(/normally/i.test(subtreeText(status)), "status text 'Reading normally'");
  assert(/s ago/.test(subtreeText(el(doc, "last-read"))), "last-read shows '..s ago'");
  console.log("PASS split-mode scenario");
}

async function testHeavy() {
  const ts = 1_760_000_500, {doc} = await runScenario("heavy", fx.heavyCurrent(ts), ts + 2);
  const banner = el(doc, "cluster-banner");
  assert(subtreeText(banner).includes("Heavy"), "heavy banner shows Heavy");
  const chips = findDescendants(banner, n => n.nodeType === 1 && hasClass(n, "mode-chip"));
  assert(chips.length === 1 && hasClass(chips[0], "heavy-qwen"), "mode chip heavy-qwen styled");
  assert(subtreeText(banner).includes("Qwen/Qwen3-30B-A3B"), "vLLM served model shown");
  assert(/62%/.test(subtreeText(banner)), "KV cache ~62% shown");
  const bannerText = subtreeText(banner);
  assert(bannerText.includes("3 / 1"), "vLLM running/queue 3 / 1 shown");

  const warn = el(doc, "warn-panel");
  assert(!warn.hidden, "warnings panel visible in heavy mode (dgx02 stale)");
  assert(/dgx02/.test(subtreeText(warn)), "dgx02 stale warning present");

  const grid = el(doc, "node-grid");
  assert(subtreeText(grid).includes("DGX03"), "DGX03 node card present");

  const status = el(doc, "connection-status");
  assert(/good/.test(status.className), "connection status good in heavy mode");
  console.log("PASS heavy-mode scenario");
}

(async () => {
  try {
    await testSplit();
    await testHeavy();
    console.log("\nALL FRONTEND SMOKE TESTS PASSED");
    process.exit(0);
  } catch (err) {
    console.error("\nSMOKE TEST ERROR:", err && err.stack || err);
    process.exit(1);
  }
})();
