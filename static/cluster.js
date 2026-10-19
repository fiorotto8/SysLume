"use strict";
// SysLume Cluster — orchestrator.
// Owns the 15s polling of /api/cluster/current, adapts it into the shared
// common.js status payload (which drives #connection-status / #last-read),
// and delegates all DOM painting to SysClusterRender and SysClusterHistory.
// The single 15s sampling loop lives in the backend runtime; this only reads.
(() => {
  const {byId, get, receive, offline, notice, date} = M;
  const POLL_MS = 15000;
  let controller = null;

  function adapt(current) {
    const snap = current.snapshot || {};
    return {
      meta: {hostname: "SysLume Cluster · DGX01", model: "3-node DGX cluster",
             interval_seconds: 15},
      sample: {timestamp: snap.timestamp},
      age_seconds: current.snapshot_age_seconds,
      collector_error: current.available ? null : (current.reason || "unavailable"),
    };
  }

  async function refresh() {
    if (controller) controller.abort();
    controller = new AbortController();
    const my = controller;
    try {
      const current = await get("/api/cluster/current", my.signal);
      if (my !== controller) return;
      receive(adapt(current));
      const snap = current.snapshot;
      SysClusterRender.renderBanner(snap && snap.cluster, current.service, snap);
      SysClusterRender.renderNodes(snap && snap.nodes);
      SysClusterRender.renderWarnings(snap && snap.warnings);
      const sub = byId("cluster-subtitle");
      if (sub) sub.textContent = current.available
        ? `updated ${date(current.snapshot_timestamp)} · ${Math.round(current.snapshot_age_seconds)}s old`
        : "waiting for the first reading";
      notice(current.available ? "" : (current.reason || "no sample yet"));
    } catch (error) {
      if (error.name === "AbortError") return;
      offline();
      notice(`Cannot reach the cluster monitor: ${error.message}`);
    }
  }

  function init() {
    if (window.SysClusterHistory) SysClusterHistory.init();
    refresh();
    setInterval(refresh, POLL_MS);
    document.addEventListener("visibilitychange", () => { if (!document.hidden) refresh(); });
  }

  // Deferred script: the document is parsed, run immediately.
  init();
})();
