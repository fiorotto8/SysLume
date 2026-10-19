"use strict";
// SysLume shared canvas chart engine.
//
// This is the *same* Chart engine used by the single-host "History" page
// (static/history.js), extracted verbatim so the cluster dashboard reuses the
// identical rendering, tooltips, drag-zoom and wheel-zoom behaviour.  A chart
// is bound to a shared, mutable ``range`` object ``{from, to, interval}`` and
// reports range changes back through ``range.onChange(start, end)`` so a page
// can reload history for the new window.
//
// Data fed to ``setData`` must look like ``cluster.history.service_history()``
// or one node's entry from ``node_history()["per_node"]``:
//   {timestamps:[..], sample_counts:[..], bucket_seconds, sample_count,
//    series:{<metric>:{mean:[], min:[], max:[], count:[]}}}
window.SysCharts = (() => {
  const {has, number} = M;

  function niceMax(value) {
    if (!has(value) || value <= 0) return 1;
    const exponent = 10 ** Math.floor(Math.log10(value)), fraction = value / exponent;
    return (fraction <= 1 ? 1 : fraction <= 2 ? 2 : fraction <= 2.5 ? 2.5 : fraction <= 5 ? 5 : 10) * exponent;
  }
  function axisNumber(v) { return v >= 1000 ? number(v, 0) : v >= 10 ? number(v, 0) : v >= 1 ? number(v, 1) : number(v, 2); }

  class Chart {
    constructor(def, range, container) {
      this.def = def; this.range = range;
      this.factor = def.factor || 1;
      this.hidden = new Set(); this.data = null; this.hover = null; this.drag = null;
      this.panel = document.createElement("section");
      this.panel.className = "chart-panel" + (def.wide ? " wide" : "");
      const heading = document.createElement("div"); heading.className = "panel-head";
      const title = document.createElement("h2"); title.textContent = def.title;
      const unit = document.createElement("span"); unit.className = "hint"; unit.textContent = def.unit;
      heading.append(title, unit);
      const legend = document.createElement("div"); legend.className = "legend";
      def.keys.forEach(([key, label], index) => {
        const button = document.createElement("button"); button.setAttribute("aria-pressed", "true");
        const swatch = document.createElement("span"); swatch.style.background = `var(--chart-${index + 1})`;
        button.append(swatch, document.createTextNode(label));
        button.onclick = () => { this.hidden.has(key) ? this.hidden.delete(key) : this.hidden.add(key); button.classList.toggle("disabled", this.hidden.has(key)); button.setAttribute("aria-pressed", String(!this.hidden.has(key))); this.draw(); };
        legend.append(button);
      });
      this.wrap = document.createElement("div"); this.wrap.className = "canvas-wrap";
      this.canvas = document.createElement("canvas"); this.canvas.setAttribute("role", "img");
      this.canvas.setAttribute("aria-label", `${def.title} time plot`);
      this.tooltip = document.createElement("div"); this.tooltip.className = "tooltip"; this.tooltip.hidden = true;
      this.wrap.append(this.canvas, this.tooltip);
      this.foot = document.createElement("div"); this.foot.className = "chart-footer";
      this.panel.append(heading, legend, this.wrap, this.foot);
      if (container) container.append(this.panel);
      this.ctx = this.canvas.getContext("2d");
      this.resize = new ResizeObserver(() => this.draw()); this.resize.observe(this.wrap);
      this.canvas.addEventListener("pointerdown", event => this.down(event));
      this.canvas.addEventListener("pointermove", event => this.move(event));
      this.canvas.addEventListener("pointerup", event => this.up(event));
      this.canvas.addEventListener("pointercancel", () => { this.drag = null; this.draw(); });
      this.canvas.addEventListener("pointerleave", () => { if (!this.drag) { this.hover = null; this.tooltip.hidden = true; this.draw(); } });
      this.canvas.addEventListener("dblclick", () => { if (this.range.onReset) this.range.onReset(); });
      this.canvas.addEventListener("wheel", event => {
        if (!event.ctrlKey) return;
        event.preventDefault();
        const x = this.position(event).x, width = this.width - 66;
        const fraction = Math.max(0, Math.min(1, (x - 51) / width));
        if (this.range.onZoom) this.range.onZoom(event.deltaY > 0 ? 1.25 : 0.8, fraction);
      }, {passive: false});
    }
    position(event) { const rect = this.canvas.getBoundingClientRect(); return {x: event.clientX - rect.left, y: event.clientY - rect.top}; }
    down(event) {
      if (event.button !== 0) return;
      const point = this.position(event);
      if (point.x < 51 || point.x > this.width - 15 || point.y < 12 || point.y > this.height - 32) return;
      this.drag = {start: point.x, end: point.x}; this.canvas.setPointerCapture(event.pointerId);
    }
    move(event) {
      const point = this.position(event);
      if (this.drag) { this.drag.end = Math.max(51, Math.min(this.width - 15, point.x)); this.tooltip.hidden = true; }
      else this.hover = point;
      this.draw();
    }
    up(event) {
      if (!this.drag) return;
      const {start, end} = this.drag; this.drag = null;
      if (this.canvas.hasPointerCapture(event.pointerId)) this.canvas.releasePointerCapture(event.pointerId);
      if (Math.abs(end - start) >= 8 && this.range.onChange) {
        const span = this.range.to - this.range.from, width = this.width - 66;
        this.range.onChange(this.range.from + (Math.min(start, end) - 51) / width * span,
          this.range.from + (Math.max(start, end) - 51) / width * span);
      } else this.draw();
    }
    setData(data) {
      this.data = data; this.hover = null; this.tooltip.hidden = true;
      const interval = this.range.interval || (data && data.bucket_seconds) || 15;
      this.foot.textContent = data && data.sample_count
        ? (data.bucket_seconds > interval ? "Mean lines · shaded min–max ranges" : "Recorded samples")
        : "No stored measurements in this range";
      this.draw();
    }
    draw() {
      const style = getComputedStyle(document.documentElement);
      const color = name => style.getPropertyValue(name).trim();
      const palette = [1, 2, 3, 4].map(n => color(`--chart-${n}`));
      const width = this.width = Math.max(180, this.wrap.clientWidth), height = this.height = this.wrap.clientHeight;
      const ratio = window.devicePixelRatio || 1;
      if (this.canvas.width !== Math.round(width * ratio) || this.canvas.height !== Math.round(height * ratio)) {
        this.canvas.width = Math.round(width * ratio); this.canvas.height = Math.round(height * ratio);
      }
      const ctx = this.ctx; ctx.setTransform(ratio, 0, 0, ratio, 0, 0); ctx.clearRect(0, 0, width, height); ctx.fillStyle = color("--paper"); ctx.fillRect(0, 0, width, height);
      const left = 51, right = width - 15, top = 12, bottom = height - 32;
      const from = this.range.from, to = this.range.to, span = (to - from) || 1;
      const x = t => left + (t - from) / span * (right - left);
      let maximum = 0, readings = false;
      if (this.data) this.def.keys.forEach(([key]) => {
        if (this.hidden.has(key)) return;
        const series = this.data.series[key];
        if (series) for (const value of series.max) if (has(value)) { maximum = Math.max(maximum, value / this.factor); readings = true; }
      });
      const upper = this.def.max || niceMax(maximum * 1.07);
      const y = value => bottom - value / upper * (bottom - top);
      ctx.font = "10px system-ui, sans-serif"; ctx.fillStyle = color("--muted"); ctx.lineWidth = 1;
      for (let i = 0; i <= 4; i++) {
        const value = upper * i / 4, yy = y(value);
        ctx.strokeStyle = color("--line"); ctx.beginPath(); ctx.moveTo(left, yy); ctx.lineTo(right,yy); ctx.stroke();
        ctx.textAlign = "right"; ctx.fillText(axisNumber(value), left - 9, yy + 3);
      }
      const ticks = width < 430 ? 3 : width < 750 ? 4 : 7;
      for (let i = 0; i < ticks; i++) {
        const time = from + span * i / (ticks - 1), xx = x(time), d = new Date(time * 1000);
        const label = span >= 172800 ? d.toLocaleDateString(undefined, {month: "short", day: "numeric"}) : d.toLocaleTimeString(undefined, {hour: "2-digit", minute: "2-digit", ...(span < 600 ? {second: "2-digit"} : {})});
        ctx.textAlign = i === 0 ? "left" : i === ticks - 1 ? "right" : "center";
        ctx.fillText(label, xx, bottom + 20);
      }
      if (!this.data || !this.data.timestamps.length || !readings) {
        ctx.fillStyle = color("--muted"); ctx.textAlign = "center"; ctx.font = "12px system-ui, sans-serif";
        ctx.fillText(!this.data ? "Loading measurements…" : !this.data.timestamps.length ? "No samples in this time range" : this.hidden.size === this.def.keys.length ? "All series hidden" : "No sensor readings in this time range", (left + right) / 2, (top + bottom) / 2);
        this.tooltip.hidden = true; return;
      }
      const times = this.data.timestamps;
      ctx.save(); ctx.beginPath(); ctx.rect(left, top, right - left, bottom - top); ctx.clip();
      this.def.keys.forEach(([key], index) => {
        if (this.hidden.has(key)) return;
        const series = this.data.series[key]; if (!series) return;
        const halfBucket = this.data.bucket_seconds / span * (right - left) / 2;
        ctx.fillStyle = palette[index]; ctx.globalAlpha = 0.16;
        for (let i = 0; i < times.length; i++) {
          if (!has(series.min[i]) || !has(series.max[i]) || series.min[i] === series.max[i]) continue;
          const yy = y(series.max[i] / this.factor), low = y(series.min[i] / this.factor);
          ctx.fillRect(x(times[i]) - halfBucket, yy, Math.max(1, halfBucket * 2), Math.max(1, low - yy));
        }
        ctx.globalAlpha = 1; ctx.strokeStyle = palette[index]; ctx.lineWidth = 1.65;
        ctx.beginPath(); let active = false;
        for (let i = 0; i < times.length; i++) {
          const value = series.mean[i];
          if (!has(value)) { active = false; continue; }
          const xx = x(times[i]), yy = y(value / this.factor);
          active ? ctx.lineTo(xx, yy) : ctx.moveTo(xx, yy); active = true;
        }
        ctx.stroke();
        ctx.fillStyle = palette[index];
        for (let i = 0; i < times.length; i++) if (has(series.mean[i]) &&
          (i === 0 || !has(series.mean[i - 1])) && (i === times.length - 1 || !has(series.mean[i + 1]))) {
          ctx.beginPath(); ctx.arc(x(times[i]), y(series.mean[i] / this.factor), 2.5, 0, Math.PI * 2); ctx.fill();
        }
      });
      if (this.drag) {
        ctx.fillStyle = color("--selection"); ctx.fillRect(Math.min(this.drag.start, this.drag.end), top, Math.abs(this.drag.end - this.drag.start), bottom - top);
        ctx.strokeStyle = color("--accent"); ctx.lineWidth = 1; ctx.strokeRect(Math.min(this.drag.start, this.drag.end), top, Math.abs(this.drag.end - this.drag.start), bottom - top);
      }
      if (this.hover && !this.drag && this.hover.x >= left && this.hover.x <= right && this.hover.y >= top && this.hover.y <= bottom) {
        const target = from + (this.hover.x - left) / (right - left) * span;
        let low = 0, high = times.length - 1;
        while (low < high) { const middle = Math.floor((low + high) / 2); if (times[middle] < target) low = middle + 1; else high = middle; }
        let idx = low;
        if (idx > 0 && Math.abs(times[idx - 1] - target) < Math.abs(times[idx] - target)) idx--;
        if (Math.abs(times[idx] - target) <= this.data.bucket_seconds * 1.5 && this.data.sample_counts[idx]) {
          const xx = x(times[idx]); ctx.strokeStyle = color("--muted"); ctx.setLineDash([3, 3]); ctx.beginPath(); ctx.moveTo(xx, top); ctx.lineTo(xx, bottom); ctx.stroke(); ctx.setLineDash([]);
          const lines = [M.date(times[idx])];
          this.def.keys.forEach(([key, label]) => {
            if (this.hidden.has(key)) return;
            const series = this.data.series[key], mean = series.mean[idx];
            if (!has(mean)) { lines.push(`${label}: —`); return; }
            let line = `${label}: ${number(mean / this.factor, 2)} ${this.def.unit}`;
            if (this.data.sample_counts[idx] > 1 && series.min[idx] !== series.max[idx]) line += `\n  min ${number(series.min[idx] / this.factor, 2)} · max ${number(series.max[idx] / this.factor, 2)}`;
            lines.push(line);
          });
          if (this.data.sample_counts[idx] > 1) lines.push(`${this.data.sample_counts[idx]} samples in this bucket`);
          this.tooltip.textContent = lines.join("\n"); this.tooltip.hidden = false;
          const tooltipWidth = this.tooltip.offsetWidth;
          this.tooltip.style.left = `${Math.max(0, Math.min(width - tooltipWidth, this.hover.x + 13))}px`;
          this.tooltip.style.top = `${Math.max(0, Math.min(height - this.tooltip.offsetHeight, 18))}px`;
        } else this.tooltip.hidden = true;
      } else this.tooltip.hidden = true;
      ctx.restore();
    }
  }
  return {Chart, niceMax, axisNumber};
})();
