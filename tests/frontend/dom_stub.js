"use strict";
// Minimal DOM + canvas 2d stub, just enough to evaluate the browser
// frontend modules (common.js, charts.js, cluster-render.js,
// cluster-history.js, cluster.js) in Node and assert on the produced tree.
// className and classList share one Set so both stay consistent.
function make2dCtx() {
  const target = {canvas: null, measureText: () => ({width: 0})};
  return new Proxy(target, {
    get(t, p) { if (p in t) return t[p]; return function () {}; },
    set(t, p, v) { t[p] = v; return true; },
  });
}
class Element {
  constructor(tag) {
    this.nodeType = 1; this.tagName = (tag || "div").toUpperCase();
    this.children = []; this.parent = null; this._text = "";
    this.attributes = {}; this.dataset = {}; this.style = {};
    this.hidden = false; this.value = ""; this.checked = false;
    this.clientWidth = 600; this.clientHeight = 240;
    this.offsetWidth = 120; this.offsetHeight = 24;
    this._classes = new Set(); this._innerHTML = ""; this._listeners = {};
    this.classList = {
      add: (...c) => { c.forEach(x => x && this._classes.add(x)); },
      remove: (...c) => { c.forEach(x => this._classes.delete(x)); },
      toggle: (c, force) => { const want = force === undefined ? !this._classes.has(c) : force; want ? this._classes.add(c) : this._classes.delete(c); return want; },
      contains: (c) => this._classes.has(c),
    };
  }
  get className() { return Array.from(this._classes).join(" "); }
  set className(v) { this._classes.clear(); String(v || "").trim().split(/\s+/).forEach(t => t && this._classes.add(t)); }
  appendChild(node) { this._add(node); return node; }
  append(...nodes) { nodes.forEach(n => this._add(n)); }
  _add(node) {
    if (typeof node === "string" || typeof node === "number") node = new TextNode(node);
    if (node == null) return;
    if (node.parent) node.parent.children = node.parent.children.filter(c => c !== node);
    node.parent = this; this.children.push(node);
  }
  remove() { if (this.parent) this.parent.children = this.parent.children.filter(c => c !== this); }
  addEventListener(type, fn) { (this._listeners[type] = this._listeners[type] || []).push(fn); }
  dispatch(type, ev) { (this._listeners[type] || []).forEach(f => f(ev || {})); }
  setAttribute(k, v) { this.attributes[k] = String(v); if (k === "id") activeDoc && activeDoc._registry.set(String(v), this); }
  getAttribute(k) { return k in this.attributes ? this.attributes[k] : null; }
  hasAttribute(k) { return k in this.attributes; }
  set id(v) { this.attributes.id = v; if (activeDoc) activeDoc._registry.set(v, this); }
  get id() { return this.attributes.id; }
  set innerHTML(v) { this._innerHTML = String(v); this.children = []; this._text = ""; }
  get innerHTML() { return this._innerHTML; }
  set textContent(v) { this._innerHTML = ""; this.children = []; this._text = String(v == null ? "" : v); }
  get textContent() { return this.children.length ? this.children.map(subtreeText).join("") : this._text; }
  getBoundingClientRect() { return {left: 0, top: 0, right: 600, bottom: 240, width: 600, height: 240, x: 0, y: 0}; }
  getContext() { if (!this._ctx) this._ctx = make2dCtx(); return this._ctx; }
  setPointerCapture() {} releasePointerCapture() {} hasPointerCapture() { return false; }
  focus() {}
}
class TextNode { constructor(t) { this.nodeType = 3; this._text = String(t == null ? "" : t); this.parent = null; } }
function subtreeText(node) {
  if (!node) return "";
  if (node.nodeType === 3) return node._text;
  return (node._text || "") + (node.children || []).map(subtreeText).join("");
}
let activeDoc = null;
function makeDocument(rootEl) {
  const registry = new Map();
  const doc = {
    _registry: registry, documentElement: rootEl, body: rootEl, hidden: false,
    createElement: tag => new Element(tag),
    createTextNode: t => new TextNode(t),
    getElementById: id => registry.get(id) || null,
    addEventListener() {}, removeEventListener() {},
    querySelector: () => null, querySelectorAll: () => [],
  };
  activeDoc = doc;
  return doc;
}
module.exports = {Element, TextNode, subtreeText, makeDocument, make2dCtx};
