// toeval console, live view: tails /api/events and animates what the pipeline really does.
// Every particle, pulse and flash stands for an event the running process wrote (ADR 0007);
// nothing moves while nothing runs. Uses `snapshot`, `esc`, `count`, `span`, `bytes`, `relative`
// and `poll` from app.js.
"use strict";

const EVENTS_MS = 1000;
const SVGNS = "http://www.w3.org/2000/svg";
const reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
const LAYERS = [
  { id: "raw", title: "raw", x: 380 },
  { id: "staging", title: "staging", x: 580 },
  { id: "marts", title: "marts", x: 760 },
];
const LAYER_W = 120;
const STREAM_X = 20;
const STREAM_W = 180;
const STREAM_H = 52;

const live = {
  cursor: null,
  streams: {}, // "elia/load" -> progress of its latest run seen in events
  transform: null,
  busy: false, // a process has emitted events and not yet said "idle"
  lastEventAt: 0,
};
let flowKeys = [];
let particles = [];
let raf = null;

// -- layout ------------------------------------------------------------------------------------

const domId = (key) => key.replace(/[^a-z0-9]/gi, "-");

function streamKeys() {
  const keys = (snapshot && snapshot.streams ? snapshot.streams : []).map((s) => `${s.source}/${s.stream}`);
  for (const key of Object.keys(live.streams)) if (!keys.includes(key)) keys.push(key);
  return keys;
}

function height(n) {
  return Math.max(260, n * (STREAM_H + 16) + 20);
}

function buildFlow() {
  const keys = streamKeys();
  if (keys.join() === flowKeys.join() && $("flow").childElementCount) return;
  flowKeys = keys;
  particles.forEach((p) => p.el.remove());
  particles = [];

  const H = height(keys.length);
  const mid = H / 2;
  const gap = H / Math.max(keys.length, 1);
  const svg = $("flow");
  svg.setAttribute("viewBox", `0 0 900 ${H}`);

  const pipes = keys.map((key, i) => {
    const y = gap * (i + 0.5);
    const x0 = STREAM_X + STREAM_W;
    const x1 = LAYERS[0].x;
    return `<path class="pipe" id="pipe-${domId(key)}" d="M${x0},${y} C${x0 + 90},${y} ${x1 - 90},${mid} ${x1},${mid}"/>`;
  });
  pipes.push(`<path class="pipe" id="pipe-raw-staging" d="M${LAYERS[0].x + LAYER_W},${mid} L${LAYERS[1].x},${mid}"/>`);
  pipes.push(`<path class="pipe" id="pipe-staging-marts" d="M${LAYERS[1].x + LAYER_W},${mid} L${LAYERS[2].x},${mid}"/>`);

  const streamNodes = keys.map((key, i) => {
    const y = gap * (i + 0.5) - STREAM_H / 2;
    const [source, stream] = key.split("/");
    return `
      <g class="node stream" id="node-${domId(key)}" transform="translate(${STREAM_X},${y})">
        <rect width="${STREAM_W}" height="${STREAM_H}" rx="10"/>
        <text x="14" y="21" class="node-title"><tspan class="node-source">${esc(source)} ·</tspan> ${esc(stream)}</text>
        <text x="14" y="40" class="node-status" id="status-${domId(key)}"></text>
      </g>`;
  });

  const layerNodes = LAYERS.map((l) => `
    <g class="node layer-node" id="node-${l.id}" transform="translate(${l.x},${mid - 60})">
      <rect width="${LAYER_W}" height="120" rx="14"/>
      <text x="${LAYER_W / 2}" y="48" class="node-title" text-anchor="middle">${l.title}</text>
      <text x="${LAYER_W / 2}" y="70" class="node-status" text-anchor="middle" id="size-${l.id}"></text>
      <text x="${LAYER_W / 2}" y="90" class="node-status" text-anchor="middle" id="note-${l.id}"></text>
    </g>`);

  svg.innerHTML = `<g>${pipes.join("")}</g><g>${streamNodes.join("")}${layerNodes.join("")}</g><g id="particles"></g><g id="labels"></g>`;
  renderFlow();
}

// -- live state from events ----------------------------------------------------------------------

function active() {
  // Trust "busy" while the lock holder is alive, or briefly after an event before the next
  // snapshot shows the lock. A killed process never says "idle", but its lock is dead.
  if (!live.busy) return false;
  const lock = snapshot && snapshot.lock;
  if (lock && lock.alive) return true;
  return Date.now() - live.lastEventAt < 15000;
}

function handle(e, animate) {
  const key = e.source && e.stream ? `${e.source}/${e.stream}` : null;
  let s = key ? live.streams[key] : null;
  if (e.type !== "log_started") live.lastEventAt = animate ? Date.now() : Date.parse(e.at) || 0;
  switch (e.type) {
    case "run_started":
      live.busy = true;
      live.streams[key] = { phase: "starting", mode: e.mode, deep: e.deep, rows: 0, files: 0, index: 0, total: null, since: e.at };
      break;
    case "planned":
      if (s) s.total = e.requests;
      break;
    case "request_started":
      live.busy = true;
      if (s) Object.assign(s, { phase: "fetching", index: e.index, total: e.total, requestSince: e.at });
      break;
    case "request_done":
      if (!s) break;
      s.phase = "received";
      s.rows += e.rows;
      if (e.outcome === "written") s.files += 1;
      if (animate) arrive(key, e);
      break;
    case "run_finished":
      if (s) Object.assign(s, { phase: e.status === "success" ? "done" : "failed", duration: e.duration_s, finishedAt: e.at, error: e.error });
      if (animate) flash(`node-${domId(key)}`, e.status === "success" ? "ok" : "error");
      break;
    case "transform_started":
      live.busy = true;
      live.transform = { phase: "running", built: 0, total: e.models, since: e.at };
      break;
    case "model_built":
      if (live.transform) {
        Object.assign(live.transform, { built: live.transform.built + 1, current: e.model });
        if (e.layer === "marts") live.transform.marts = true;
      }
      if (animate) {
        const toMarts = e.layer === "marts";
        spawn($(toMarts ? "pipe-staging-marts" : "pipe-raw-staging"), dots(e.rows), "ok", () => {
          flash(toMarts ? "node-marts" : "node-staging", "ok");
          label(toMarts ? "marts" : "staging", e.model.replace(/^(stg|mart)_/, ""));
        });
      }
      break;
    case "transform_finished":
      if (live.transform) Object.assign(live.transform, { phase: e.status, finishedAt: e.at, duration: e.duration_s, warnings: e.warnings, error: e.error });
      if (animate) for (const l of LAYERS) flash(`node-${l.id}`, e.status === "success" ? "ok" : "error");
      break;
    case "idle":
    case "orphans_aborted":
      live.busy = e.type === "orphans_aborted";
      for (const st of Object.values(live.streams)) {
        if (st.phase === "fetching" || st.phase === "starting" || st.phase === "received") st.phase = "stopped";
      }
      if (live.transform && live.transform.phase === "running") live.transform.phase = "stopped";
      if (animate && e.type === "idle") setTimeout(poll, 400); // the state DB is free again
      break;
  }
  if (key && !live.streams[key]) live.streams[key] = { phase: "idle", rows: 0, files: 0 };
  feed(e, animate);
}

// -- animation ---------------------------------------------------------------------------------

function dots(rows) {
  return Math.max(1, Math.min(14, Math.round(Math.log10((rows || 0) + 1) * 2.5)));
}

function arrive(key, e) {
  const pipe = $(`pipe-${domId(key)}`);
  if (e.outcome === "written") {
    spawn(pipe, dots(e.rows), "ok", () => { flash("node-raw", "ok"); label("raw", `+${count(e.rows)} rows`); });
  } else if (e.outcome === "unchanged") {
    spawn(pipe, Math.min(4, dots(e.rows)), "grey", null, 0.55); // fades out: nothing new stored
  } else if (e.outcome === "quarantined") {
    spawn(pipe, dots(e.rows), "error", () => flash("node-raw", "error"), 0.8);
  } else {
    flash(`node-${domId(key)}`, "grey"); // 304 not modified: nothing came over the wire
  }
}

function spawn(path, n, kind, onArrive, travel = 1) {
  if (!path || reducedMotion || document.hidden) {
    if (onArrive) onArrive();
    return;
  }
  const group = $("particles");
  const length = path.getTotalLength();
  const start = performance.now();
  for (let i = 0; i < n && particles.length < 160; i++) {
    const el = document.createElementNS(SVGNS, "circle");
    el.setAttribute("r", (2.2 + Math.random() * 1.6).toFixed(1));
    el.setAttribute("class", `particle ${kind}`);
    el.setAttribute("opacity", "0");
    group.appendChild(el);
    particles.push({
      el, path, length, travel,
      t0: start + i * 90,
      duration: 1400 + Math.random() * 500,
      wobble: (Math.random() - 0.5) * 10,
      done: i === n - 1 ? onArrive : null,
    });
  }
  if (!raf) raf = requestAnimationFrame(tick);
}

function tick(now) {
  for (let i = particles.length - 1; i >= 0; i--) {
    const p = particles[i];
    const t = (now - p.t0) / p.duration;
    if (t < 0) continue;
    if (t >= p.travel) {
      p.el.remove();
      particles.splice(i, 1);
      if (p.done) p.done();
      continue;
    }
    const e = t < 0.5 ? 2 * t * t : 1 - (-2 * t + 2) ** 2 / 2;
    const pt = p.path.getPointAtLength(e * p.length);
    p.el.setAttribute("cx", pt.x.toFixed(1));
    p.el.setAttribute("cy", (pt.y + p.wobble * Math.sin(Math.PI * e)).toFixed(1));
    const fade = p.travel < 1 ? 1 - t / p.travel : Math.min(1, t * 8);
    p.el.setAttribute("opacity", fade.toFixed(2));
  }
  raf = particles.length ? requestAnimationFrame(tick) : null;
}

function flash(id, kind) {
  const node = $(id);
  if (!node || reducedMotion) return;
  node.classList.remove("flash-ok", "flash-error", "flash-grey");
  void node.getBoundingClientRect(); // restart the animation
  node.classList.add(`flash-${kind}`);
}

function label(layerId, text) {
  if (reducedMotion || document.hidden) return;
  const layer = LAYERS.find((l) => l.id === layerId);
  const node = $(`node-${layerId}`);
  if (!layer || !node) return;
  const y = node.transform.baseVal.consolidate().matrix.f;
  const el = document.createElementNS(SVGNS, "text");
  el.setAttribute("x", layer.x + LAYER_W / 2);
  el.setAttribute("y", y - 8);
  el.setAttribute("text-anchor", "middle");
  el.setAttribute("class", "float-label");
  el.textContent = text;
  $("labels").appendChild(el);
  el.addEventListener("animationend", () => el.remove());
}

// -- rendering ---------------------------------------------------------------------------------

function streamStatus(key) {
  const s = live.streams[key];
  const now = Date.now();
  const busy = active();
  if (s && busy && s.phase === "starting") return ["running", s.deep ? "starting (deep re-fetch)…" : "starting…"];
  if (s && busy && (s.phase === "fetching" || s.phase === "received")) {
    const of = s.total ? `${s.index}/${s.total}` : `${s.index}`;
    const waiting = s.phase === "fetching" && s.requestSince ? ` · ${span((now - Date.parse(s.requestSince)) / 1000)}` : "";
    return ["running", `request ${of} · ${count(s.rows)} rows${waiting}`];
  }
  if (s && s.finishedAt && now - Date.parse(s.finishedAt) < 30 * 60 * 1000) {
    if (s.phase === "failed") return ["error", `failed ${relative(s.finishedAt)}`];
    if (s.phase === "done") return ["ok", `done ${relative(s.finishedAt)} · ${count(s.rows)} rows`];
  }
  const info = snapshot && snapshot.streams && snapshot.streams.find((x) => `${x.source}/${x.stream}` === key);
  if (info && info.paused) return ["", "paused · scheduled runs skip it"];
  if (info && info.consecutive_failures) return ["error", `failing · retry ${relative(info.next_due)}`];
  if (info) return ["", `idle · next ${info.next_due ? relative(info.next_due) : "now"}`];
  return ["", "idle"];
}

function renderFlow() {
  if (!$("flow").childElementCount) return;
  const busy = active();
  for (const key of flowKeys) {
    const [level, text] = streamStatus(key);
    const status = $(`status-${domId(key)}`);
    if (status) status.textContent = text;
    const node = $(`node-${domId(key)}`);
    if (node) node.dataset.level = level;
    const s = live.streams[key];
    const pipe = $(`pipe-${domId(key)}`);
    if (pipe) pipe.classList.toggle("active", busy && !!s && s.phase === "fetching");
  }

  const t = live.transform;
  const transforming = busy && t && t.phase === "running";
  $("pipe-raw-staging").classList.toggle("active", !!transforming);
  $("pipe-staging-marts").classList.toggle("active", !!transforming && !!t.marts);
  for (const l of LAYERS) $(`node-${l.id}`).classList.toggle("active", !!transforming && l.id !== "raw");
  const anyFetching = busy && Object.values(live.streams).some((s) => s.phase === "fetching");
  $("node-raw").classList.toggle("active", anyFetching);

  const st = snapshot && snapshot.storage;
  if (st) {
    $("size-raw").textContent = bytes(st.raw_bytes);
    $("size-staging").textContent = bytes(st.staging_bytes);
    $("size-marts").textContent = bytes(st.marts_bytes);
  }
  $("note-raw").textContent = anyFetching ? "receiving…" : "every version";
  $("note-staging").textContent = transforming ? `building ${t.built}/${t.total}` : "latest per key";
  $("note-marts").textContent = transforming && t.marts ? "building…" : "energy · weather";

  const state = $("flow-state");
  if (busy) {
    state.textContent = "· live: a run is in progress";
    state.className = "small live-on";
  } else {
    state.textContent = "· idle: animations show real pipeline events only";
    state.className = "muted small";
  }
}

// -- feed --------------------------------------------------------------------------------------

const feedTime = new Intl.DateTimeFormat("en-GB", { timeZone: "Europe/Brussels", hour: "2-digit", minute: "2-digit", second: "2-digit" });
const feedDay = new Intl.DateTimeFormat("en-GB", { timeZone: "Europe/Brussels", day: "numeric", month: "short" });

function describe(e) {
  const who = e.source ? `${e.source}/${e.stream}` : "";
  switch (e.type) {
    case "run_started":
      return ["running", who, `${e.mode} run started${e.deep ? " (deep re-fetch)" : ""}`];
    case "planned":
      return ["", who, `${e.requests} request${e.requests === 1 ? "" : "s"} planned`];
    case "request_done": {
      const what = {
        written: "stored in raw",
        unchanged: "unchanged, not stored again",
        not_modified: "not modified (304)",
        quarantined: "failed a hard check: quarantined",
      }[e.outcome] || e.outcome;
      return [e.outcome === "quarantined" ? "error" : e.outcome === "written" ? "ok" : "", who,
        `request ${e.index}/${e.total} · ${count(e.rows)} rows · ${what}`];
    }
    case "run_finished":
      return e.status === "success"
        ? ["ok", who, `done in ${span(e.duration_s)} · ${count(e.rows)} rows · ${e.files} file${e.files === 1 ? "" : "s"}`]
        : ["error", who, `failed after ${span(e.duration_s)}: ${e.error}`];
    case "transform_started":
      return ["running", "transform", `rebuilding ${e.models} models from raw`];
    case "model_built":
      return ["ok", "transform", `${e.model} · ${count(e.rows)} rows`];
    case "model_skipped":
      return ["", "transform", `${e.model} skipped (no input yet)`];
    case "transform_finished":
      return e.status === "success"
        ? [e.warnings ? "warn" : "ok", "transform", `done in ${span(e.duration_s)} · ${e.checks} checks, ${e.warnings} warning${e.warnings === 1 ? "" : "s"}`]
        : ["error", "transform", `failed: ${e.error}`];
    case "orphans_aborted":
      return ["warn", "pipeline", `marked ${e.count} run(s) of a killed process as failed`];
    case "idle":
      return ["", "pipeline", "finished; state database released"];
    default:
      return null; // request_started is shown on the diagram, not in the feed
  }
}

function feed(e, animate) {
  const text = e.type === "request_started" ? null : describe(e);
  if (!text) return;
  const [level, who, what] = text;
  const at = new Date(e.at);
  const today = feedDay.format(at) === feedDay.format(new Date());
  const li = document.createElement("li");
  li.dataset.level = level;
  if (animate && !reducedMotion) li.classList.add("new");
  li.innerHTML = `<time title="${esc(at.toISOString())}">${today ? "" : `${feedDay.format(at)} `}${feedTime.format(at)}</time>` +
    `<span class="who">${esc(who)}</span><span class="what">${esc(what)}</span>`;
  const list = $("feed");
  list.prepend(li);
  while (list.childElementCount > 60) list.lastElementChild.remove();
  $("feed-empty").hidden = true;
}

// -- polling -----------------------------------------------------------------------------------

async function pollEvents() {
  try {
    const url = live.cursor === null ? "/api/events" : `/api/events?cursor=${encodeURIComponent(live.cursor)}`;
    const response = await fetch(url, { cache: "no-store" });
    if (!response.ok) return;
    const body = await response.json();
    const first = live.cursor === null;
    live.cursor = body.cursor;
    for (const e of body.events) handle(e, !first); // history is listed, not animated
    buildFlow();
    renderFlow();
  } catch (err) {
    // app.js reports a lost connection
  }
}

pollEvents();
setInterval(pollEvents, EVENTS_MS);
setInterval(() => { buildFlow(); renderFlow(); }, 1000);
