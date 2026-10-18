// toeval console: polls /api/snapshot and renders it. Read-only (ADR 0007).
"use strict";

const POLL_MS = 4000;
const TZ = "Europe/Brussels";
const LEVEL_TEXT = {
  ok: "All systems normal",
  running: "Ingesting…",
  warn: "Needs attention",
  error: "Something is broken",
};

let snapshot = null;
let fetchedAt = null;
let connectionError = null;

const $ = (id) => document.getElementById(id);

function esc(value) {
  return String(value ?? "").replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  })[c]);
}

// -- formatting --------------------------------------------------------------------------------

const dateFmt = new Intl.DateTimeFormat("en-GB", {
  timeZone: TZ, day: "numeric", month: "short", hour: "2-digit", minute: "2-digit",
});
const timeFmt = new Intl.DateTimeFormat("en-GB", { timeZone: TZ, hour: "2-digit", minute: "2-digit" });

function when(iso) {
  if (!iso) return "–";
  const d = new Date(iso);
  const sameDay = d.toLocaleDateString("en-GB", { timeZone: TZ }) ===
    new Date().toLocaleDateString("en-GB", { timeZone: TZ });
  return sameDay ? `today ${timeFmt.format(d)}` : dateFmt.format(d);
}

function span(seconds) {
  const s = Math.abs(Math.round(seconds));
  if (s < 60) return `${s} s`;
  const m = Math.round(s / 60);
  if (m < 60) return `${m} min`;
  const h = Math.floor(m / 60);
  if (h < 48) return m % 60 && h < 10 ? `${h} h ${m % 60} min` : `${h} h`;
  return `${Math.round(h / 24)} days`;
}

function relative(iso) {
  if (!iso) return "–";
  const seconds = (new Date(iso) - Date.now()) / 1000;
  return seconds >= 0 ? `in ${span(seconds)}` : `${span(seconds)} ago`;
}

function count(n) {
  if (n == null) return "–";
  if (n >= 1e6) return `${(n / 1e6).toFixed(n >= 1e7 ? 0 : 1)} M`;
  if (n >= 1e4) return `${Math.round(n / 1e3)} k`;
  return n.toLocaleString("en-GB");
}

function bytes(n) {
  if (!n) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  const i = Math.min(units.length - 1, Math.floor(Math.log(n) / Math.log(1024)));
  return `${(n / 1024 ** i).toFixed(i >= 2 ? 0 : 1)} ${units[i]}`;
}

function duration(seconds) {
  return seconds == null ? "–" : span(seconds);
}

// -- rendering ---------------------------------------------------------------------------------

function render() {
  renderHealth();
  renderUpdated();
  if (!snapshot || !snapshot.streams) return;
  renderBanner();
  renderStreams();
  renderLayers();
  renderQuality();
  renderRuns();
  $("data-dir").textContent = snapshot.data_dir || "";
}

function renderHealth() {
  const box = $("health");
  if (connectionError) {
    box.dataset.level = "error";
    $("health-label").textContent = "Console can't reach the server";
    $("reasons").innerHTML = `<li data-level="error">${esc(connectionError)}</li>`;
    setFavicon("error");
    return;
  }
  if (!snapshot || !snapshot.health) return;
  const { level, reasons } = snapshot.health;
  box.dataset.level = level;
  $("health-label").textContent = LEVEL_TEXT[level] || level;
  $("reasons").innerHTML = reasons.slice(0, 4)
    .map((r) => `<li data-level="${esc(r.level)}" title="${esc(r.message)}">${esc(r.message)}</li>`)
    .join("") + (reasons.length > 4 ? `<li>+ ${reasons.length - 4} more</li>` : "");
  setFavicon(level);
  document.title = level === "ok" ? "toeval console" : `(${LEVEL_TEXT[level]}) toeval console`;

  const task = snapshot.scheduler;
  const chip = $("scheduler");
  if (!task) {
    chip.dataset.level = "";
    chip.textContent = "scheduler: unknown";
  } else if (!task.installed) {
    chip.dataset.level = "error";
    chip.textContent = "scheduler: not installed";
  } else {
    chip.dataset.level = task.state === "Disabled" ? "error" : task.result_level;
    chip.textContent = `scheduler ${task.state.toLowerCase()} · ${task.result} · next ${relative(task.next_run_at)}`;
  }
}

function renderUpdated() {
  if (!fetchedAt) return;
  const age = (Date.now() - fetchedAt) / 1000;
  $("updated").textContent = connectionError ? `last contact ${span(age)} ago` : `updated ${span(age)} ago`;
}

function renderBanner() {
  const banner = $("banner");
  if (snapshot.state_available === false) {
    banner.hidden = false;
    banner.textContent =
      `A run is using the state database, so details are from ${when(snapshot.generated_at)}. ` +
      "They refresh as soon as it finishes.";
  } else if (snapshot.note) {
    banner.hidden = false;
    banner.textContent = snapshot.note;
  } else {
    banner.hidden = true;
  }
}

function streamLevel(s) {
  if (s.consecutive_failures) return "error";
  if (s.last_run && s.last_run.status === "running" && snapshot.lock && snapshot.lock.alive) return "running";
  if (s.overdue || (s.data_age_hours ?? 0) > snapshot.freshness_warn_hours) return "warn";
  return "ok";
}

function renderStreams() {
  const streams = snapshot.streams;
  if (!streams.length) {
    $("streams").innerHTML = `<div class="empty">No streams ingested yet. Run <code>toeval ingest elia</code>.</div>`;
    return;
  }
  $("streams").innerHTML = streams.map((s) => {
    const level = streamLevel(s);
    const limit = snapshot.freshness_warn_hours;
    const fill = s.data_age_hours == null ? 0 : Math.max(4, 100 * (1 - s.data_age_hours / limit));
    const run = s.last_run;
    const runText = run
      ? `${run.status} ${relative(run.started_at)} · ${count(run.rows_fetched)} rows · ${duration(run.duration_s)}`
      : "never";
    const next = s.consecutive_failures
      ? `<span class="error">retry ${relative(s.next_due)}</span> (${s.consecutive_failures} failed in a row)`
      : s.next_due ? `${relative(s.next_due)} · ${when(s.next_due)}` : "now";
    return `
      <article class="card" data-level="${level}">
        <div class="card-head">
          <div class="card-title"><span class="source">${esc(s.source)} /</span> ${esc(s.stream)}</div>
          <div class="card-sub">${esc(s.schedule)}</div>
        </div>
        <div class="freshness">
          <div class="big">${s.data_until ? `data until ${when(s.data_until)}` : "no data yet"}</div>
          <div class="card-sub">${s.data_age_hours == null ? "" : `latest measured value is ${span(s.data_age_hours * 3600)} old`}</div>
          <div class="bar" title="age vs. the ${limit} h freshness limit"><i style="width:${fill}%"></i></div>
        </div>
        <dl class="facts">
          <dt>Last run</dt><dd${run && run.status === "failed" ? ' class="error"' : ""}>${esc(runText)}</dd>
          <dt>Next run</dt><dd>${next}</dd>
          <dt>Raw</dt><dd>${count(s.raw_files)} files · ${count(s.raw_rows)} rows</dd>
          ${run && run.error ? `<dt>Error</dt><dd class="error">${esc(run.error)}</dd>` : ""}
        </dl>
      </article>`;
  }).join("");
}

function renderLayers() {
  const st = snapshot.storage;
  const t = snapshot.transform;
  const rawRows = snapshot.streams.reduce((n, s) => n + s.raw_rows, 0);
  const rawFiles = snapshot.streams.reduce((n, s) => n + s.raw_files, 0);
  const transformLevel = t && t.consecutive_failures ? "error" : "ok";
  const transformText = !t || !t.last_attempt_at
    ? "never run"
    : t.consecutive_failures
      ? `${t.consecutive_failures} failed attempt(s) · ${esc(t.last_error)}`
      : `last rebuild ${relative(t.last_success_at)}`;
  $("layers").innerHTML = `
    <div class="layer"><div class="name">Raw</div><div class="value">${bytes(st.raw_bytes)}</div>
      <div class="detail">${count(rawFiles)} files · ${count(rawRows)} rows, every version kept</div></div>
    <div class="layer"><div class="name">Staging</div><div class="value">${bytes(st.staging_bytes)}</div>
      <div class="detail">latest version per key</div></div>
    <div class="layer"><div class="name">Marts</div><div class="value">${bytes(st.marts_bytes)}</div>
      <div class="detail">energy · 15 min and hourly</div></div>
    <div class="layer" data-level="${st.quarantine_files ? "error" : "ok"}"><div class="name">Quarantine</div>
      <div class="value">${st.quarantine_files} files</div><div class="detail">batches that failed a hard check</div></div>
    <div class="layer wide" data-level="${transformLevel}"><div class="name">Transform</div>
      <div class="value">${t && t.last_success_at ? when(t.last_success_at) : "–"}</div>
      <div class="detail">${transformText}</div></div>`;
}

function renderQuality() {
  const q = snapshot.quality;
  $("quality-at").textContent = q.length ? `· checked ${relative(q[0].checked_at)}` : "";
  if (!q.length) {
    $("quality").innerHTML = `<tr><td class="empty">No checks yet: they run after each transform.</td></tr>`;
    return;
  }
  // One row per check, one column per subject; details in the tooltip.
  const subjects = [...new Set(q.map((c) => c.subject))].sort();
  const checks = [...new Set(q.map((c) => c.check))].sort();
  const cell = (check, subject) => {
    const c = q.find((x) => x.check === check && x.subject === subject);
    if (!c) return `<td class="na">·</td>`;
    const value = c.value == null ? "n/a" : `${Number(c.value.toFixed(1))} ${esc(c.unit)}`;
    return `<td class="${c.ok ? "ok" : "warn"}" title="${esc(c.details)}"><span class="tick"></span>${value}</td>`;
  };
  $("quality").innerHTML = `
    <thead><tr><th>Check</th>${subjects.map((s) => `<th>${esc(s.split("/").pop())}</th>`).join("")}</tr></thead>
    <tbody>${checks.map((check) => `
      <tr><td class="check">${esc(check.replace(/_/g, " "))}</td>
      ${subjects.map((subject) => cell(check, subject)).join("")}</tr>`).join("")}
    </tbody>`;
}

function renderRuns() {
  const runs = snapshot.recent_runs;
  $("runs").innerHTML = runs.length ? runs.map((r) => `
    <tr>
      <td>${when(r.started_at)}</td>
      <td>${esc(r.source)}/${esc(r.stream)}</td>
      <td>${esc(r.mode)}</td>
      <td><span class="status ${esc(r.status)}">${esc(r.status)}</span></td>
      <td class="num">${count(r.requests)}</td>
      <td class="num">${count(r.rows_fetched)}</td>
      <td class="num">${count(r.files_written)}</td>
      <td class="num">${duration(r.duration_s)}</td>
      <td class="detail${r.error ? " error" : ""}">${esc(r.error || r.watermark_after || "")}</td>
    </tr>`).join("") : `<tr><td colspan="9" class="empty">No runs yet.</td></tr>`;
}

function setFavicon(level) {
  const colors = { ok: "%2336d399", running: "%234ea8ff", warn: "%23f5b942", error: "%23ff5d6c" };
  $("favicon").href = "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E" +
    `%3Ccircle cx='16' cy='16' r='12' fill='${colors[level] || "%238a93b8"}'/%3E%3C/svg%3E`;
}

// -- polling -----------------------------------------------------------------------------------

async function poll() {
  try {
    const response = await fetch("/api/snapshot", { cache: "no-store" });
    const body = await response.json();
    if (response.ok || body.streams) {
      snapshot = body;
      connectionError = null;
    } else if (body.state_available === false) {
      connectionError = null;
      snapshot = snapshot || { health: { level: "running", reasons: [{ level: "running", message: "a run holds the state database; waiting for it" }] } };
    }
    fetchedAt = Date.now();
  } catch (err) {
    connectionError = "is `toeval ui` still running?";
  }
  render();
}

poll();
setInterval(poll, POLL_MS);
setInterval(() => { renderUpdated(); if (snapshot && snapshot.streams) { renderStreams(); renderHealth(); } }, 1000);
