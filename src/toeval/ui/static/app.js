// toeval console: polls /api/snapshot and renders it, and starts actions (ADR 0007).
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
let jobs = [];

const $ = (id) => document.getElementById(id);
const TOKEN = document.querySelector('meta[name="toeval-token"]').content;

// Only touch the DOM when the markup changed, so a button isn't replaced mid-click.
function setHTML(el, html) {
  if (el.dataset.html !== html) {
    el.innerHTML = html;
    el.dataset.html = html;
  }
}

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
  renderJobs();
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
  if (s.paused) return "paused";
  if (s.consecutive_failures) return "error";
  if (s.last_run && s.last_run.status === "running" && snapshot.lock && snapshot.lock.alive) return "running";
  if (s.overdue || (s.data_age_hours ?? 0) > snapshot.freshness_warn_hours) return "warn";
  return "ok";
}

function renderStreams() {
  const streams = snapshot.streams;
  if (!streams.length) {
    setHTML($("streams"), `<div class="empty">No streams ingested yet. Run <code>toeval ingest elia</code>.</div>`);
    return;
  }
  const busy = busyReason();
  setHTML($("streams"), streams.map((s) => {
    const level = streamLevel(s);
    const limit = snapshot.freshness_warn_hours;
    const fill = s.data_age_hours == null ? 0 : Math.max(4, 100 * (1 - s.data_age_hours / limit));
    const run = s.last_run;
    const runText = run
      ? `${run.status} ${relative(run.started_at)} · ${count(run.rows_fetched)} rows · ${duration(run.duration_s)}`
      : "never";
    const next = s.paused
      ? `<span class="paused-text">paused: scheduled runs skip this stream</span>`
      : s.consecutive_failures
        ? `<span class="error">retry ${relative(s.next_due)}</span> (${s.consecutive_failures} failed in a row)`
        : s.next_due ? `${relative(s.next_due)} · ${when(s.next_due)}` : "now";
    const ref = `data-source="${esc(s.source)}" data-stream="${esc(s.stream)}"`;
    return `
      <article class="card" data-level="${level}">
        <div class="card-head">
          <div class="card-title"><span class="source">${esc(s.source)} /</span> ${esc(s.stream)}</div>
          <div class="card-sub">${esc(s.schedule)}${s.settings && s.settings.customized ? ' <span class="custom" title="set in sources.toml">custom</span>' : ""}</div>
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
        <div class="card-actions">
          <button type="button" data-action="fetch" ${ref} ${busy ? `disabled title="${esc(busy)}"` : 'title="Run an incremental fetch now, whatever the schedule says"'}>Fetch now</button>
          ${s.paused
            ? `<button type="button" data-action="resume" ${ref} title="Scheduled runs pick this stream up again">Resume</button>`
            : `<button type="button" data-action="pause" ${ref} class="quiet" title="Scheduled runs skip this stream until you resume it">Pause</button>`}
          <button type="button" data-action="schedule" ${ref} class="quiet" title="Change when scheduled runs fetch this stream">Schedule…</button>
        </div>
      </article>`;
  }).join(""));
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
  const busy = busyReason();
  setHTML($("layers"), `
    <div class="layer"><div class="name">Raw</div><div class="value">${bytes(st.raw_bytes)}</div>
      <div class="detail">${count(rawFiles)} files · ${count(rawRows)} rows, every version kept</div></div>
    <div class="layer"><div class="name">Staging</div><div class="value">${bytes(st.staging_bytes)}</div>
      <div class="detail">latest version per key</div></div>
    <div class="layer"><div class="name">Marts</div><div class="value">${bytes(st.marts_bytes)}</div>
      <div class="detail">energy · 15 min and hourly</div></div>
    <div class="layer" data-level="${st.quarantine_files ? "error" : "ok"}"><div class="name">Quarantine</div>
      <div class="value">${st.quarantine_files} files</div><div class="detail">batches that failed a hard check</div></div>
    <div class="layer wide" data-level="${transformLevel}">
      <div class="layer-row">
        <div><div class="name">Transform</div>
          <div class="value">${t && t.last_success_at ? when(t.last_success_at) : "–"}</div>
          <div class="detail">${transformText}</div></div>
        <button type="button" data-action="transform" ${busy ? `disabled title="${esc(busy)}"` : 'title="Rebuild staging and marts from raw and run the quality checks"'}>Rebuild now</button>
      </div></div>`);
}

// -- actions -----------------------------------------------------------------------------------

function busyReason() {
  const job = jobs.find((j) => j.status === "running");
  if (job) return `${job.label} is running`;
  if (snapshot && snapshot.lock && snapshot.lock.alive) return "a run is in progress";
  return null;
}

async function action(path, body) {
  try {
    const response = await fetch(path, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Toeval-Token": TOKEN },
      body: JSON.stringify(body || {}),
    });
    const result = await response.json().catch(() => ({}));
    if (!response.ok) {
      const detail = typeof result.detail === "string" ? result.detail : `failed (${response.status})`;
      toast(detail, "error");
      return null;
    }
    return result;
  } catch (err) {
    toast("the console server is not reachable", "error");
    return null;
  }
}

document.addEventListener("click", async (event) => {
  const button = event.target.closest("button[data-action]");
  if (!button || button.disabled) return;
  const { action: kind, source, stream } = button.dataset;
  if (kind === "schedule") {
    openSchedule(source, stream);
    return;
  }
  button.disabled = true;
  if (kind === "fetch" || kind === "transform") {
    const job = kind === "fetch"
      ? await action("/api/actions/ingest", { source, stream })
      : await action("/api/actions/transform");
    if (job) {
      toast(`started: ${job.label}`, "ok");
      await pollJobs();
    }
  } else if (kind === "pause" || kind === "resume") {
    const done = await action("/api/actions/pause", { source, stream, paused: kind === "pause" });
    if (done) {
      toast(`${source}/${stream} ${kind === "pause" ? "paused: scheduled runs skip it" : "resumed"}`, "ok");
      const s = snapshot.streams.find((x) => x.source === source && x.stream === stream);
      if (s) s.paused = done.paused;
      poll();
    }
  }
  button.disabled = false;
  render();
});

// -- schedule editor ---------------------------------------------------------------------------

const dialog = $("schedule-dialog");
const form = $("schedule-form");
let editing = null; // the stream being edited

function openSchedule(source, stream) {
  editing = snapshot.streams.find((s) => s.source === source && s.stream === stream);
  if (!editing || !editing.settings) return;
  const st = editing.settings;
  $("sd-title").textContent = `Schedule for ${source}/${stream}`;
  $("sd-window").hidden = st.default_revision_window_days == null;
  const recheck = st.default_revision_window_days == null ? "" : ` · re-check ${st.default_revision_window_days} days`;
  $("sd-default").textContent = `Default: ${st.default_schedule_text}${recheck}.`;
  fillSchedule(st.schedule, st.revision_window_days);
  dialog.showModal();
}

function fillSchedule(schedule, windowDays) {
  const f = form.elements;
  f.kind.value = schedule.kind;
  const minutes = schedule.every_minutes ?? 60;
  const hours = minutes % 60 === 0;
  f.every.value = hours ? minutes / 60 : minutes;
  f.unit.value = hours ? "60" : "1";
  f.day.value = schedule.day ?? 1;
  f.at.value = schedule.at ?? "12:00";
  f.window.value = windowDays ?? "";
  $("sd-error").textContent = "";
  showKind();
}

function showKind() {
  const kind = form.elements.kind.value;
  for (const field of form.querySelectorAll("[data-kinds]")) {
    field.hidden = !field.dataset.kinds.split(" ").includes(kind);
  }
}

function readSchedule() {
  const f = form.elements;
  const kind = f.kind.value;
  const schedule = { kind };
  if (kind === "interval") {
    schedule.every_minutes = Number(f.every.value) * Number(f.unit.value);
    if (!Number.isInteger(schedule.every_minutes) || schedule.every_minutes < 5) {
      throw new Error("the interval must be a whole number of at least 5 minutes");
    }
  } else {
    if (!/^\d\d:\d\d$/.test(f.at.value)) throw new Error("pick a time");
    schedule.at = f.at.value;
    if (kind === "monthly") {
      schedule.day = Number(f.day.value);
      if (!Number.isInteger(schedule.day) || schedule.day < 1 || schedule.day > 28) {
        throw new Error("the day must be 1 to 28, so it exists in every month");
      }
    }
  }
  let windowDays = null;
  if (!$("sd-window").hidden && f.window.value !== "") {
    windowDays = Number(f.window.value);
    if (!Number.isInteger(windowDays) || windowDays < 1 || windowDays > 365) {
      throw new Error("re-check between 1 and 365 days");
    }
  }
  return { schedule, revision_window_days: windowDays };
}

form.addEventListener("change", (event) => { if (event.target.name === "kind") showKind(); });

form.addEventListener("click", (event) => {
  const button = event.target.closest("button[data-sd]");
  if (!button) return;
  if (button.dataset.sd === "cancel") dialog.close();
  if (button.dataset.sd === "defaults" && editing) {
    fillSchedule(editing.settings.default_schedule, editing.settings.default_revision_window_days);
  }
});

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (!editing) return;
  let body;
  try {
    body = readSchedule();
  } catch (err) {
    $("sd-error").textContent = err.message;
    return;
  }
  const { source, stream } = editing;
  const save = form.querySelector('button[type="submit"]');
  save.disabled = true;
  const done = await action("/api/actions/schedule", { source, stream, ...body });
  save.disabled = false;
  if (!done) return;
  Object.assign(editing, { schedule: done.schedule, settings: done.settings });
  dialog.close();
  toast(`${source}/${stream}: ${done.schedule}`, "ok");
  render();
  poll();
});

function toast(message, level) {
  const el = document.createElement("div");
  el.className = "toast";
  el.dataset.level = level;
  el.textContent = message;
  $("toasts").appendChild(el);
  setTimeout(() => el.classList.add("leaving"), 4500);
  setTimeout(() => el.remove(), 5000);
}

function renderJobs() {
  $("jobs-section").hidden = !jobs.length;
  setHTML($("jobs"), jobs.map((j, i) => {
    const level = j.status === "running" ? "running" : j.status === "ok" ? "success" : "failed";
    const took = j.finished_at ? span((Date.parse(j.finished_at) - Date.parse(j.started_at)) / 1000) : "";
    const ended = j.finished_at ? ` · ${j.outcome} after ${took}` : "";
    return `
      <details class="job"${i === 0 && j.status !== "ok" ? " open" : ""}>
        <summary>
          <span class="status ${level}">${esc(j.status)}</span>
          <span class="job-label">${esc(j.label)}</span>
          <span class="muted small">${when(j.started_at)}${esc(ended)}</span>
          <code class="muted small job-cmd">${esc(j.command)}</code>
        </summary>
        <pre>${esc(j.output.join("\n")) || "(no output yet)"}</pre>
      </details>`;
  }).join(""));
}

let jobsWereRunning = false;

async function pollJobs() {
  try {
    const response = await fetch("/api/jobs", { cache: "no-store" });
    jobs = (await response.json()).jobs;
  } catch (err) {
    return;
  }
  const running = jobs.some((j) => j.status === "running");
  if (jobsWereRunning && !running && jobs.length) {
    const last = jobs[0];
    toast(`${last.label}: ${last.outcome}`, last.status === "ok" ? "ok" : "error");
    poll();
  }
  jobsWereRunning = running;
  if (snapshot && snapshot.streams) render();
}

function renderQuality() {
  const q = snapshot.quality;
  $("quality-at").textContent = q.length ? `· checked ${relative(q[0].checked_at)}` : "";
  if (!q.length) {
    setHTML($("quality"), `<tr><td class="empty">No checks yet: they run after each transform.</td></tr>`);
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
  setHTML($("quality"), `
    <thead><tr><th>Check</th>${subjects.map((s) => `<th>${esc(s.split("/").pop())}</th>`).join("")}</tr></thead>
    <tbody>${checks.map((check) => `
      <tr><td class="check">${esc(check.replace(/_/g, " "))}</td>
      ${subjects.map((subject) => cell(check, subject)).join("")}</tr>`).join("")}
    </tbody>`);
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
pollJobs();
setInterval(poll, POLL_MS);
setInterval(pollJobs, 2000);
setInterval(() => { renderUpdated(); if (snapshot && snapshot.streams) { renderStreams(); renderHealth(); } }, 1000);
