"""What the admin console shows: one JSON-ready snapshot of the pipeline's state.

Read-only (ADR 0007): the state DB is opened read-only for the duration of one
snapshot. While an ingest holds it (DuckDB allows one writing process), the snapshot says so and
the app keeps serving the last readable one.
"""

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb

from toeval.config import Settings
from toeval.connectors.spec import DailySchedule, IntervalSchedule, MonthlySchedule, StreamSpec
from toeval.lock import pid_alive
from toeval.pipeline import Pipeline
from toeval.state import SCHEMA_VERSION, StateStore
from toeval.ui.scheduler import TaskInfo

# A stream whose next run is this far in the past while nothing runs: the scheduler isn't working.
OVERDUE_AFTER = timedelta(minutes=30)

Json = dict[str, Any]


class StateBusyError(RuntimeError):
    """A run holds the state DB; try again after it finishes."""


@dataclass(frozen=True)
class Reason:
    level: str  # "error" | "warn" | "running"
    message: str


LEVELS = ("ok", "running", "warn", "error")  # increasing severity


def build_snapshot(settings: Settings, *, now: datetime, task: TaskInfo | None) -> Json:
    lock = read_lock(settings.lock_path)
    if not settings.state_db_path.exists():
        return _empty(settings, now, lock, task)
    try:
        store = StateStore(settings.state_db_path, read_only=True)
    except duckdb.IOException as exc:
        raise StateBusyError(str(exc)) from None
    with store:
        if store.schema_version() < SCHEMA_VERSION:
            return _empty(settings, now, lock, task, note="state DB not upgraded yet")
        return _snapshot(settings, store, now, lock, task)


def _snapshot(
    settings: Settings, store: StateStore, now: datetime, lock: Json | None, task: TaskInfo | None
) -> Json:
    pipeline = Pipeline(settings, store, clock=lambda: now)
    raw_stats = store.raw_stats()
    runs = store.recent_runs(200)
    last_run: dict[tuple[str, str], dict[str, Any]] = {}
    for run in runs:  # newest first
        last_run.setdefault((str(run["source"]), str(run["stream"])), run)

    streams = []
    for s in pipeline.status():
        spec = pipeline.spec(s.source).stream(s.stream)
        wm = s.state.watermark
        data_until = wm.value if wm is not None and isinstance(wm.value, datetime) else None
        files, rows = raw_stats.get((s.source, s.stream), (0, 0))
        streams.append(
            {
                "source": s.source,
                "stream": s.stream,
                "schedule": describe_schedule(spec),
                "paused": spec.paused,
                "settings": stream_settings(spec, pipeline.default_spec(s.source).stream(s.stream)),
                "watermark": wm.serialize() if wm else None,
                "data_until": _iso(data_until),
                "data_age_hours": _hours(now - data_until) if data_until else None,
                # The freshness check's threshold, so the card's bar agrees with it.
                "freshness_limit_hours": _hours(spec.freshness_limit),
                "last_success_at": _iso(s.state.last_success_at),
                "consecutive_failures": s.state.consecutive_failures,
                "last_failure_at": _iso(s.state.last_failure_at),
                "next_due": _iso(s.next_due),
                "overdue": not spec.paused and _overdue(s.next_due, now, lock),
                "raw_files": files,
                "raw_rows": rows,
                "last_run": _run(last_run.get((s.source, s.stream))),
            }
        )

    t = store.transform_status()
    transform = {
        "last_success_at": _iso(t.last_success_started_at),
        "last_attempt_at": _iso(t.last_attempt_at),
        "last_status": t.last_status,
        "last_error": t.last_error,
        "consecutive_failures": t.consecutive_failures,
    }
    quality = [
        {
            "check": q.check,
            "subject": q.subject,
            "ok": q.ok,
            "value": q.value,
            "unit": q.unit,
            "details": q.details,
            "checked_at": _iso(q.checked_at),
        }
        for q in store.latest_quality_results()
    ]
    snapshot = {
        "generated_at": _iso(now),
        "state_available": True,
        "note": None,
        "lock": lock,
        "scheduler": task.as_json() if task else None,
        "streams": streams,
        "transform": transform,
        "quality": quality,
        "recent_runs": [_run(r) for r in runs[:15]],
        "storage": storage(settings),
        "data_dir": str(settings.data_path),
    }
    snapshot["health"] = health(snapshot)
    return snapshot


def health(snapshot: Json) -> Json:
    """Overall level plus the reasons, most severe first."""
    reasons: list[Reason] = []
    lock = snapshot.get("lock")
    if lock and lock.get("alive"):
        reasons.append(Reason("running", f"a run is in progress (pid {lock.get('pid')})"))
    for s in snapshot.get("streams", []):
        name = f"{s['source']}/{s['stream']}"
        if s["consecutive_failures"]:
            reasons.append(
                Reason(
                    "error",
                    f"{name}: {s['consecutive_failures']} failed run(s) in a row; "
                    f"next retry {s['next_due'] or 'now'}",
                )
            )
        elif s["overdue"]:
            reasons.append(Reason("warn", f"{name}: overdue since {s['next_due']}"))
        if s.get("paused"):
            reasons.append(Reason("warn", f"{name} is paused: scheduled runs skip it"))
    t = snapshot.get("transform") or {}
    if t.get("consecutive_failures"):
        reasons.append(
            Reason(
                "error",
                f"transform: {t['consecutive_failures']} failed attempt(s): {t['last_error']}",
            )
        )
    for q in snapshot.get("quality", []):
        if not q["ok"]:
            reasons.append(Reason("warn", f"quality {q['check']} {q['subject']}: {q['details']}"))
    task = snapshot.get("scheduler")
    if task is not None:
        if not task["installed"]:
            reasons.append(Reason("error", "scheduled task is not installed"))
        elif task["state"] == "Disabled":
            reasons.append(Reason("error", "scheduled task is disabled"))
        elif task["result_level"] == "error":
            reasons.append(Reason("error", f"scheduled task: {task['result']}"))
    if snapshot.get("note"):
        reasons.append(Reason("warn", snapshot["note"]))

    reasons.sort(key=lambda r: -LEVELS.index(r.level))
    level = reasons[0].level if reasons else "ok"
    return {"level": level, "reasons": [{"level": r.level, "message": r.message} for r in reasons]}


def describe_schedule(stream: StreamSpec) -> str:
    sched = stream.schedule
    if isinstance(sched, IntervalSchedule):
        return f"every {_duration(sched.every)}"
    if isinstance(sched, DailySchedule):
        return f"daily at {sched.at:%H:%M}"
    if isinstance(sched, MonthlySchedule):
        return f"monthly on day {sched.day} at {sched.at:%H:%M}"
    return str(sched)  # pragma: no cover


def schedule_fields(stream: StreamSpec) -> Json:
    """The schedule as the console's editor fields (times are Brussels wall-clock)."""
    sched = stream.schedule
    if isinstance(sched, IntervalSchedule):
        return {"kind": "interval", "every_minutes": round(sched.every.total_seconds() / 60)}
    if isinstance(sched, DailySchedule):
        return {"kind": "daily", "at": f"{sched.at:%H:%M}"}
    if isinstance(sched, MonthlySchedule):
        return {"kind": "monthly", "day": sched.day, "at": f"{sched.at:%H:%M}"}
    raise TypeError(sched)  # pragma: no cover


def stream_settings(effective: StreamSpec, default: StreamSpec) -> Json:
    """What the schedule editor shows: the effective values and the connector's defaults."""

    def days(d: timedelta | None) -> float | None:
        return None if d is None else d.total_seconds() / 86400

    return {
        "schedule": schedule_fields(effective),
        "revision_window_days": days(effective.revision_window),
        "default_schedule": schedule_fields(default),
        "default_schedule_text": describe_schedule(default),
        "default_revision_window_days": days(default.revision_window),
        "customized": (effective.schedule, effective.revision_window)
        != (default.schedule, default.revision_window),
    }


def read_lock(path: Path) -> Json | None:
    """The lock file's holder, or None when no run holds the lock."""
    try:
        content = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, ValueError):
        return {"pid": None, "since": None, "alive": True}
    pid = content.get("pid")
    since = content.get("since")
    return {
        "pid": pid,
        "since": _iso(datetime.fromtimestamp(since, UTC)) if since else None,
        "alive": pid_alive(int(pid)) if isinstance(pid, int) else True,
    }


def storage(settings: Settings) -> Json:
    def size(path: Path) -> int:
        return sum(f.stat().st_size for f in path.rglob("*") if f.is_file()) if path.exists() else 0

    return {
        "raw_bytes": size(settings.raw_path),
        "staging_bytes": size(settings.staging_path),
        "marts_bytes": size(settings.marts_path),
        "quarantine_bytes": size(settings.quarantine_path),
        "quarantine_files": (
            sum(1 for _ in settings.quarantine_path.rglob("*.parquet"))
            if settings.quarantine_path.exists()
            else 0
        ),
    }


def _empty(
    settings: Settings,
    now: datetime,
    lock: Json | None,
    task: TaskInfo | None,
    note: str = "nothing ingested yet",
) -> Json:
    snapshot = {
        "generated_at": _iso(now),
        "state_available": True,
        "note": note,
        "lock": lock,
        "scheduler": task.as_json() if task else None,
        "streams": [],
        "transform": None,
        "quality": [],
        "recent_runs": [],
        "storage": storage(settings),
        "data_dir": str(settings.data_path),
    }
    snapshot["health"] = health(snapshot)
    return snapshot


def _run(run: dict[str, Any] | None) -> Json | None:
    if run is None:
        return None
    started, finished = run["started_at"], run["finished_at"]
    return {
        "run_id": run["run_id"],
        "source": run["source"],
        "stream": run["stream"],
        "mode": run["mode"],
        "status": run["status"],
        "started_at": _iso(started),
        "finished_at": _iso(finished),
        "duration_s": (finished - started).total_seconds() if started and finished else None,
        "requests": run["requests"],
        "rows_fetched": run["rows_fetched"],
        "files_written": run["files_written"],
        "watermark_after": run["watermark_after"],
        "error": run["error"],
    }


def _overdue(next_due: datetime | None, now: datetime, lock: Json | None) -> bool:
    running = bool(lock and lock.get("alive"))
    return next_due is not None and not running and now - next_due > OVERDUE_AFTER


def _duration(d: timedelta) -> str:
    seconds = int(d.total_seconds())
    for unit, size in (("d", 86400), ("h", 3600), ("min", 60)):
        if seconds % size == 0 and seconds >= size:
            return f"{seconds // size} {unit}"
    return f"{seconds} s"


def _hours(d: timedelta) -> float:
    return round(d.total_seconds() / 3600, 1)


def _iso(t: datetime | None) -> str | None:
    return t.astimezone(UTC).isoformat() if t else None
