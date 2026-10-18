"""The admin console: snapshot contents, health rules, API behaviour, Task Scheduler parsing."""

import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from toeval.config import Settings
from toeval.state import QualityResult, RunStats, StateStore, StreamState, Watermark
from toeval.ui import app as app_module
from toeval.ui.app import TaskCache, create_app
from toeval.ui.scheduler import TaskInfo, describe_result, parse_task, query_task
from toeval.ui.snapshot import StateBusyError, build_snapshot, health, read_lock

NOW = datetime(2026, 10, 19, 13, tzinfo=UTC)  # after Elia's 12:00 Brussels run
READY = TaskInfo(installed=True, state="Ready", last_result=0, next_run_at=NOW)


def succeed(store: StateStore, stream: str, at: datetime, data_until: datetime) -> None:
    run = store.start_run("elia", stream, "incremental", at)
    state = StreamState(
        "elia", stream, watermark=Watermark.timestamp(data_until), last_success_at=at
    )
    store.finish_run_success(run, state, RunStats(requests=1, rows_fetched=96), at, None)


@pytest.fixture
def ingested(settings: Settings) -> Settings:
    """All three Elia streams succeeded at today's 12:00 run."""
    with StateStore(settings.state_db_path) as store:
        for stream in ("load", "wind", "solar"):
            succeed(store, stream, NOW - timedelta(hours=1), NOW - timedelta(hours=20))
        t = store.start_transform(NOW - timedelta(minutes=50))
        store.finish_transform(t, NOW - timedelta(minutes=50), models_built=5)
    return settings


# -- snapshot and health -------------------------------------------------------------------------


def test_snapshot_of_a_healthy_pipeline(ingested: Settings) -> None:
    snap = build_snapshot(ingested, now=NOW, task=READY)

    assert snap["health"] == {"level": "ok", "reasons": []}
    assert [s["stream"] for s in snap["streams"]] == ["load", "wind", "solar"]
    load = snap["streams"][0]
    assert load["schedule"] == "daily at 12:00"
    assert load["data_age_hours"] == 20.0
    assert load["next_due"] == "2026-10-20T10:00:00+00:00"  # tomorrow 12:00 Brussels
    assert load["last_run"]["status"] == "success"
    assert snap["transform"]["last_status"] == "success"
    assert snap["scheduler"]["result"] == "last run succeeded"
    json.dumps(snap)  # JSON-ready


def test_nothing_ingested_yet(settings: Settings) -> None:
    snap = build_snapshot(settings, now=NOW, task=None)

    assert snap["streams"] == []
    assert snap["health"]["level"] == "warn"
    assert "nothing ingested yet" in snap["health"]["reasons"][0]["message"]


def test_failing_stream_is_an_error_with_its_retry_time(ingested: Settings) -> None:
    with StateStore(ingested.state_db_path) as store:
        yesterday = NOW - timedelta(days=1)
        succeed(store, "wind", yesterday, yesterday - timedelta(hours=20))
        run = store.start_run("elia", "wind", "incremental", NOW)
        store.finish_run_failed(run, RunStats(), NOW, None, "HttpError: 503")

    snap = build_snapshot(ingested, now=NOW, task=READY)

    assert snap["health"]["level"] == "error"
    assert "elia/wind: 1 failed run(s)" in snap["health"]["reasons"][0]["message"]
    wind = snap["streams"][1]
    assert wind["last_run"]["error"] == "HttpError: 503"
    assert wind["next_due"] == "2026-10-19T13:05:00+00:00"  # today's run is due; 5 min backoff


def test_overdue_stream_means_the_scheduler_is_not_running(ingested: Settings) -> None:
    later = NOW + timedelta(days=1, hours=1)  # a day later, nothing ran

    snap = build_snapshot(ingested, now=later, task=READY)

    assert snap["health"]["level"] == "warn"
    assert all(s["overdue"] for s in snap["streams"])


def test_quality_warnings_and_scheduler_problems_are_reported(ingested: Settings) -> None:
    with StateStore(ingested.state_db_path) as store:
        store.add_quality_results(
            [QualityResult(NOW, "freshness", "elia/solar", 80.0, "hours", False, "last measured …")]
        )
    disabled = TaskInfo(installed=True, state="Disabled", last_result=0)

    snap = build_snapshot(ingested, now=NOW, task=disabled)

    messages = [r["message"] for r in snap["health"]["reasons"]]
    assert snap["health"]["level"] == "error"
    assert messages[0] == "scheduled task is disabled"  # errors before warnings
    assert any("quality freshness elia/solar" in m for m in messages)


def test_a_live_run_shows_as_running() -> None:
    snap = {"lock": {"pid": os.getpid(), "alive": True}, "streams": [], "quality": []}

    assert health(snap)["level"] == "running"


def test_read_lock(tmp_path: Path) -> None:
    path = tmp_path / "ingest.lock"
    assert read_lock(path) is None

    dead = subprocess.Popen([sys.executable, "-c", "pass"])
    dead.wait()
    path.write_text(json.dumps({"pid": dead.pid, "since": NOW.timestamp(), "token": "t"}), "utf-8")

    assert read_lock(path) == {"pid": dead.pid, "since": NOW.isoformat(), "alive": False}


def test_busy_state_db_raises(ingested: Settings) -> None:
    holder = subprocess.Popen(
        [sys.executable, "-c", HOLD_DB, str(ingested.state_db_path)],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert holder.stdout is not None
        assert holder.stdout.readline().strip() == "ready"
        with pytest.raises(StateBusyError):
            build_snapshot(ingested, now=NOW, task=None)
    finally:
        holder.kill()
        holder.wait()


HOLD_DB = """
import sys, time, duckdb
con = duckdb.connect(sys.argv[1])
print("ready", flush=True)
time.sleep(60)
"""


# -- API -----------------------------------------------------------------------------------------


def client(settings: Settings, **kwargs: Any) -> TestClient:
    app = create_app(settings, task_query=lambda: READY, clock=lambda: NOW, **kwargs)
    return TestClient(app, base_url="http://127.0.0.1")


def test_page_and_snapshot_are_served(ingested: Settings) -> None:
    c = client(ingested)

    page = c.get("/")
    snap = c.get("/api/snapshot")

    assert page.status_code == 200
    assert "toeval" in page.text
    assert c.get("/static/app.js").status_code == 200
    assert snap.status_code == 200
    assert snap.json()["health"]["level"] == "ok"


def test_other_hosts_are_rejected(ingested: Settings) -> None:
    c = TestClient(create_app(ingested, task_query=lambda: None), base_url="http://evil.example")

    assert c.get("/api/snapshot").status_code == 400


def test_busy_state_db_serves_the_last_snapshot_with_a_live_lock(
    ingested: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    c = client(ingested)
    assert c.get("/api/snapshot").json()["state_available"]
    ingested.lock_path.write_text(json.dumps({"pid": os.getpid(), "since": NOW.timestamp()}))

    def busy(*_args: object, **_kwargs: object) -> None:
        raise StateBusyError("locked")

    monkeypatch.setattr(app_module, "build_snapshot", busy)
    snap = c.get("/api/snapshot").json()

    assert snap["state_available"] is False
    assert snap["streams"]  # the last readable state
    assert snap["health"]["level"] == "running"  # recomputed with the live lock


def test_busy_before_any_snapshot_is_503(
    settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    def busy(*_args: object, **_kwargs: object) -> None:
        raise StateBusyError("locked")

    monkeypatch.setattr(app_module, "build_snapshot", busy)

    assert client(settings).get("/api/snapshot").status_code == 503


def test_task_cache_refreshes_after_max_age() -> None:
    calls = []
    cache = TaskCache(lambda: calls.append(1) or READY, max_age=-1)

    assert cache.get(wait=True) == READY
    cache.get(wait=True)
    assert len(calls) == 2


# -- Task Scheduler ------------------------------------------------------------------------------


def test_parse_task_output() -> None:
    out = (
        '{"installed":true,"state":"Ready","last_run":"2026-10-19T00:38:29+02:00",'
        '"last_result":0,"next_run":"2026-10-19T00:43:28+02:00"}'
    )

    task = parse_task(out)

    assert task == TaskInfo(
        installed=True,
        state="Ready",
        last_run_at=datetime(2026, 10, 18, 22, 38, 29, tzinfo=UTC),
        last_result=0,
        next_run_at=datetime(2026, 10, 18, 22, 43, 28, tzinfo=UTC),
    )
    assert parse_task('{"installed": false}') == TaskInfo(installed=False)
    assert parse_task("garbage") is None


@pytest.mark.parametrize(
    ("code", "level"),
    [(0, "ok"), (0x41301, "ok"), (0x41303, "ok"), (1, "error"), (0x80070002, "error")],
)
def test_task_results(code: int, level: str) -> None:
    assert describe_result(code)[0] == level


@pytest.mark.skipif(sys.platform != "win32", reason="Task Scheduler is Windows-only")
def test_query_task_handles_powershell_failure() -> None:
    def failing(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess([], 1, "", "boom")

    assert query_task(runner=failing) is None
