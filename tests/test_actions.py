"""Console actions: pausing via sources.toml, starting CLI jobs, and the session token."""

import json
import os
import sys
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from fakes import ORIGIN, FakeClock, FakeLiveConnector, FakeSeriesConnector, SeriesServer, bind
from toeval.cli import app as cli
from toeval.config import Settings, get_settings
from toeval.pipeline import Pipeline
from toeval.sources_file import set_stream_option
from toeval.state import StateStore
from toeval.ui.actions import BusyError, JobRunner
from toeval.ui.app import ScheduleRequest, create_app, schedule_overrides
from toeval.ui.scheduler import TaskInfo
from toeval.ui.snapshot import build_snapshot

NOW = datetime(2026, 1, 20, 13, tzinfo=UTC)
TOKEN = "test-token"
# Stands in for `python -m toeval`: prints its arguments; exits 1 when the last one is "fail".
ECHO = (
    sys.executable,
    "-c",
    "import sys; print('args', *sys.argv[1:]); sys.exit(sys.argv[-1] == 'fail')",
)
SLEEPER = (sys.executable, "-c", "import time; time.sleep(30)")


def wait(runner: JobRunner) -> None:
    deadline = time.monotonic() + 20
    while runner.running() is not None:
        assert time.monotonic() < deadline, "job did not finish"
        time.sleep(0.05)


# -- sources.toml ----------------------------------------------------------------------------------


def test_pause_and_resume_edit_sources_toml_and_keep_comments(tmp_path: Path) -> None:
    path = tmp_path / "sources.toml"
    path.write_text('# my notes\n[elia.http]\ntimeout = "240s"  # slow exports\n', "utf-8")

    set_stream_option(path, "elia", "load", "paused", True)
    paused = path.read_text("utf-8")
    set_stream_option(path, "elia", "load", "paused", None)
    resumed = path.read_text("utf-8")

    assert "[elia.streams.load]\npaused = true" in paused
    assert "# my notes" in paused
    assert "# slow exports" in paused
    assert "streams" not in resumed  # the emptied tables are gone again
    assert "# slow exports" in resumed


def test_invalid_edits_write_nothing(tmp_path: Path) -> None:
    path = tmp_path / "sources.toml"
    path.write_text('[elia.streams.load]\nrevision_window = "7d"\n', "utf-8")
    before = path.read_text("utf-8")

    with pytest.raises(KeyError):
        set_stream_option(path, "elia", "nope", "paused", True)
    with pytest.raises(KeyError):
        set_stream_option(path, "nope", "load", "paused", True)
    with pytest.raises(ValueError, match="paused"):
        set_stream_option(path, "elia", "load", "paused", "sometimes")

    assert path.read_text("utf-8") == before
    assert not (tmp_path / "sources.toml.tmp").exists()


def test_paused_streams_are_skipped_by_run_due_only(settings: Settings, store: StateStore) -> None:
    server = SeriesServer()
    server.set_range(ORIGIN, NOW, lambda t: 1.0)
    connectors = {"series": bind(FakeSeriesConnector, server)}
    set_stream_option(
        settings.sources_path, "series", "hourly", "paused", True, connectors=connectors
    )
    pipeline = Pipeline(settings, store, clock=FakeClock(NOW), connectors=connectors)

    assert pipeline.due() == []
    assert pipeline.run_due() == []
    assert pipeline.run("series")[0].succeeded  # an explicit run still works


# -- CLI -----------------------------------------------------------------------------------------


@pytest.fixture
def cli_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, restore_logging: None
) -> Iterator[Path]:
    monkeypatch.setenv("TOEVAL_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("TOEVAL_SOURCES_FILE", str(tmp_path / "sources.toml"))
    get_settings.cache_clear()
    yield tmp_path / "sources.toml"
    get_settings.cache_clear()


def test_pause_and_resume_commands(cli_env: Path) -> None:
    runner = CliRunner()

    paused = runner.invoke(cli, ["pause", "elia", "wind"])
    assert paused.exit_code == 0, paused.output
    assert "elia/wind: paused" in paused.output
    assert "paused = true" in cli_env.read_text("utf-8")

    assert runner.invoke(cli, ["resume", "elia", "wind"]).exit_code == 0
    assert "paused" not in cli_env.read_text("utf-8")
    assert runner.invoke(cli, ["pause", "elia", "nope"]).exit_code == 2


# -- jobs ----------------------------------------------------------------------------------------


def test_a_job_runs_the_command_and_reports_its_outcome(settings: Settings) -> None:
    runner = JobRunner(settings, command=ECHO)

    runner.start("fetch elia/load", ["ingest", "elia", "load"])
    wait(runner)
    runner.start("broken", ["transform", "fail"])
    wait(runner)
    newest, oldest = runner.recent()

    assert oldest["status"] == "ok"
    assert oldest["command"] == "toeval ingest elia load"
    assert oldest["output"] == ["args ingest elia load"]
    assert newest["status"] == "failed"
    assert newest["outcome"] == "failed"
    assert len(list((settings.log_path / "console").glob("*.log"))) == 2


def test_one_job_at_a_time(settings: Settings) -> None:
    runner = JobRunner(settings, command=SLEEPER)
    job = runner.start("slow", ["x"])
    try:
        with pytest.raises(BusyError):
            runner.start("another", ["y"])
    finally:
        job.process.kill()
        job.process.wait()


# -- API -----------------------------------------------------------------------------------------


def client(settings: Settings, runner: JobRunner | None = None) -> TestClient:
    app = create_app(
        settings,
        task_query=lambda: TaskInfo(installed=True, state="Ready", last_result=0),
        clock=lambda: NOW,
        jobs=runner or JobRunner(settings, command=ECHO),
        token=TOKEN,
    )
    return TestClient(app, base_url="http://127.0.0.1")


AUTH = {"X-Toeval-Token": TOKEN}


def test_the_page_carries_the_session_token(settings: Settings) -> None:
    page = client(settings).get("/").text

    assert f'<meta name="toeval-token" content="{TOKEN}">' in page


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"X-Toeval-Token": "wrong"},
        {"X-Toeval-Token": TOKEN, "Origin": "https://evil.example"},
    ],
)
def test_actions_need_the_token_and_a_local_origin(
    settings: Settings, headers: dict[str, str]
) -> None:
    c = client(settings)

    assert c.post("/api/actions/transform", headers=headers).status_code == 403
    assert (
        c.post(
            "/api/actions/pause",
            headers=headers,
            json={"source": "elia", "stream": "load", "paused": True},
        ).status_code
        == 403
    )
    assert not settings.sources_path.exists()


def test_fetch_now_starts_an_ingest_job(settings: Settings) -> None:
    runner = JobRunner(settings, command=ECHO)
    c = client(settings, runner)

    started = c.post(
        "/api/actions/ingest",
        headers=AUTH | {"Origin": "http://127.0.0.1:8765"},
        json={"source": "elia", "stream": "solar"},
    )
    wait(runner)
    jobs = c.get("/api/jobs").json()["jobs"]

    assert started.status_code == 202
    assert started.json()["label"] == "fetch elia/solar"
    assert jobs[0]["output"] == ["args ingest elia solar"]
    assert (
        c.post(
            "/api/actions/ingest", headers=AUTH, json={"source": "elia", "stream": "x"}
        ).status_code
        == 404
    )


def test_actions_wait_while_a_run_holds_the_lock(settings: Settings) -> None:
    settings.lock_path.parent.mkdir(parents=True)
    settings.lock_path.write_text(json.dumps({"pid": os.getpid(), "since": NOW.timestamp()}))
    c = client(settings)

    response = c.post("/api/actions/transform", headers=AUTH)

    assert response.status_code == 409
    assert "in progress" in response.json()["detail"]


def test_pause_from_the_console_shows_in_the_snapshot(settings: Settings) -> None:
    c = client(settings)
    with StateStore(settings.state_db_path):
        pass  # migrated, nothing ingested

    response = c.post(
        "/api/actions/pause",
        headers=AUTH,
        json={"source": "elia", "stream": "wind", "paused": True},
    )
    snap = build_snapshot(settings, now=NOW, task=None)
    wind = next(s for s in snap["streams"] if s["stream"] == "wind")

    assert response.status_code == 200
    assert wind["paused"] is True
    assert wind["overdue"] is False
    assert "elia/wind is paused: scheduled runs skip it" in [
        r["message"] for r in snap["health"]["reasons"]
    ]


# -- schedule editor -----------------------------------------------------------------------------


def schedule_body(stream: str, schedule: dict[str, object], window: int | None = None) -> dict:
    return {
        "source": "elia",
        "stream": stream,
        "schedule": schedule,
        "revision_window_days": window,
    }


def test_editing_a_schedule_writes_only_real_deviations(settings: Settings) -> None:
    c = client(settings)

    edited = c.post(
        "/api/actions/schedule",
        headers=AUTH,
        json=schedule_body("wind", {"kind": "interval", "every_minutes": 120}, 30),
    )
    written = settings.sources_path.read_text("utf-8")
    back = c.post(
        "/api/actions/schedule",
        headers=AUTH,
        json=schedule_body("wind", {"kind": "daily", "at": "12:00"}, 14),  # Elia's defaults
    )

    assert edited.status_code == 200, edited.text
    assert edited.json()["schedule"] == "every 2 h"
    assert edited.json()["settings"]["customized"] is True
    assert 'schedule = {kind = "interval", every = "120m"}' in written
    assert 'revision_window = "30d"' in written
    assert back.json()["settings"]["customized"] is False
    assert settings.sources_path.read_text("utf-8").strip() == ""


def test_an_edited_schedule_drives_the_pipeline(settings: Settings, store: StateStore) -> None:
    server = SeriesServer()
    server.set_range(ORIGIN, NOW, lambda t: 1.0)
    connectors = {"series": bind(FakeSeriesConnector, server)}
    pipeline = Pipeline(settings, store, clock=FakeClock(NOW), connectors=connectors)
    pipeline.run("series")
    assert pipeline.due() == []  # daily at 06:30 by default, and it just ran

    set_stream_option(
        settings.sources_path,
        "series",
        "hourly",
        "schedule",
        {"kind": "monthly", "day": 3, "at": "07:15"},
        connectors=connectors,
    )
    spec = Pipeline(settings, store, connectors=connectors).spec("series").stream("hourly")

    assert spec.schedule.kind == "monthly"
    assert spec.schedule.next_after(NOW) == datetime(2026, 2, 3, 6, 15, tzinfo=UTC)


@pytest.mark.parametrize(
    "schedule",
    [
        {"kind": "interval", "every_minutes": 2},  # below the scheduler's 5-min tick
        {"kind": "daily", "at": "25:00"},
        {"kind": "daily"},
        {"kind": "monthly", "day": 31, "at": "09:00"},
        {"kind": "weekly", "at": "09:00"},
    ],
)
def test_impossible_schedules_are_refused(settings: Settings, schedule: dict) -> None:
    response = client(settings).post(
        "/api/actions/schedule", headers=AUTH, json=schedule_body("load", schedule)
    )

    assert response.status_code == 422
    assert not settings.sources_path.exists()


def test_the_snapshot_offers_the_editor_its_values_and_defaults(settings: Settings) -> None:
    settings.sources_path.write_text(
        '[elia.streams.solar]\nschedule = {kind = "daily", at = "13:30"}\n', "utf-8"
    )
    with StateStore(settings.state_db_path):
        pass

    snap = build_snapshot(settings, now=NOW, task=None)
    solar = next(s for s in snap["streams"] if s["stream"] == "solar")

    assert solar["schedule"] == "daily at 13:30"
    assert solar["settings"] == {
        "schedule": {"kind": "daily", "at": "13:30"},
        "revision_window_days": 14,
        "default_schedule": {"kind": "daily", "at": "12:00"},
        "default_schedule_text": "daily at 12:00",
        "default_revision_window_days": 14,
        "customized": True,
    }


def test_a_revision_window_needs_a_stream_that_has_one() -> None:
    live = FakeLiveConnector.spec.streams[0]
    body = ScheduleRequest.model_validate(
        {
            "source": "live",
            "stream": live.name,
            "schedule": {"kind": "interval", "every_minutes": 5},
            "revision_window_days": 3,
        }
    )

    with pytest.raises(ValueError, match="no revision window"):
        schedule_overrides(body, live)
    assert schedule_overrides(body.model_copy(update={"revision_window_days": None}), live) == {
        "schedule": None,  # 5 min is that stream's default
        "revision_window": None,
    }
