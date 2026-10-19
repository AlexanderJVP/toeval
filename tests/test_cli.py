import subprocess
import sys
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest
from typer.testing import CliRunner

from fakes import ORIGIN, FakeLiveConnector, FakeSeriesConnector, LiveServer, SeriesServer, bind
from toeval import __version__
from toeval.cli import app
from toeval.config import get_settings
from toeval.connectors import registry
from toeval.lock import FileLock
from toeval.state import MIGRATIONS, StateStore

runner = CliRunner()


@pytest.fixture
def series() -> SeriesServer:
    server = SeriesServer()
    server.set_range(ORIGIN, datetime.now(UTC) - timedelta(hours=1), lambda t: 1.0)
    return server


@pytest.fixture
def data_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, series: SeriesServer, restore_logging: None
) -> Iterator[Path]:
    """CLI with its data dir in tmp and the registry pointing at fake sources."""
    data = tmp_path / "data"
    monkeypatch.setenv("TOEVAL_DATA_DIR", str(data))
    monkeypatch.setenv("TOEVAL_SOURCES_FILE", str(tmp_path / "sources.toml"))
    live = LiveServer()
    live.publish("Antwerpen-Centraal", [])
    live.publish("Berchem", [])
    monkeypatch.setattr(
        registry,
        "CONNECTORS",
        {"series": bind(FakeSeriesConnector, series), "live": bind(FakeLiveConnector, live)},
    )
    get_settings.cache_clear()
    yield data
    get_settings.cache_clear()


def test_version_command_prints_version() -> None:
    result = runner.invoke(app, ["version"])

    assert result.exit_code == 0
    assert result.stdout.strip() == __version__


def test_ingest_runs_all_streams_of_a_source_and_logs_to_file(data_dir: Path) -> None:
    result = runner.invoke(app, ["ingest", "series"])

    assert result.exit_code == 0, result.output
    assert "series/hourly: ok" in result.output
    assert "series/hourly" in (data_dir / "logs" / "toeval.log").read_text(encoding="utf-8")
    assert not (data_dir / "state" / "ingest.lock").exists()  # released


def test_failed_stream_gives_exit_code_1(data_dir: Path, series: SeriesServer) -> None:
    series.fail_next = [404]

    result = runner.invoke(app, ["ingest", "series"])

    assert result.exit_code == 1
    assert "series/hourly: FAILED" in result.output


@pytest.mark.parametrize(
    "args",
    [
        ["ingest", "nope"],
        ["ingest", "series", "nope"],
        ["backfill", "live", "departures"],  # live-only: no history to backfill
        ["backfill", "series", "hourly", "--from", "yesterday"],
    ],
)
def test_usage_errors_give_exit_code_2(data_dir: Path, args: list[str]) -> None:
    result = runner.invoke(app, args)

    assert result.exit_code == 2, result.output
    assert "error:" in result.output


def test_backfill_reads_naive_times_as_brussels(data_dir: Path, series: SeriesServer) -> None:
    result = runner.invoke(
        app, ["backfill", "series", "hourly", "--from", "2026-01-02", "--to", "2026-01-03"]
    )

    assert result.exit_code == 0, result.output
    starts = [datetime.fromisoformat(r.url.params["start"]) for r in series.requests]
    assert starts == [datetime(2026, 1, 1, 23, tzinfo=UTC)]


def test_held_lock_makes_ingest_exit_3_and_run_due_skip(
    data_dir: Path, series: SeriesServer
) -> None:
    with FileLock(data_dir / "state" / "ingest.lock"):
        ingest = runner.invoke(app, ["ingest", "series"])
        run_due = runner.invoke(app, ["run-due"])

    assert ingest.exit_code == 3
    assert run_due.exit_code == 0
    assert series.requests == []


def test_run_due_then_status(data_dir: Path) -> None:
    before = runner.invoke(app, ["status"])
    first = runner.invoke(app, ["run-due"])
    second = runner.invoke(app, ["run-due"])
    status = runner.invoke(app, ["status"])

    assert "No state yet" in before.output
    assert first.exit_code == 0, first.output
    assert "series/hourly: ok" in first.output
    assert "live/departures: ok" in first.output
    assert second.exit_code == 0
    assert "series/hourly" not in second.output  # daily stream: not due again
    assert status.exit_code == 0, status.output
    assert "series/hourly: watermark 20" in status.output
    assert "live/departures: watermark -" in status.output


def test_status_while_another_process_holds_the_state_db(data_dir: Path) -> None:
    runner.invoke(app, ["ingest", "series"])
    db = data_dir / "state" / "state.duckdb"
    holder = subprocess.Popen(
        [sys.executable, "-c", HOLD_DB, str(db)], stdout=subprocess.PIPE, text=True
    )
    try:
        assert holder.stdout is not None
        assert holder.stdout.readline().strip() == "ready"
        result = runner.invoke(app, ["status"])
    finally:
        holder.kill()
        holder.wait()

    assert result.exit_code == 3, result.output
    assert "in progress" in result.output


HOLD_DB = """
import sys, time, duckdb
con = duckdb.connect(sys.argv[1])
print("ready", flush=True)
time.sleep(60)
"""
# Like the admin console: a read-only connection, held for `argv[2]` seconds.
READ_DB = """
import sys, time, duckdb
con = duckdb.connect(sys.argv[1], read_only=True)
print("ready", flush=True)
time.sleep(float(sys.argv[2]))
con.close()
time.sleep(60)
"""


def hold_read_only(db: Path, seconds: float) -> subprocess.Popen[str]:
    reader = subprocess.Popen(
        [sys.executable, "-c", READ_DB, str(db), str(seconds)], stdout=subprocess.PIPE, text=True
    )
    assert reader.stdout is not None
    assert reader.stdout.readline().strip() == "ready"
    return reader


def test_a_run_waits_for_a_reader_of_the_state_db(data_dir: Path) -> None:
    runner.invoke(app, ["ingest", "series"])
    reader = hold_read_only(data_dir / "state" / "state.duckdb", 0.5)
    try:
        result = runner.invoke(app, ["ingest", "series"])
    finally:
        reader.kill()
        reader.wait()

    assert result.exit_code == 0, result.output


def test_a_state_db_held_too_long_gives_exit_code_3(
    data_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner.invoke(app, ["ingest", "series"])
    monkeypatch.setattr("toeval.cli.STATE_OPEN_TIMEOUT", 0.3)
    reader = hold_read_only(data_dir / "state" / "state.duckdb", 60)
    try:
        result = runner.invoke(app, ["run-due"])
    finally:
        reader.kill()
        reader.wait()

    assert result.exit_code == 3, result.output
    assert "in use by another program" in result.output
    assert not (data_dir / "state" / "ingest.lock").exists()  # the lock was released


def test_transform_command_and_run_due_trigger(data_dir: Path) -> None:
    transform = runner.invoke(app, ["transform"])
    first = runner.invoke(app, ["run-due"])  # writes raw files -> transforms
    second = runner.invoke(app, ["run-due"])  # nothing written, nothing missing -> no transform

    assert transform.exit_code == 0, transform.output
    assert "transform: built nothing" in transform.output  # fake sources have no Elia models
    assert "transform:" in first.output
    assert "transform:" not in second.output


def test_taking_the_lock_cleans_up_after_a_killed_run(data_dir: Path) -> None:
    db = data_dir / "state" / "state.duckdb"
    with StateStore(db) as store:
        orphan = store.start_run("series", "hourly", "incremental", datetime.now(UTC))

    result = runner.invoke(app, ["ingest", "series"])

    assert result.exit_code == 0, result.output
    with StateStore(db) as store:
        assert store.get_run(orphan)["status"] == "failed"


def test_status_shows_a_failed_transform(data_dir: Path) -> None:
    runner.invoke(app, ["ingest", "series"])
    with StateStore(data_dir / "state" / "state.duckdb") as store:
        failed = store.start_transform(datetime.now(UTC))
        store.finish_transform(failed, datetime.now(UTC), models_built=0, error="boom")

    result = runner.invoke(app, ["status"])

    assert "1 failed attempt(s) since" in result.output
    assert "boom" in result.output


def test_status_on_a_state_db_that_is_not_migrated_yet(data_dir: Path) -> None:
    db = data_dir / "state" / "state.duckdb"
    db.parent.mkdir(parents=True)
    con = duckdb.connect(str(db))
    con.execute(MIGRATIONS[0][2])
    con.close()

    result = runner.invoke(app, ["status"])

    assert result.exit_code == 0, result.output
    assert "next run" in result.output


def test_runs_write_progress_events_for_the_console(data_dir: Path) -> None:
    from toeval.events import read_events

    runner.invoke(app, ["ingest", "live"])
    runner.invoke(app, ["ingest", "series"])
    events, cursor = read_events(data_dir / "state" / "events.jsonl", None, tail=1000)
    types = [e["type"] for e in events]

    assert types[0] == "run_started"
    assert types.count("idle") == 2  # one per process
    assert "request_done" in types
    assert "transform_finished" in types
    assert types[-1] == "idle"  # emitted after the state DB was closed

    runner.invoke(app, ["run-due"])  # nothing due (live polls every 5 min): silent
    assert read_events(data_dir / "state" / "events.jsonl", cursor)[0] == []
