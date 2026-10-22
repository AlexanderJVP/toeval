"""Progress events: the JSON-lines log, reading it with a cursor, and what runs emit."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from fakes import NO_SLEEP, ORIGIN, FakeClock, FakeSeriesConnector, SeriesServer
from test_transform import constant, load_rows
from toeval.config import Settings
from toeval.events import EventLog, read_events, rotated
from toeval.state import StateStore
from toeval.warehouse.raw import RawWriter
from toeval.warehouse.transform import MODELS, transform

NOW = datetime(2026, 1, 20, 13, tzinfo=UTC)


class Recorder:
    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []

    def emit(self, type: str, **fields: Any) -> None:
        self.events.append({"type": type, **fields})

    def types(self) -> list[str]:
        return [e["type"] for e in self.events]


# -- the log -------------------------------------------------------------------------------------


def test_events_are_read_after_a_cursor(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    events = EventLog(path, clock=lambda: NOW)
    events.emit("run_started", stream="load")

    history, cursor = read_events(path, None)
    events.emit("request_done", rows=96)
    new, cursor2 = read_events(path, cursor)
    nothing, cursor3 = read_events(path, cursor2)

    assert history == [{"at": NOW.isoformat(), "type": "run_started", "stream": "load"}]
    assert [e["type"] for e in new] == ["request_done"]
    assert nothing == []
    assert cursor3 == cursor2


def test_without_a_cursor_only_the_tail_is_returned(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    events = EventLog(path)
    for i in range(10):
        events.emit("tick", i=i)

    history, _ = read_events(path, None, tail=3)

    assert [e["i"] for e in history] == [7, 8, 9]


def test_a_half_written_line_waits_for_the_next_read(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    EventLog(path)
    _, cursor = read_events(path, None)
    with path.open("a", encoding="utf-8") as f:
        f.write('{"type": "tick", "i": ')

    first, cursor = read_events(path, cursor)
    with path.open("a", encoding="utf-8") as f:
        f.write("1}\n")
    second, _ = read_events(path, cursor)

    assert first == []
    assert second == [{"type": "tick", "i": 1}]


def test_rotation_finishes_the_old_file_first(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    old = EventLog(path)
    old.emit("tick", i=1)
    _, cursor = read_events(path, None)
    old.emit("tick", i=2)  # written after the reader's last poll

    new = EventLog(path, max_bytes=1)  # the next process rotates
    new.emit("tick", i=3)
    events, cursor = read_events(path, cursor)

    assert rotated(path).exists()
    assert [e["i"] for e in events] == [2, 3]
    assert read_events(path, cursor)[0] == []


def test_missing_or_foreign_file_reads_as_empty(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    assert read_events(path, None) == ([], "")
    path.write_text("not json\n", encoding="utf-8")
    assert read_events(path, "x:5") == ([], "")


def test_unwritable_log_never_fails_the_caller(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    path.mkdir()  # a directory where the file should be

    events = EventLog(path)
    events.emit("tick")

    assert events.emitted == 0


def test_events_are_one_json_object_per_line(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    EventLog(path).emit("run_finished", error="naïve\nmultiline", at_=ORIGIN)

    lines = path.read_text(encoding="utf-8").splitlines()

    assert len(lines) == 2  # header + event
    assert json.loads(lines[1])["error"] == "naïve\nmultiline"


# -- what runs emit ------------------------------------------------------------------------------


def test_a_connector_run_reports_each_request(settings: Settings, store: StateStore) -> None:
    server = SeriesServer()
    server.set_range(ORIGIN, NOW - timedelta(hours=1), lambda t: 1.0)
    recorder = Recorder()
    connector = FakeSeriesConnector(
        settings,
        store,
        transport=server.transport(),
        clock=(clock := FakeClock(NOW)),
        http_options=NO_SLEEP,
        events=recorder,
    )

    connector.run("hourly")
    clock.advance(timedelta(minutes=10))
    connector.run("hourly")  # the revision window: new request keys, so written once more
    third = len(recorder.events)
    clock.advance(timedelta(minutes=10))
    connector.run("hourly")  # same keys, same data

    first = [e for e in recorder.events if e["run"] == recorder.events[0]["run"]]
    types = [e["type"] for e in first]
    assert types[:2] == ["run_started", "planned"]
    assert types[-1] == "run_finished"
    requests = first[1]["requests"]
    done = [e for e in first if e["type"] == "request_done"]
    assert len(done) == requests == types.count("request_started")
    assert [e["index"] for e in done] == list(range(1, requests + 1))
    assert {e["outcome"] for e in done} == {"written"}
    assert sum(e["rows"] for e in done) == first[-1]["rows"]
    assert first[-1]["status"] == "success"
    assert first[-1]["files"] == requests
    third_done = [e for e in recorder.events[third:] if e["type"] == "request_done"]
    assert {e["outcome"] for e in third_done} == {"unchanged"}
    assert all(e.get("file") is None for e in third_done)


def test_a_failed_run_reports_its_error(settings: Settings, store: StateStore) -> None:
    server = SeriesServer(fail_next=[404])
    recorder = Recorder()
    connector = FakeSeriesConnector(
        settings,
        store,
        transport=server.transport(),
        clock=FakeClock(NOW),
        http_options=NO_SLEEP,
        events=recorder,
    )

    connector.run("hourly")

    finished = recorder.events[-1]
    assert finished["type"] == "run_finished"
    assert finished["status"] == "failed"
    assert "404" in finished["error"]


def test_a_transform_reports_each_model(settings: Settings, store: StateStore) -> None:
    load = load_rows(ORIGIN, ORIGIN + timedelta(hours=1), constant(1.0))
    writer = RawWriter(settings.raw_path, settings.quarantine_path, store)
    writer.write("elia", "load", load, run_id="r1", request_key="k", ingested_at=ORIGIN)
    recorder = Recorder()

    transform(settings, store, now=ORIGIN, events=recorder)

    assert recorder.types()[0] == "transform_started"
    built = [e for e in recorder.events if e["type"] == "model_built"]
    assert [e["model"] for e in built] == ["stg_elia_load", "mart_calendar_day"]
    assert built[0]["rows"] == 4
    assert recorder.types().count("model_skipped") == len(MODELS) - len(built)
    assert recorder.events[-1]["type"] == "transform_finished"
    assert recorder.events[-1]["status"] == "success"
