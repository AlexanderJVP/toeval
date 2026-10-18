"""Due checks, retry backoff and overrides, against all three fake source shapes."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from fakes import (
    ORIGIN,
    FakeClock,
    FakeLiveConnector,
    FakePeriodConnector,
    FakeSeriesConnector,
    LiveServer,
    PeriodServer,
    SeriesServer,
    bind,
)
from toeval.config import Settings
from toeval.connectors.base import Connector, RunMode
from toeval.pipeline import Pipeline, next_due, retry_delay
from toeval.state import StateStore, StreamState

NO_SLEEP = {"sleep": lambda _s: None}
NOW = datetime(2026, 1, 20, 13, tzinfo=UTC)  # after the series' 06:30 Brussels schedule


class Servers:
    def __init__(self) -> None:
        self.series = SeriesServer()
        self.series.set_range(ORIGIN, NOW, lambda t: 1.0)
        self.live = LiveServer()
        self.live.publish("Antwerpen-Centraal", [{"vehicle": "IC1", "scheduled": "08:00"}])
        self.live.publish("Berchem", [])
        self.period = PeriodServer(rows=[{"period": "2026-09", "value": 1.0}])

    def connectors(self) -> dict[str, type[Connector]]:
        return {
            "series": bind(FakeSeriesConnector, self.series),
            "live": bind(FakeLiveConnector, self.live),
            "stats": bind(FakePeriodConnector, self.period),
        }


@pytest.fixture
def servers() -> Servers:
    return Servers()


def make(settings: Settings, store: StateStore, servers: Servers, clock: FakeClock) -> Pipeline:
    return Pipeline(settings, store, clock=clock, connectors=servers.connectors())


def ran(results: list) -> list[tuple[str, str]]:
    return [(r.source, r.stream) for r in results]


# -- backoff -------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("failures", "delay"),
    [
        (0, timedelta(0)),
        (1, timedelta(minutes=5)),
        (2, timedelta(minutes=10)),
        (4, timedelta(minutes=40)),
        (7, timedelta(hours=5, minutes=20)),
        (8, timedelta(hours=6)),
        (1000, timedelta(hours=6)),
    ],
)
def test_retry_delay_doubles_up_to_six_hours(failures: int, delay: timedelta) -> None:
    assert retry_delay(failures) == delay


def test_next_due_waits_for_both_schedule_and_backoff() -> None:
    stream = FakeSeriesConnector.spec.stream("hourly")  # daily 06:30 Brussels
    yesterday = datetime(2026, 1, 19, 6, 0, tzinfo=UTC)
    schedule = datetime(2026, 1, 20, 5, 30, tzinfo=UTC)

    assert next_due(stream, StreamState("s", "hourly")) is None
    assert next_due(stream, StreamState("s", "hourly", last_success_at=yesterday)) == schedule
    failing = StreamState(
        "s", "hourly", last_success_at=yesterday, consecutive_failures=3, last_failure_at=schedule
    )
    assert next_due(stream, failing) == schedule + timedelta(minutes=20)


# -- run-due -------------------------------------------------------------------------------------


def test_run_due_runs_each_stream_on_its_own_cadence(
    settings: Settings, store: StateStore, servers: Servers
) -> None:
    clock = FakeClock(NOW)
    pipeline = make(settings, store, servers, clock)

    first = pipeline.run_due()
    immediately = pipeline.run_due()
    clock.advance(timedelta(minutes=5))
    five_minutes_later = pipeline.run_due()

    assert sorted(ran(first)) == [("live", "departures"), ("series", "hourly"), ("stats", "cpi")]
    assert all(r.succeeded for r in first)
    assert immediately == []
    assert ran(five_minutes_later) == [("live", "departures")]


def test_failing_stream_backs_off_and_recovers(
    settings: Settings, store: StateStore, servers: Servers
) -> None:
    clock = FakeClock(NOW)
    pipeline = make(settings, store, servers, clock)
    servers.series.fail_next = [404, 404]

    def series_runs() -> list[bool]:
        return [r.succeeded for r in pipeline.run_due() if r.source == "series"]

    assert series_runs() == [False]  # fails, retry in 5 min
    clock.advance(timedelta(minutes=4))
    assert series_runs() == []
    clock.advance(timedelta(minutes=1))
    assert series_runs() == [False]  # second failure, retry in 10 min
    clock.advance(timedelta(minutes=9))
    assert series_runs() == []
    clock.advance(timedelta(minutes=1))
    assert series_runs() == [True]

    state = store.get_stream_state("series", "hourly")
    assert (state.consecutive_failures, state.last_failure_at) == (0, None)


def test_backfill_neither_counts_nor_clears_failures(
    settings: Settings, store: StateStore, servers: Servers
) -> None:
    clock = FakeClock(NOW)
    pipeline = make(settings, store, servers, clock)
    servers.series.fail_next = [404]
    pipeline.run("series")

    result = pipeline.run("series", mode=RunMode.BACKFILL, backfill_to=ORIGIN + timedelta(days=2))

    assert result[0].succeeded
    assert store.get_stream_state("series", "hourly").consecutive_failures == 1


# -- configuration -------------------------------------------------------------------------------


def test_sources_toml_overrides_are_applied(
    settings: Settings, store: StateStore, servers: Servers
) -> None:
    Path(settings.sources_path).write_text(
        '[live.streams.departures]\nschedule = { kind = "interval", every = "1h" }\n',
        encoding="utf-8",
    )
    clock = FakeClock(NOW)
    pipeline = make(settings, store, servers, clock)
    pipeline.run_due()
    clock.advance(timedelta(minutes=5))

    assert pipeline.spec("live").stream("departures").schedule.every == timedelta(hours=1)
    assert pipeline.run_due() == []


def test_unknown_source_in_sources_toml_fails_fast(
    settings: Settings, store: StateStore, servers: Servers
) -> None:
    Path(settings.sources_path).write_text("[elai.http]\nburst = 2\n", encoding="utf-8")

    with pytest.raises(ValueError, match="unknown sources"):
        make(settings, store, servers, FakeClock(NOW))


def test_unknown_stream_fails_before_anything_runs(
    settings: Settings, store: StateStore, servers: Servers
) -> None:
    pipeline = make(settings, store, servers, FakeClock(NOW))

    with pytest.raises(KeyError, match="nope"):
        pipeline.run("series", ["hourly", "nope"])
    assert servers.series.requests == []
