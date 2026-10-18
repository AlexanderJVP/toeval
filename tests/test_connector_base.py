"""The run loop, exercised through the three fake source shapes in fakes.py."""

from datetime import UTC, datetime, timedelta

import pyarrow.parquet as pq
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
)
from toeval.config import Settings
from toeval.connectors.base import RunMode, UnsupportedModeError
from toeval.state import StateStore, Watermark

NO_SLEEP = {"sleep": lambda _s: None}


def make_series(
    settings: Settings, store: StateStore, server: SeriesServer, clock: FakeClock
) -> FakeSeriesConnector:
    return FakeSeriesConnector(
        settings, store, transport=server.transport(), clock=clock, http_options=NO_SLEEP
    )


def requested_starts(server: SeriesServer) -> list[datetime]:
    return [datetime.fromisoformat(r.url.params["start"]) for r in server.requests]


@pytest.fixture
def series_server() -> SeriesServer:
    server = SeriesServer()
    # Measured up to Jan 20 12:00, then a day of forecast-only rows (value None).
    server.set_range(ORIGIN, datetime(2026, 1, 20, 12, tzinfo=UTC), lambda t: float(t.hour))
    server.set_range(
        datetime(2026, 1, 20, 12, tzinfo=UTC), datetime(2026, 1, 21, 12, tzinfo=UTC), lambda t: None
    )
    return server


# -- time series ---------------------------------------------------------------------------------


def test_first_run_fetches_full_history_and_sets_watermark_to_last_measured(
    settings: Settings, store: StateStore, series_server: SeriesServer
) -> None:
    clock = FakeClock(datetime(2026, 1, 20, 13, tzinfo=UTC))

    result = make_series(settings, store, series_server, clock).run("hourly")

    assert result.succeeded
    assert requested_starts(series_server)[0] == ORIGIN
    assert result.watermark_after == Watermark.timestamp(datetime(2026, 1, 20, 11, tzinfo=UTC))
    assert result.stats.rows_fetched == (19 * 24 + 12) + 24  # measured + forecast-only rows
    assert store.get_stream_state("series", "hourly").watermark == result.watermark_after


def test_incremental_run_refetches_the_revision_window_before_the_watermark(
    settings: Settings, store: StateStore, series_server: SeriesServer
) -> None:
    clock = FakeClock(datetime(2026, 1, 20, 13, tzinfo=UTC))
    make_series(settings, store, series_server, clock).run("hourly")
    series_server.requests.clear()
    clock.advance(timedelta(days=1))

    make_series(settings, store, series_server, clock).run("hourly")

    # watermark Jan 20 11:00 minus the 2-day revision window
    assert requested_starts(series_server)[0] == datetime(2026, 1, 18, 11, tzinfo=UTC)


def test_missed_runs_catch_up_from_the_old_watermark(
    settings: Settings, store: StateStore, series_server: SeriesServer
) -> None:
    clock = FakeClock(datetime(2026, 1, 20, 13, tzinfo=UTC))
    make_series(settings, store, series_server, clock).run("hourly")

    # Laptop off for three weeks; meanwhile the source published that whole period.
    series_server.set_range(
        datetime(2026, 1, 20, 12, tzinfo=UTC), datetime(2026, 2, 10, 12, tzinfo=UTC), lambda t: 1.0
    )
    series_server.requests.clear()
    clock.now = datetime(2026, 2, 10, 13, tzinfo=UTC)

    result = make_series(settings, store, series_server, clock).run("hourly")

    starts = requested_starts(series_server)
    assert starts[0] == datetime(2026, 1, 18, 11, tzinfo=UTC)  # old watermark - window
    assert len(starts) == 4  # several chunks, no gap
    assert result.watermark_after == Watermark.timestamp(datetime(2026, 2, 10, 11, tzinfo=UTC))


def test_failed_run_does_not_advance_the_watermark(
    settings: Settings, store: StateStore, series_server: SeriesServer
) -> None:
    clock = FakeClock(datetime(2026, 1, 20, 13, tzinfo=UTC))
    make_series(settings, store, series_server, clock).run("hourly")
    before = store.get_stream_state("series", "hourly")
    series_server.set_range(
        datetime(2026, 1, 20, 12, tzinfo=UTC), datetime(2026, 1, 25, tzinfo=UTC), lambda t: 1.0
    )
    series_server.fail_next = [404]
    clock.advance(timedelta(days=4))

    result = make_series(settings, store, series_server, clock).run("hourly")

    assert not result.succeeded
    assert "404" in (result.error or "")
    assert store.get_stream_state("series", "hourly") == before
    assert store.get_run(result.run_id)["status"] == "failed"


def test_rerun_without_changes_writes_no_new_raw_files(
    settings: Settings, store: StateStore, series_server: SeriesServer
) -> None:
    clock = FakeClock(datetime(2026, 1, 20, 13, tzinfo=UTC))
    make_series(settings, store, series_server, clock).run("hourly")  # initial history
    clock.advance(timedelta(hours=1))
    first = make_series(settings, store, series_server, clock).run("hourly")
    clock.advance(timedelta(minutes=10))

    repeat = make_series(settings, store, series_server, clock).run("hourly")

    assert first.stats.files_written == 1  # new request key (watermark - window)
    assert repeat.succeeded
    assert repeat.stats.files_written == 0  # same keys, same content


def test_revised_value_is_written_and_carries_the_newer_ingestion_time(
    settings: Settings, store: StateStore, series_server: SeriesServer
) -> None:
    clock = FakeClock(datetime(2026, 1, 20, 13, tzinfo=UTC))
    make_series(settings, store, series_server, clock).run("hourly")
    series_server.values[datetime(2026, 1, 20, 10, tzinfo=UTC)] = 99.0  # source corrects a value
    clock.advance(timedelta(hours=1))

    result = make_series(settings, store, series_server, clock).run("hourly")

    assert result.stats.files_written == 1
    latest = store.raw_files("series", "hourly")[-1]
    table = pq.read_table(settings.raw_path / latest.path)
    assert 99.0 in table["value"].to_pylist()
    assert set(table["_ingested_at"].to_pylist()) == {clock.now}


def test_deep_refetch_runs_monthly_and_looks_further_back(
    settings: Settings, store: StateStore, series_server: SeriesServer
) -> None:
    clock = FakeClock(datetime(2026, 1, 20, 13, tzinfo=UTC))
    make_series(settings, store, series_server, clock).run("hourly")  # first run counts as deep

    series_server.requests.clear()
    clock.advance(timedelta(days=1))
    make_series(settings, store, series_server, clock).run("hourly")
    normal_start = requested_starts(series_server)[0]

    series_server.requests.clear()
    clock.advance(timedelta(days=30))
    make_series(settings, store, series_server, clock).run("hourly")
    deep_start = requested_starts(series_server)[0]

    assert normal_start == datetime(2026, 1, 18, 11, tzinfo=UTC)  # 2-day window
    assert deep_start == datetime(2026, 1, 10, 11, tzinfo=UTC)  # 10-day deep window


def test_backfill_of_an_old_range_never_moves_the_watermark_back(
    settings: Settings, store: StateStore, series_server: SeriesServer
) -> None:
    clock = FakeClock(datetime(2026, 1, 20, 13, tzinfo=UTC))
    connector = make_series(settings, store, series_server, clock)
    connector.run("hourly")
    watermark = store.get_stream_state("series", "hourly").watermark

    result = connector.run(
        "hourly",
        RunMode.BACKFILL,
        backfill_from=ORIGIN,
        backfill_to=datetime(2026, 1, 5, tzinfo=UTC),
    )

    assert result.succeeded
    assert store.get_stream_state("series", "hourly").watermark == watermark


def test_duplicate_natural_keys_fail_the_quality_gate_and_are_quarantined(
    settings: Settings, store: StateStore
) -> None:
    server = SeriesServer()
    t = datetime(2026, 1, 1, 5, tzinfo=UTC)
    server.values[t] = 1.0
    real_handler = server.handler

    def duplicating(request):  # type: ignore[no-untyped-def]
        response = real_handler(request)
        rows = response.json()
        return type(response)(200, json=rows + rows)

    server.handler = duplicating  # type: ignore[method-assign]
    clock = FakeClock(datetime(2026, 1, 2, tzinfo=UTC))

    result = make_series(settings, store, server, clock).run("hourly")

    assert not result.succeeded
    assert "duplicate natural keys" in (result.error or "")
    assert store.raw_files("series", "hourly") == []
    assert list((settings.raw_path / "_quarantine").rglob("*.parquet"))


# -- live snapshots ------------------------------------------------------------------------------


def make_live(
    settings: Settings, store: StateStore, server: LiveServer, clock: FakeClock
) -> FakeLiveConnector:
    import httpx

    return FakeLiveConnector(
        settings,
        store,
        transport=httpx.MockTransport(server.handler),
        clock=clock,
        http_options=NO_SLEEP,
    )


def test_live_stream_uses_etags_and_skips_unchanged_boards(
    settings: Settings, store: StateStore
) -> None:
    server = LiveServer()
    server.publish("Antwerpen-Centraal", [{"vehicle": "IC1", "scheduled": "08:00", "delay": 0}])
    server.publish("Berchem", [{"vehicle": "S1", "scheduled": "08:02", "delay": 60}])
    clock = FakeClock(datetime(2026, 10, 18, 8, tzinfo=UTC))
    make_live(settings, store, server, clock).run("departures")

    server.publish("Berchem", [{"vehicle": "S1", "scheduled": "08:02", "delay": 180}])
    clock.advance(timedelta(minutes=5))
    result = make_live(settings, store, server, clock).run("departures")

    assert result.succeeded
    assert result.stats.not_modified == 1  # Antwerpen-Centraal answered 304
    assert result.stats.files_written == 1  # only Berchem changed
    assert result.watermark_after is None
    assert server.requests[-2].headers["If-None-Match"] == '"Antwerpen-Centraal-1"'


def test_live_stream_refuses_backfill(settings: Settings, store: StateStore) -> None:
    connector = make_live(settings, store, LiveServer(), FakeClock(datetime.now(UTC)))

    with pytest.raises(UnsupportedModeError, match="no queryable history"):
        connector.run("departures", RunMode.BACKFILL)


# -- periodic files ------------------------------------------------------------------------------


def test_period_stream_tracks_reference_period_and_uses_last_modified(
    settings: Settings, store: StateStore
) -> None:
    import httpx

    server = PeriodServer(
        rows=[{"period": "2026-08", "value": 120.1}, {"period": "2026-09", "value": 120.4}]
    )

    def connector() -> FakePeriodConnector:
        return FakePeriodConnector(
            settings,
            store,
            transport=httpx.MockTransport(server.handler),
            clock=FakeClock(datetime(2026, 10, 5, 8, tzinfo=UTC)),
            http_options=NO_SLEEP,
        )

    first = connector().run("cpi")
    second = connector().run("cpi")

    assert first.watermark_after == Watermark.period("2026-09")
    assert second.stats.not_modified == 1
    assert second.watermark_after == Watermark.period("2026-09")
    assert server.requests[-1].headers["If-Modified-Since"] == server.last_modified
