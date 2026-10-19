"""Open-Meteo connector against a recorded archive response (tests/fixtures/openmeteo/README.md)."""

import json
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from itertools import pairwise
from pathlib import Path
from typing import Any

import httpx
import pyarrow.parquet as pq
import pytest

from fakes import FakeClock
from toeval.config import Settings
from toeval.connectors.base import FetchRequest, RunMode
from toeval.connectors.http import HttpClient
from toeval.connectors.openmeteo import (
    BASE_URL,
    LOCATIONS,
    VARIABLES,
    OpenMeteoConnector,
    parse,
    query,
)
from toeval.state import StateStore, Watermark

FIXTURE = Path(__file__).parent / "fixtures" / "openmeteo" / "archive-2026-10-12_2026-10-19.json"
NO_SLEEP = {"sleep": lambda _s: None}

# Fixture facts (see the README there): 2026-10-12 .. 2026-10-19, ERA5 values up to 10-13 23:00.
LAST_VALUE = datetime(2026, 10, 13, 23, tzinfo=UTC)
NOW = datetime(2026, 10, 19, 19, tzinfo=UTC)


@dataclass
class ArchiveServer:
    """Serves the fixture's hours within [start_date, end_date] and, like the real API, refuses
    dates after today."""

    clock: FakeClock
    body: list[dict[str, Any]] = field(
        default_factory=lambda: json.loads(FIXTURE.read_text("utf-8"))
    )
    requests: list[httpx.Request] = field(default_factory=list)

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        params = request.url.params
        assert (params["models"], params["timezone"], params["wind_speed_unit"]) == (
            "era5",
            "GMT",
            "ms",
        )
        first, last = (
            date.fromisoformat(params["start_date"]),
            date.fromisoformat(params["end_date"]),
        )
        today = self.clock().date()
        for name, value in (("start_date", first), ("end_date", last)):
            if value > today:
                reason = f"Parameter '{name}' is out of allowed range from 1940-01-01 to {today}"
                return httpx.Response(400, json={"error": True, "reason": reason})
        out = []
        for item in self.body:
            hourly = item["hourly"]
            keep = [first <= date.fromisoformat(t[:10]) <= last for t in hourly["time"]]
            out.append(
                item
                | {
                    "hourly": {
                        k: [v for v, ok in zip(vs, keep, strict=True) if ok]
                        for k, vs in hourly.items()
                    }
                }
            )
        return httpx.Response(200, json=out)

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handler)

    def dates(self) -> list[tuple[date, date]]:
        return [
            (
                date.fromisoformat(r.url.params["start_date"]),
                date.fromisoformat(r.url.params["end_date"]),
            )
            for r in self.requests
        ]


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock(NOW)


@pytest.fixture
def server(clock: FakeClock) -> ArchiveServer:
    return ArchiveServer(clock)


def make(settings: Settings, store: StateStore, server: ArchiveServer) -> OpenMeteoConnector:
    return OpenMeteoConnector(
        settings, store, transport=server.transport(), clock=server.clock, http_options=NO_SLEEP
    )


def raw_table(settings: Settings) -> Any:
    files = sorted((settings.raw_path / "openmeteo" / "hourly").rglob("*.parquet"))
    return [pq.read_table(f) for f in files]


# -- requests and planning -----------------------------------------------------------------------


def test_query_asks_era5_for_all_locations_in_gmt_and_m_per_s() -> None:
    q = query("2025-01-01", "2025-12-31")

    assert q["latitude"].split(",")[-1] == "51.60"  # offshore
    assert len(q["longitude"].split(",")) == len(LOCATIONS) == 7
    assert q["hourly"].split(",") == list(VARIABLES)
    assert len(VARIABLES) <= 10  # more would make every request cost extra calls
    assert (q["models"], q["timezone"], q["wind_speed_unit"]) == ("era5", "GMT", "ms")


def test_first_run_fetches_the_history_in_yearly_requests_up_to_today(
    settings: Settings, store: StateStore, server: ArchiveServer
) -> None:
    result = make(settings, store, server).run("hourly")

    assert result.succeeded, result.error
    dates = server.dates()
    assert dates[0][0] == date(2014, 12, 31)
    assert all(last - first < timedelta(days=365) for first, last in dates)
    assert all(a[1] + timedelta(days=1) == b[0] for a, b in pairwise(dates))  # contiguous
    assert dates[-1][1] == NOW.date()  # never after today: the API would refuse it
    assert str(server.requests[0].url).startswith(BASE_URL)


def test_hours_era5_does_not_cover_yet_are_dropped_and_set_no_watermark(
    settings: Settings, store: StateStore, server: ArchiveServer
) -> None:
    result = make(settings, store, server).run("hourly")
    (table,) = raw_table(settings)

    assert result.watermark_after == Watermark.timestamp(LAST_VALUE)
    assert table.num_rows == 7 * 48  # 10-12 and 10-13; the null hours after are not stored
    assert table["time"].to_pylist()[-1] == LAST_VALUE
    assert sorted(set(table["location"].to_pylist())) == sorted(loc.name for loc in LOCATIONS)
    assert table.column_names[:4] == ["location", "grid_latitude", "grid_longitude", "time"]


def test_offshore_point_snaps_to_its_own_sea_cell(
    settings: Settings, store: StateStore, server: ArchiveServer
) -> None:
    make(settings, store, server).run("hourly")
    (table,) = raw_table(settings)
    cells = {
        (loc, lat, lon)
        for loc, lat, lon in zip(
            table["location"].to_pylist(),
            table["grid_latitude"].to_pylist(),
            table["grid_longitude"].to_pylist(),
            strict=True,
        )
    }

    assert ("offshore", 51.5, 3.0) in cells
    assert len({(lat, lon) for _, lat, lon in cells}) == 7


def test_next_day_refetches_the_revision_window(
    settings: Settings, store: StateStore, server: ArchiveServer, clock: FakeClock
) -> None:
    make(settings, store, server).run("hourly")
    server.requests.clear()
    clock.advance(timedelta(days=1))

    result = make(settings, store, server).run("hourly")

    assert result.succeeded, result.error
    # watermark 10-13 23:00 - 14 days, floored to a UTC day; one request of ~10 calls.
    assert server.dates() == [(date(2026, 9, 29), date(2026, 10, 20))]


def test_rerun_on_the_same_day_writes_nothing_new(
    settings: Settings, store: StateStore, server: ArchiveServer, clock: FakeClock
) -> None:
    make(settings, store, server).run("hourly")
    clock.advance(timedelta(minutes=30))
    make(settings, store, server).run("hourly")  # first incremental window: new key
    clock.advance(timedelta(minutes=30))

    result = make(settings, store, server).run("hourly")

    assert result.succeeded, result.error
    assert result.stats.rows_fetched == 7 * 48
    assert result.stats.files_written == 0


def test_monthly_deep_refetch_goes_back_120_days(
    settings: Settings, store: StateStore, server: ArchiveServer, clock: FakeClock
) -> None:
    make(settings, store, server).run("hourly")
    server.requests.clear()
    clock.advance(timedelta(days=31))

    make(settings, store, server).run("hourly")

    first = server.dates()[0][0]
    assert first == (LAST_VALUE - timedelta(days=120)).date()


def test_backfill_keeps_only_rows_inside_the_requested_range(
    settings: Settings, store: StateStore, server: ArchiveServer
) -> None:
    # 2026-10-13 00:00 Brussels is 10-12 22:00 UTC: the API dates cover more than that.
    result = make(settings, store, server).run(
        "hourly",
        RunMode.BACKFILL,
        backfill_from=datetime(2026, 10, 12, 22, tzinfo=UTC),
        backfill_to=datetime(2026, 10, 13, 22, tzinfo=UTC),
    )
    (table,) = raw_table(settings)

    assert result.succeeded, result.error
    assert server.dates() == [(date(2026, 10, 12), date(2026, 10, 13))]
    assert table.num_rows == 7 * 24
    assert min(table["time"].to_pylist()) == datetime(2026, 10, 12, 22, tzinfo=UTC)


# -- parsing and quality checks ------------------------------------------------------------------


def body() -> list[dict[str, Any]]:
    return json.loads(FIXTURE.read_text("utf-8"))


def test_unexpected_units_fail_the_parse() -> None:
    changed = body()
    changed[3]["hourly_units"]["wind_speed_100m"] = "km/h"

    with pytest.raises(ValueError, match="liege: unexpected units"):
        parse(changed)


def test_a_response_with_missing_locations_fails_the_parse() -> None:
    with pytest.raises(ValueError, match="list of 7 locations"):
        parse(body()[:6])


def test_two_locations_on_one_grid_cell_are_quarantined(
    settings: Settings, store: StateStore, server: ArchiveServer
) -> None:
    server.body[1] |= {
        "latitude": server.body[0]["latitude"],
        "longitude": server.body[0]["longitude"],
    }

    result = make(settings, store, server).run("hourly")

    assert not result.succeeded
    assert "share an ERA5 grid cell" in (result.error or "")
    assert list(settings.quarantine_path.rglob("*.parquet"))
    assert raw_table(settings) == []


# -- live ----------------------------------------------------------------------------------------


@pytest.mark.live
def test_live_one_day_for_all_locations(settings: Settings, store: StateStore) -> None:
    connector = OpenMeteoConnector(settings, store)
    stream = connector.source.stream("hourly")
    http = HttpClient(connector.source.http, connector.source.auth, user_agent=settings.user_agent)
    params = {
        "start": "2025-06-01T00:00:00+00:00",
        "end": "2025-06-02T00:00:00+00:00",
        "start_date": "2025-06-01",
        "end_date": "2025-06-01",
    }
    try:
        table = connector.fetch(stream, FetchRequest("live", params), http).table
    finally:
        http.close()

    assert table is not None
    assert table.num_rows == 7 * 24
    assert table["temperature_2m"].null_count == 0
    assert connector.validate(stream, table) == []
