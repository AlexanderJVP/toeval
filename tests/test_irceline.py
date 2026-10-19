"""IRCEL-CELINE connector against recorded API responses (tests/fixtures/irceline/README.md)."""

import json
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from pathlib import Path
from typing import Any

import httpx
import pyarrow.parquet as pq
import pytest

from fakes import FakeClock
from toeval.config import Settings
from toeval.connectors.base import FetchRequest
from toeval.connectors.http import HttpClient
from toeval.connectors.irceline import BASE_URL, POLLUTANTS, STATIONS, IrcelineConnector
from toeval.state import StateStore, Watermark

FIXTURES = Path(__file__).parent / "fixtures" / "irceline"
NO_SLEEP = {"sleep": lambda _s: None}

# Fixture facts (see the README there): 54 series, values 2026-10-17 00:00 .. 2026-10-20 00:00Z
# (both ends), 3,725 non-null ones before 10-20 00:00, the latest at 2026-10-19 22:00.
SERIES = 54
VALUES = 3725
LAST_VALUE = datetime(2026, 10, 19, 22, tzinfo=UTC)
NOW = datetime(2026, 10, 19, 23, 30, tzinfo=UTC)
RING_NO2 = "10761"  # 42R804 (Antwerpen Ring), first value in 2017


def ms(t: datetime) -> int:
    return int(t.timestamp() * 1000)


@dataclass
class SosServer:
    """Serves the fixture like the real API: inclusive timespans, at most 366 days."""

    listings: dict[str, list[dict[str, Any]]] = field(
        default_factory=lambda: json.loads((FIXTURES / "timeseries.json").read_text("utf-8"))
    )
    data: dict[str, dict[str, Any]] = field(
        default_factory=lambda: json.loads(
            (FIXTURES / "getData-2026-10-17_2026-10-20.json").read_text("utf-8")
        )
    )
    requests: list[httpx.Request] = field(default_factory=list)

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path.removeprefix("/sos/api/v1")
        if path == "/timeseries":
            assert request.url.params["expanded"] == "true"
            return httpx.Response(200, json=self.listings[request.url.params["phenomenon"]])
        series_id = path.split("/")[2]
        start, end = (datetime.fromisoformat(t) for t in request.url.params["timespan"].split("/"))
        if end - start >= timedelta(days=367):
            reason = "Requested timespan is to long, please use a period shorter than 'P367D'"
            return httpx.Response(400, json={"statusCode": 400, "userMessage": reason})
        values = [v for v in self.data[series_id]["values"]
                  if ms(start) <= v["timestamp"] <= ms(end)]  # fmt: skip
        return httpx.Response(200, json={"values": values})

    def data_requests(self) -> list[tuple[str, datetime, datetime]]:
        out = []
        for r in self.requests:
            if r.url.path.endswith("/getData"):
                a, b = r.url.params["timespan"].split("/")
                out.append((r.url.path.split("/")[-2], datetime.fromisoformat(a),
                            datetime.fromisoformat(b)))  # fmt: skip
        return out

    def listing_requests(self) -> int:
        return sum(1 for r in self.requests if r.url.path.endswith("/timeseries"))


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock(NOW)


@pytest.fixture
def server() -> SosServer:
    return SosServer()


def make(settings: Settings, store: StateStore, server: SosServer, clock: FakeClock) -> Any:
    return IrcelineConnector(
        settings,
        store,
        transport=httpx.MockTransport(server.handler),
        clock=clock,
        http_options=NO_SLEEP,
    )


def raw_tables(settings: Settings) -> list[Any]:
    return [pq.read_table(f) for f in sorted((settings.raw_path / "irceline").rglob("*.parquet"))]


def test_first_run_fetches_the_history_in_windows_the_api_accepts(
    settings: Settings, store: StateStore, server: SosServer, clock: FakeClock
) -> None:
    result = make(settings, store, server, clock).run("hourly")

    assert result.succeeded, result.error
    windows = sorted({(a, b) for _, a, b in server.data_requests()})
    assert windows[0][0] == datetime(2011, 1, 1, tzinfo=UTC)
    assert all(b - a <= timedelta(days=365) for a, b in windows)
    assert all(w1[1] == w2[0] for w1, w2 in pairwise(windows))  # contiguous
    assert windows[-1][1] > NOW
    assert server.listing_requests() == len(POLLUTANTS)  # once per run, not per window
    assert str(server.requests[0].url).startswith(BASE_URL)


def test_series_are_only_asked_for_windows_they_have_values_in(
    settings: Settings, store: StateStore, server: SosServer, clock: FakeClock
) -> None:
    make(settings, store, server, clock).run("hourly")

    ring = [(a, b) for sid, a, b in server.data_requests() if sid == RING_NO2]
    assert min(b for _, b in ring) > datetime(2017, 1, 1, tzinfo=UTC)
    per_window = Counter((a, b) for _, a, b in server.data_requests())
    assert max(per_window.values()) == SERIES


def test_null_hours_are_dropped_and_the_watermark_is_the_last_value(
    settings: Settings, store: StateStore, server: SosServer, clock: FakeClock
) -> None:
    result = make(settings, store, server, clock).run("hourly")
    (table,) = raw_tables(settings)  # one file per window with data, not one per series

    assert result.watermark_after == Watermark.timestamp(LAST_VALUE)
    assert table.num_rows == VALUES
    assert table["value"].null_count == 0
    assert set(table["station"].to_pylist()) == {s.code for s in STATIONS}
    assert set(table["pollutant"].to_pylist()) == set(POLLUTANTS)
    assert len(set(table["series_id"].to_pylist())) == SERIES
    assert max(table["time"].to_pylist()) == LAST_VALUE
    # The window ends at 2026-10-20 00:00 or later: no row at or past the window's end.
    assert table.column_names[:3] == ["station", "station_label", "latitude"]


def test_next_day_refetches_the_revision_window_for_every_series(
    settings: Settings, store: StateStore, server: SosServer, clock: FakeClock
) -> None:
    make(settings, store, server, clock).run("hourly")
    server.requests.clear()
    clock.advance(timedelta(days=1))

    result = make(settings, store, server, clock).run("hourly")

    assert result.succeeded, result.error
    windows = {(a, b) for _, a, b in server.data_requests()}
    # watermark 10-19 22:00 - 14 days, floored to a day; the end is on the 365-day chunk grid
    # (the API returns nothing for the future, and the span stays under 367 days).
    assert windows == {(datetime(2026, 10, 5, tzinfo=UTC), datetime(2026, 12, 28, tzinfo=UTC))}
    assert len(server.data_requests()) == SERIES
    assert server.listing_requests() == len(POLLUTANTS)


def test_rerun_on_the_same_day_writes_nothing_new(
    settings: Settings, store: StateStore, server: SosServer, clock: FakeClock
) -> None:
    make(settings, store, server, clock).run("hourly")
    clock.advance(timedelta(minutes=30))
    make(settings, store, server, clock).run("hourly")  # first incremental window: new key
    clock.advance(timedelta(minutes=30))

    result = make(settings, store, server, clock).run("hourly")

    assert result.succeeded, result.error
    assert result.stats.rows_fetched == VALUES
    assert result.stats.files_written == 0


def test_an_unexpected_unit_fails_the_run(
    settings: Settings, store: StateStore, server: SosServer, clock: FakeClock
) -> None:
    server.listings[POLLUTANTS["pm10"]][0]["uom"] = "mg/m³"

    result = make(settings, store, server, clock).run("hourly")

    assert not result.succeeded
    assert "unexpected unit" in (result.error or "")
    assert raw_tables(settings) == []


def test_every_curated_station_is_in_the_recorded_listing() -> None:
    listings = json.loads((FIXTURES / "timeseries.json").read_text("utf-8"))
    labels = {t["station"]["properties"]["label"].split("-")[0].strip()
              for items in listings.values() for t in items}  # fmt: skip
    assert labels == {s.code for s in STATIONS}
    assert sum(len(items) for items in listings.values()) == SERIES


# -- live ----------------------------------------------------------------------------------------


@pytest.mark.live
def test_live_one_day_for_all_series(settings: Settings, store: StateStore) -> None:
    connector = IrcelineConnector(settings, store)
    stream = connector.source.stream("hourly")
    http = HttpClient(connector.source.http, connector.source.auth, user_agent=settings.user_agent)
    params = {"start": "2025-06-01T00:00:00+00:00", "end": "2025-06-02T00:00:00+00:00"}
    try:
        table = connector.fetch(stream, FetchRequest("live", params), http).table
    finally:
        http.close()

    assert table is not None
    assert len(set(table["series_id"].to_pylist())) >= SERIES - 3  # a station may be down
    assert table.num_rows > 0.9 * SERIES * 24
    assert min(table["time"].to_pylist()) >= datetime(2025, 6, 1, tzinfo=UTC)
    assert connector.validate(stream, table) == []
