"""Elia connector against recorded exports (tests/fixtures/elia/README.md)."""

import logging
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from fakes import FakeClock
from toeval.config import Settings
from toeval.connectors.base import FetchRequest, RunMode
from toeval.connectors.elia import BASE_URL, DATASETS, EliaConnector, where_clause
from toeval.connectors.http import HttpClient
from toeval.connectors.planning import TimeWindow
from toeval.state import StateStore, Watermark

FIXTURES = Path(__file__).parent / "fixtures" / "elia"
WHERE_RE = re.compile(r"^datetime >= '(.+?)' and datetime < '(.+?)'$")
NO_SLEEP = {"sleep": lambda _s: None}

# Fixture facts (see the README there).
LOAD_LAST_MEASURED = datetime(2026, 10, 18, 8, tzinfo=UTC)
DAY_LAST_QUARTER = datetime(2026, 10, 17, 21, 45, tzinfo=UTC)
NOW = datetime(2026, 10, 18, 19, tzinfo=UTC)


@dataclass
class EliaServer:
    """Serves each dataset's fixture, filtered by the export's `where` clause like the real API."""

    tables: dict[str, pa.Table] = field(default_factory=dict)
    requests: list[httpx.Request] = field(default_factory=list)

    @classmethod
    def recorded(cls) -> "EliaServer":
        ids = [ds.dataset_id for ds in DATASETS.values()]
        return cls({i: pq.read_table(FIXTURES / f"{i}.parquet") for i in ids})

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        match = re.fullmatch(
            r"/api/explore/v2\.1/catalog/datasets/(\w+)/exports/parquet", request.url.path
        )
        assert match, request.url
        where = WHERE_RE.match(request.url.params["where"])
        assert where, request.url.params["where"]
        start, end = (datetime.fromisoformat(v) for v in where.groups())
        table = self.tables[match.group(1)]
        rows = table.filter(
            pc.and_(pc.greater_equal(table["datetime"], start), pc.less(table["datetime"], end))
        )
        sink = pa.BufferOutputStream()
        pq.write_table(rows, sink)
        return httpx.Response(200, content=sink.getvalue().to_pybytes())

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handler)

    def windows(self) -> list[tuple[datetime, datetime]]:
        out = []
        for r in self.requests:
            where = WHERE_RE.match(r.url.params["where"])
            assert where
            start, end = (datetime.fromisoformat(v) for v in where.groups())
            out.append((start, end))
        return out

    def tamper(self, dataset_id: str, change: Callable[[pa.Table], pa.Table]) -> None:
        self.tables[dataset_id] = change(self.tables[dataset_id])


@pytest.fixture
def server() -> EliaServer:
    return EliaServer.recorded()


def make(
    settings: Settings, store: StateStore, server: EliaServer, clock: FakeClock
) -> EliaConnector:
    return EliaConnector(
        settings, store, transport=server.transport(), clock=clock, http_options=NO_SLEEP
    )


def raw_rows(settings: Settings, stream: str) -> int:
    files = list((settings.raw_path / "elia" / stream).rglob("*.parquet"))
    return sum(pq.read_metadata(f).num_rows for f in files)


# -- planning ------------------------------------------------------------------------------------


def test_where_clause_uses_utc_z_timestamps() -> None:
    window = TimeWindow(
        datetime(2014, 12, 31, 23, tzinfo=UTC), datetime(2015, 12, 31, 23, tzinfo=UTC)
    )

    assert where_clause(window) == (
        "datetime >= '2014-12-31T23:00:00Z' and datetime < '2015-12-31T23:00:00Z'"
    )


def test_first_load_run_exports_full_history_in_yearly_chunks(
    settings: Settings, store: StateStore, server: EliaServer
) -> None:
    result = make(settings, store, server, FakeClock(NOW)).run("load")

    assert result.succeeded, result.error
    windows = server.windows()
    assert windows[0][0] == datetime(2014, 12, 31, 23, tzinfo=UTC)
    assert all(end - start <= timedelta(days=365) for start, end in windows)
    assert all(a[1] == b[0] for a, b in pairwise(windows))  # contiguous
    assert windows[-1][1] >= NOW + timedelta(days=8)  # covers the week-ahead forecast rows
    assert all(r.url.path.endswith("/ods001/exports/parquet") for r in server.requests)
    assert str(server.requests[0].url).startswith(BASE_URL)


def test_forecast_rows_are_stored_but_do_not_advance_the_watermark(
    settings: Settings, store: StateStore, server: EliaServer
) -> None:
    result = make(settings, store, server, FakeClock(NOW)).run("load")

    assert result.watermark_after == Watermark.timestamp(LOAD_LAST_MEASURED)
    assert result.stats.rows_fetched == 288
    assert raw_rows(settings, "load") == 288  # forecast rows are kept in raw


def test_next_day_refetches_the_revision_window(
    settings: Settings, store: StateStore, server: EliaServer
) -> None:
    clock = FakeClock(NOW)
    make(settings, store, server, clock).run("load")
    server.requests.clear()
    clock.advance(timedelta(days=1))

    result = make(settings, store, server, clock).run("load")

    assert result.succeeded, result.error
    # watermark - 14 days = 2026-10-04 08:00, floored to the 23:00Z day grid of backfill_start.
    assert server.windows()[0][0] == datetime(2026, 10, 3, 23, tzinfo=UTC)


def test_rerun_on_the_same_day_writes_nothing_new(
    settings: Settings, store: StateStore, server: EliaServer
) -> None:
    clock = FakeClock(NOW)
    make(settings, store, server, clock).run("load")  # first run: history-sized chunks
    clock.advance(timedelta(minutes=30))
    make(settings, store, server, clock).run("load")  # first incremental window: new key
    clock.advance(timedelta(minutes=30))

    result = make(settings, store, server, clock).run("load")

    assert result.succeeded, result.error
    assert result.stats.rows_fetched == 288
    assert result.stats.files_written == 0


def test_wind_and_solar_have_no_forecast_horizon(
    settings: Settings, store: StateStore, server: EliaServer
) -> None:
    connector = make(settings, store, server, FakeClock(NOW))

    for stream, dataset in (("wind", "ods031"), ("solar", "ods032")):
        server.requests.clear()
        result = connector.run(stream)

        assert result.succeeded, result.error
        assert result.watermark_after == Watermark.timestamp(DAY_LAST_QUARTER)
        assert server.windows()[-1][1] < NOW + DATASETS[stream].chunk
        assert all(r.url.path.endswith(f"/{dataset}/exports/parquet") for r in server.requests)
        assert all(end - start <= DATASETS[stream].chunk for start, end in server.windows())


def test_solar_history_starts_in_october_2020(
    settings: Settings, store: StateStore, server: EliaServer
) -> None:
    make(settings, store, server, FakeClock(NOW)).run("solar")

    assert server.windows()[0][0] == datetime(2020, 9, 30, 22, tzinfo=UTC)


def test_backfill_of_one_year(settings: Settings, store: StateStore, server: EliaServer) -> None:
    result = make(settings, store, server, FakeClock(NOW)).run(
        "wind",
        RunMode.BACKFILL,
        backfill_from=datetime(2025, 1, 1, tzinfo=UTC),
        backfill_to=datetime(2026, 1, 1, tzinfo=UTC),
    )

    assert result.succeeded, result.error
    windows = server.windows()
    assert windows[0][0] == datetime(2025, 1, 1, tzinfo=UTC)
    assert windows[-1][1] == datetime(2026, 1, 1, tzinfo=UTC)


# -- fetching and quality checks -----------------------------------------------------------------


def test_rows_are_sorted_by_natural_key_so_unchanged_data_hashes_the_same(
    settings: Settings, store: StateStore, server: EliaServer
) -> None:
    shuffled = server.tables["ods032"].take(pa.array(range(1343, -1, -1)))
    server.tables["ods032"] = shuffled
    connector = make(settings, store, server, FakeClock(NOW))
    http = HttpClient(
        connector.source.http, connector.source.auth, user_agent="t", transport=server.transport()
    )
    window = TimeWindow(
        datetime(2026, 10, 16, 22, tzinfo=UTC), datetime(2026, 10, 17, 22, tzinfo=UTC)
    )

    table = connector.fetch(
        connector.source.stream("solar"),
        FetchRequest(window.key, {"where": where_clause(window)}),
        http,
    ).table

    assert table is not None
    keys = list(zip(table["datetime"].to_pylist(), table["region"].to_pylist(), strict=True))
    assert keys == sorted(keys)


def test_solar_total_that_disagrees_with_regions_is_quarantined(
    settings: Settings, store: StateStore, server: EliaServer
) -> None:
    def inflate_belgium_at_noon(t: pa.Table) -> pa.Table:
        noon = datetime(2026, 10, 17, 11, tzinfo=UTC)
        hit = pc.and_(pc.equal(t["region"], "Belgium"), pc.equal(t["datetime"], noon))
        measured = pc.if_else(hit, pc.add(t["measured"], 50.0), t["measured"])
        return t.set_column(t.schema.get_field_index("measured"), "measured", measured)

    server.tamper("ods032", inflate_belgium_at_noon)

    result = make(settings, store, server, FakeClock(NOW)).run("solar")

    assert not result.succeeded
    assert "solar hierarchy: 1 quarter-hours" in (result.error or "")
    assert "2026-10-17T11:00:00Z" in (result.error or "")
    assert list(settings.quarantine_path.rglob("*.parquet"))
    assert raw_rows(settings, "solar") == 0
    assert store.get_stream_state("elia", "solar").watermark is None


def test_solar_check_tolerates_tiny_differences_at_night(
    settings: Settings, store: StateStore, server: EliaServer
) -> None:
    def nudge_belgium(t: pa.Table) -> pa.Table:
        hit = pc.equal(t["region"], "Belgium")
        measured = pc.if_else(hit, pc.add(t["measured"], 0.5), t["measured"])  # < 1 MW floor
        return t.set_column(t.schema.get_field_index("measured"), "measured", measured)

    server.tamper("ods032", nudge_belgium)

    assert make(settings, store, server, FakeClock(NOW)).run("solar").succeeded


def test_unknown_wind_part_is_ingested_with_a_warning(
    settings: Settings,
    store: StateStore,
    server: EliaServer,
    caplog: pytest.LogCaptureFixture,
) -> None:
    def add_brussels_part(t: pa.Table) -> pa.Table:
        row = t.slice(0, 1).to_pylist()[0] | {"region": "Brussels", "gridconnectiontype": "Dso"}
        return pa.concat_tables([t, pa.Table.from_pylist([row], schema=t.schema)])

    server.tamper("ods031", add_brussels_part)

    with caplog.at_level(logging.WARNING, logger="toeval.connectors.elia"):
        result = make(settings, store, server, FakeClock(NOW)).run("wind")

    assert result.succeeded, result.error
    assert raw_rows(settings, "wind") == 481
    assert "unknown parts [('Onshore', 'Brussels', 'Dso')]" in caplog.text


def test_server_errors_are_retried(
    settings: Settings, store: StateStore, server: EliaServer
) -> None:
    failures = [503]

    def flaky(request: httpx.Request) -> httpx.Response:
        if failures:
            return httpx.Response(failures.pop())
        return server.handler(request)

    connector = EliaConnector(
        settings,
        store,
        transport=httpx.MockTransport(flaky),
        clock=FakeClock(NOW),
        http_options=NO_SLEEP,
    )

    assert connector.run("load").succeeded


# -- live ----------------------------------------------------------------------------------------


@pytest.mark.live
def test_live_one_day_load_export(settings: Settings, store: StateStore) -> None:
    connector = EliaConnector(settings, store)
    stream = connector.source.stream("load")
    window = TimeWindow(datetime(2025, 6, 1, 22, tzinfo=UTC), datetime(2025, 6, 2, 22, tzinfo=UTC))
    http = HttpClient(connector.source.http, connector.source.auth, user_agent=settings.user_agent)
    try:
        table = connector.fetch(
            stream, FetchRequest(window.key, {"where": where_clause(window)}), http
        ).table
    finally:
        http.close()

    assert table is not None
    assert table.num_rows == 96
    assert table.schema.field("datetime").type == pa.timestamp("ms", tz="UTC")
    assert table["totalload"].null_count == 0
    assert connector.validate(stream, table) == []
