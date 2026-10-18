"""Fake sources with the three shapes the interface must support (docs/decisions/0004):

- FakeSeriesConnector: queryable history + revisions (Elia, Open-Meteo, Telraam)
- FakeLiveConnector:   live snapshots, ETag/304, frequent polling (iRail)
- FakePeriodConnector: a file with full history, period watermark, monthly (Statbel)
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, time, timedelta

import httpx
import pyarrow as pa
import pyarrow.compute as pc

from toeval.connectors.base import Connector, FetchRequest, FetchResult, RunContext
from toeval.connectors.http import HttpClient
from toeval.connectors.planning import time_window_plan
from toeval.connectors.spec import (
    DailySchedule,
    DeepRefetch,
    HttpPolicy,
    IntervalSchedule,
    MonthlySchedule,
    SourceSpec,
    StreamSpec,
)
from toeval.state import StreamState, Watermark, WatermarkKind

ORIGIN = datetime(2026, 1, 1, tzinfo=UTC)
FAST_HTTP = HttpPolicy(requests_per_second=1000, burst=1000, max_retries=2)


@dataclass
class FakeClock:
    now: datetime

    def __call__(self) -> datetime:
        return self.now

    def advance(self, delta: timedelta) -> None:
        self.now += delta


# -- time series ----------------------------------------------------------------------------------


@dataclass
class SeriesServer:
    """Hourly values; `None` means published but not measured yet (like Elia forecasts)."""

    values: dict[datetime, float | None] = field(default_factory=dict)
    requests: list[httpx.Request] = field(default_factory=list)
    fail_next: list[int] = field(default_factory=list)  # status codes to return first

    def set_range(self, start: datetime, end: datetime, value: Callable[[datetime], float | None]):
        t = start
        while t < end:
            self.values[t] = value(t)
            t += timedelta(hours=1)

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.fail_next:
            return httpx.Response(self.fail_next.pop(0))
        start = datetime.fromisoformat(request.url.params["start"])
        end = datetime.fromisoformat(request.url.params["end"])
        rows = [
            {"datetime": t.isoformat(), "value": v}
            for t, v in sorted(self.values.items())
            if start <= t < end
        ]
        return httpx.Response(200, json=rows)

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handler)


class FakeSeriesConnector(Connector):
    spec = SourceSpec(
        name="series",
        http=FAST_HTTP,
        streams=(
            StreamSpec(
                name="hourly",
                schedule=DailySchedule(at=time(6, 30)),
                watermark=WatermarkKind.TIMESTAMP,
                natural_key=("datetime",),
                revision_window=timedelta(days=2),
                deep_refetch=DeepRefetch(every=timedelta(days=30), window=timedelta(days=10)),
                backfill_start=ORIGIN,
            ),
        ),
    )
    CHUNK = timedelta(days=7)

    def plan(self, stream: StreamSpec, state: StreamState, ctx: RunContext) -> list[FetchRequest]:
        return [
            FetchRequest(w.key, {"start": w.start, "end": w.end})
            for w in time_window_plan(stream, state, ctx, chunk=self.CHUNK)
        ]

    def fetch(self, stream: StreamSpec, request: FetchRequest, http: HttpClient) -> FetchResult:
        params = {k: v.isoformat() for k, v in request.params.items()}
        rows = http.get("https://fake.test/series", params).json()
        table = pa.table(
            {
                "datetime": pa.array(
                    [datetime.fromisoformat(r["datetime"]) for r in rows],
                    type=pa.timestamp("us", tz="UTC"),
                ),
                "value": pa.array([r["value"] for r in rows], type=pa.float64()),
            }
        )
        return FetchResult(table)

    def next_watermark(
        self, stream: StreamSpec, current: Watermark | None, table: pa.Table
    ) -> Watermark | None:
        measured = table.filter(pc.is_valid(table["value"]))
        if measured.num_rows == 0:
            return None
        return Watermark.timestamp(pc.max(measured["datetime"]).as_py())


# -- live snapshots ------------------------------------------------------------------------------


@dataclass
class LiveServer:
    boards: dict[str, list[dict]] = field(default_factory=dict)
    versions: dict[str, int] = field(default_factory=dict)
    requests: list[httpx.Request] = field(default_factory=list)

    def publish(self, station: str, departures: list[dict]) -> None:
        self.boards[station] = departures
        self.versions[station] = self.versions.get(station, 0) + 1

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        station = request.url.params["station"]
        etag = f'"{station}-{self.versions[station]}"'
        if request.headers.get("If-None-Match") == etag:
            return httpx.Response(304)
        return httpx.Response(200, json=self.boards[station], headers={"ETag": etag})


class FakeLiveConnector(Connector):
    STATIONS = ("Antwerpen-Centraal", "Berchem")
    spec = SourceSpec(
        name="live",
        http=FAST_HTTP,
        streams=(
            StreamSpec(
                name="departures",
                schedule=IntervalSchedule(every=timedelta(minutes=5)),
                watermark=WatermarkKind.NONE,
                natural_key=("station", "vehicle", "scheduled"),
            ),
        ),
    )

    def plan(self, stream: StreamSpec, state: StreamState, ctx: RunContext) -> list[FetchRequest]:
        return [FetchRequest(f"station={s}", {"station": s}) for s in self.STATIONS]

    def fetch(self, stream: StreamSpec, request: FetchRequest, http: HttpClient) -> FetchResult:
        r = http.get(
            "https://fake.test/liveboard", dict(request.params), conditional_key=request.key
        )
        if r.not_modified:
            return FetchResult.not_modified()
        rows = [{"station": request.params["station"], **d} for d in r.json()]
        schema = pa.schema(
            [("station", pa.string()), ("vehicle", pa.string()), ("scheduled", pa.string()),
             ("delay", pa.int64())]
        )  # fmt: skip
        return FetchResult(pa.Table.from_pylist(rows, schema=schema))

    def next_watermark(
        self, stream: StreamSpec, current: Watermark | None, table: pa.Table
    ) -> Watermark | None:
        return None


# -- periodic files ------------------------------------------------------------------------------


@dataclass
class PeriodServer:
    rows: list[dict] = field(default_factory=list)
    last_modified: str = "Mon, 05 Oct 2026 09:00:00 GMT"
    requests: list[httpx.Request] = field(default_factory=list)

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.headers.get("If-Modified-Since") == self.last_modified:
            return httpx.Response(304)
        return httpx.Response(200, json=self.rows, headers={"Last-Modified": self.last_modified})


class FakePeriodConnector(Connector):
    spec = SourceSpec(
        name="stats",
        http=FAST_HTTP,
        streams=(
            StreamSpec(
                name="cpi",
                schedule=MonthlySchedule(day=5, at=time(9, 0)),
                watermark=WatermarkKind.PERIOD,
                natural_key=("period",),
            ),
        ),
    )

    def plan(self, stream: StreamSpec, state: StreamState, ctx: RunContext) -> list[FetchRequest]:
        return [FetchRequest("cpi")]

    def fetch(self, stream: StreamSpec, request: FetchRequest, http: HttpClient) -> FetchResult:
        r = http.get("https://fake.test/cpi.json", conditional_key=request.key)
        if r.not_modified:
            return FetchResult.not_modified()
        return FetchResult(pa.Table.from_pylist(r.json()))

    def next_watermark(
        self, stream: StreamSpec, current: Watermark | None, table: pa.Table
    ) -> Watermark | None:
        return Watermark.period(pc.max(table["period"]).as_py())
