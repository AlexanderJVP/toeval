"""Elia Open Data (https://opendata.elia.be): Belgian grid load, wind and solar per quarter-hour.

Facts verified 2026-10-18 (see CLAUDE.md "Elia API facts" and docs/decisions/0003):

- Opendatasoft Explore v2.1, no key. `/records` caps offset + limit at 10,000, so every request
  is a `/exports/parquet?where=...` export of one time window. Exports are slow for the
  multi-row datasets (one year: load ~7 s, wind ~48 s, solar ~119 s), hence the per-dataset
  chunk sizes and the 180 s timeout.
- `datetime` is the interval start (UTC, `timestamp[ms, tz=UTC]`), resolution PT15M, values in MW.
- Load (ods001) includes ~7 days of forecast rows with `totalload` null. Wind (ods031) and solar
  (ods032) end at the last completed Brussels day and have no forecast rows.
- Exports send no ETag/Last-Modified and return rows in no particular order. Identical windows
  download byte-identical, so after sorting, unchanged windows are skipped by the raw dedup.
"""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from toeval.connectors.base import Connector, FetchRequest, FetchResult, RunContext
from toeval.connectors.http import HttpClient
from toeval.connectors.planning import TimeWindow, time_window_plan
from toeval.connectors.spec import (
    DailySchedule,
    DeepRefetch,
    HttpPolicy,
    SourceSpec,
    StreamSpec,
)
from toeval.state import StreamState, Watermark, WatermarkKind

log = logging.getLogger(__name__)

BASE_URL = "https://opendata.elia.be/api/explore/v2.1/catalog/datasets"

# The five disjoint wind parts (offshoreonshore, region, gridconnectiontype); there is no total.
WIND_PARTS = frozenset(
    {
        ("Offshore", "Federal", "Elia"),
        ("Onshore", "Flanders", "Dso"),
        ("Onshore", "Flanders", "Elia"),
        ("Onshore", "Wallonia", "Dso"),
        ("Onshore", "Wallonia", "Elia"),
    }
)
SOLAR_TOTAL = "Belgium"
SOLAR_REGIONS = ("Flanders", "Wallonia", "Brussels")
# Belgium matched the sum of the regions to 1e-12 MW over all of 2025; the absolute floor keeps
# the relative tolerance meaningful at night when solar is ~0.
SOLAR_TOLERANCE_REL = 0.01
SOLAR_TOLERANCE_ABS_MW = 1.0


@dataclass(frozen=True)
class EliaDataset:
    dataset_id: str
    measured_column: str  # non-null means measured; null rows are forecasts / not yet measured
    chunk: timedelta  # export window size, sized so one export stays well under the timeout
    horizon: timedelta  # how far past now the source publishes rows (forecasts)


DATASETS = {
    "load": EliaDataset("ods001", "totalload", timedelta(days=365), timedelta(days=8)),
    "wind": EliaDataset("ods031", "measured", timedelta(days=182), timedelta(0)),
    "solar": EliaDataset("ods032", "measured", timedelta(days=91), timedelta(0)),
}


def _stream(name: str, natural_key: tuple[str, ...], backfill_start: datetime) -> StreamSpec:
    return StreamSpec(
        name=name,
        # Elia processes these datasets in the (UTC) morning; a run missed today catches up.
        schedule=DailySchedule(at=time(12, 0)),
        watermark=WatermarkKind.TIMESTAMP,
        natural_key=natural_key,
        revision_window=timedelta(days=14),
        deep_refetch=DeepRefetch(every=timedelta(days=30), window=timedelta(days=90)),
        backfill_start=backfill_start,
    )


class EliaConnector(Connector):
    spec = SourceSpec(
        name="elia",
        http=HttpPolicy(timeout=timedelta(seconds=180)),
        streams=(
            # 2015-01-01 00:00 Brussels; solar history starts 2020-10-01 00:00 Brussels.
            _stream("load", ("datetime",), datetime(2014, 12, 31, 23, tzinfo=UTC)),
            _stream(
                "wind",
                ("datetime", "offshoreonshore", "region", "gridconnectiontype"),
                datetime(2014, 12, 31, 23, tzinfo=UTC),
            ),
            _stream("solar", ("datetime", "region"), datetime(2020, 9, 30, 22, tzinfo=UTC)),
        ),
    )

    def plan(self, stream: StreamSpec, state: StreamState, ctx: RunContext) -> list[FetchRequest]:
        ds = DATASETS[stream.name]
        windows = time_window_plan(stream, state, ctx, chunk=ds.chunk, horizon=ds.horizon)
        return [FetchRequest(w.key, {"where": where_clause(w)}) for w in windows]

    def fetch(self, stream: StreamSpec, request: FetchRequest, http: HttpClient) -> FetchResult:
        ds = DATASETS[stream.name]
        r = http.get(f"{BASE_URL}/{ds.dataset_id}/exports/parquet", dict(request.params))
        table = pq.read_table(pa.BufferReader(r.content))
        return FetchResult(table.sort_by([(c, "ascending") for c in stream.natural_key]))

    def next_watermark(
        self, stream: StreamSpec, current: Watermark | None, table: pa.Table
    ) -> Watermark | None:
        measured = table.filter(pc.is_valid(table[DATASETS[stream.name].measured_column]))
        if measured.num_rows == 0:
            return None
        return Watermark.timestamp(pc.max(measured["datetime"]).as_py())

    def validate(self, stream: StreamSpec, table: pa.Table) -> list[str]:
        if table.num_rows == 0:
            return []
        _warn_on_unexpected_resolution(stream, table)
        if stream.name == "wind":
            _warn_on_unknown_wind_parts(table)
        if stream.name == "solar":
            return solar_hierarchy_failures(table)
        return []


def where_clause(window: TimeWindow) -> str:
    def fmt(t: datetime) -> str:
        return t.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    return f"datetime >= '{fmt(window.start)}' and datetime < '{fmt(window.end)}'"


def solar_hierarchy_failures(table: pa.Table) -> list[str]:
    """The Belgium row must equal Flanders + Wallonia + Brussels (docs/decisions/0003).

    Only quarter-hours where the total and all three regions are measured are compared;
    incomplete hierarchies are logged, not failed, because they say nothing about consistency.
    """
    con = duckdb.connect()
    con.register("solar", table.select(["datetime", "region", "measured"]))
    regions = ", ".join(f"'{r}'" for r in SOLAR_REGIONS)
    bad, incomplete, example = con.execute(
        f"""
        WITH per_quarter AS (
            SELECT datetime,
                   max(measured) FILTER (WHERE region = '{SOLAR_TOTAL}') AS total,
                   sum(measured) FILTER (WHERE region IN ({regions})) AS regions_sum,
                   count(measured) FILTER (WHERE region IN ({regions})) AS regions_measured
            FROM solar GROUP BY datetime
        )
        SELECT
            count(*) FILTER (WHERE regions_measured = 3 AND abs(total - regions_sum)
                             > greatest(? * abs(total), ?)),
            count(*) FILTER (WHERE total IS NOT NULL AND regions_measured < 3),
            min(strftime(datetime AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ'))
                FILTER (WHERE regions_measured = 3 AND abs(total - regions_sum)
                        > greatest(? * abs(total), ?))
        FROM per_quarter
        """,
        [SOLAR_TOLERANCE_REL, SOLAR_TOLERANCE_ABS_MW] * 2,
    ).fetchone()  # type: ignore[misc]
    con.close()
    if incomplete:
        log.warning("solar: %d quarter-hours have a Belgium total but not all regions", incomplete)
    if bad:
        return [
            f"solar hierarchy: {bad} quarter-hours where {SOLAR_TOTAL} differs from the sum of "
            f"{', '.join(SOLAR_REGIONS)} by more than "
            f"max({SOLAR_TOLERANCE_REL:.0%}, {SOLAR_TOLERANCE_ABS_MW} MW) (first: {example})"
        ]
    return []


def _warn_on_unknown_wind_parts(table: pa.Table) -> None:
    parts = table.select(["offshoreonshore", "region", "gridconnectiontype"])
    seen = {
        tuple(row.values()) for row in parts.group_by(parts.column_names).aggregate([]).to_pylist()
    }
    unknown = seen - WIND_PARTS
    if unknown:
        log.warning(
            "wind: unknown parts %s; raw keeps them, but check docs/decisions/0003 before "
            "trusting the national sum",
            sorted(unknown),
        )


def _warn_on_unexpected_resolution(stream: StreamSpec, table: pa.Table) -> None:
    resolutions = set(pc.unique(table["resolutioncode"]).to_pylist())
    if resolutions - {"PT15M"}:
        log.warning("%s: unexpected resolutions %s", stream.name, sorted(resolutions))


__all__ = ["DATASETS", "EliaConnector", "solar_hierarchy_failures", "where_clause"]
