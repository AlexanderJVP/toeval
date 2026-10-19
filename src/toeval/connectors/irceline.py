"""IRCEL-CELINE air quality (https://www.irceline.be): hourly NO2, PM10, PM2.5 and O3, 15 stations.

Facts verified 2026-10-20 (see docs/decisions/0009-irceline-air-quality.md):

- 52°North timeseries REST API at `https://geo.irceline.be/sos/api/v1/`, no key, CC BY 4.0
  (credit IRCEL-CELINE). No documented rate limit, so requests are paced politely.
- A *timeseries* is one pollutant at one station (`/timeseries?phenomenon=<id>&expanded=true`
  lists them, with station label, unit and first/last value). Each curated station has one per
  pollutant today; an instrument change would start a new one, so the series are looked up on
  every run and the series id is kept per row.
- `/timeseries/<id>/getData?timespan=<start>/<end>` returns `{"values": [{timestamp, value}]}`
  with both ends included, epoch milliseconds in UTC. A timespan must be shorter than 367 days
  (HTTP 400 otherwise); ranges before the history or in the future return an empty list.
- An hourly value's timestamp is the **end** of its hour (IRCEL's open_data README: "hmean:
  timestamp = end-time of hour"; the series that start in 2011 start at 2011-01-01 01:00 UTC).
  Raw keeps the API timestamps; the alignment to `hour_start_utc` happens in the marts.
- Missing hours come back as `null` (dropped here). Data is under ~3 hours behind real time.
- Values are provisional when published; whether validated values later replace them in this API
  is not documented. The deep re-check and `revision_lag` measure it.
"""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from toeval.connectors.base import Connector, FetchRequest, FetchResult, RunContext
from toeval.connectors.http import HttpClient
from toeval.connectors.planning import time_window_plan
from toeval.connectors.spec import (
    DailySchedule,
    DeepRefetch,
    HttpPolicy,
    SourceSpec,
    StreamSpec,
)
from toeval.state import StreamState, Watermark, WatermarkKind

log = logging.getLogger(__name__)

BASE_URL = "https://geo.irceline.be/sos/api/v1"
UNIT = "µg/m³"

# Pollutant -> the API's phenomenon id.
POLLUTANTS = {"no2": "8", "pm10": "5", "pm25": "6001", "o3": "7"}


@dataclass(frozen=True)
class Station:
    code: str  # the national station code, the start of the API's station label
    near: str  # the nearest Open-Meteo location (connectors/openmeteo.py)


# Chosen 2026-10-20 (ADR 0009): long history (most since 2011/2012), all four pollutants where
# measured, near the Open-Meteo points, several in Antwerp. No station near Ostend has a long NO2
# record, so the west is covered by Houtem (near De Panne) and Roeselare. 54 series in total.
STATIONS = (
    Station("42R801", "antwerp"),  # Borgerhout, all four
    Station("42R802", "antwerp"),  # Borgerhout, same coordinates as 42R801, no O3
    Station("42M802", "antwerp"),  # Antwerpen, north of the centre
    Station("42R804", "antwerp"),  # Antwerpen (Ring), since 2017
    Station("40AL01", "antwerp"),  # Linkeroever, PM only
    Station("41R001", "brussels"),  # Molenbeek-Saint-Jean
    Station("41R012", "brussels"),  # Uccle
    Station("44R701", "ghent"),  # Gent
    Station("44R740", "ghent"),  # Sint-Kruiswinkel (port area), PM2.5 since 2023
    Station("43R222", "liege"),  # Liège (ISSeP)
    Station("43R240", "liege"),  # Engis (Meuse valley)
    Station("43N073", "brussels"),  # Vezin (Namur), between Brussels and Liège
    Station("44N029", "ostend"),  # Houtem, near the coast
    Station("44M705", "ostend"),  # Roeselare
    Station("43N121", "libramont"),  # Offagne, Ardennes
)

SCHEMA = pa.schema(
    [
        pa.field("station", pa.string()),
        pa.field("station_label", pa.string()),
        pa.field("latitude", pa.float64()),
        pa.field("longitude", pa.float64()),
        pa.field("pollutant", pa.string()),
        pa.field("series_id", pa.string()),
        pa.field("time", pa.timestamp("s", tz="UTC")),  # end of the hour
        pa.field("value", pa.float64()),  # µg/m³, hourly mean
    ]
)


@dataclass(frozen=True)
class Series:
    id: str
    station: str
    station_label: str
    latitude: float
    longitude: float
    pollutant: str
    first: datetime
    last: datetime


class IrcelineConnector(Connector):
    spec = SourceSpec(
        name="irceline",
        # Undocumented limits: at most 2 requests/s. A yearly window is one GET per series
        # (~54, ~350 kB each), so the 2011 -> today history is ~850 GETs in ~15 minutes.
        http=HttpPolicy(requests_per_second=2, timeout=timedelta(seconds=60)),
        streams=(
            StreamSpec(
                name="hourly",
                schedule=DailySchedule(at=time(12, 0)),
                watermark=WatermarkKind.TIMESTAMP,
                natural_key=("station", "pollutant", "time"),
                revision_window=timedelta(days=14),
                # Provisional: the regions validate a year's data for EU reporting during the
                # next year; whether this API then replaces values is undocumented.
                deep_refetch=DeepRefetch(every=timedelta(days=30), window=timedelta(days=400)),
                # Published within ~3 h and fetched daily at 12:00: ~27 h old just before a run.
                freshness_limit=timedelta(hours=36),
                revision_lag_limit=timedelta(days=320),  # 80% of the deep window
                # The first hour with data, 2011-01-01 00:00-01:00 UTC, has timestamp 01:00.
                backfill_start=datetime(2011, 1, 1, tzinfo=UTC),
            ),
        ),
    )

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._series: list[Series] | None = None  # looked up once per run

    def plan(self, stream: StreamSpec, state: StreamState, ctx: RunContext) -> list[FetchRequest]:
        self._series = None
        return [
            FetchRequest(
                window.key, {"start": window.start.isoformat(), "end": window.end.isoformat()}
            )
            for window in time_window_plan(stream, state, ctx, chunk=timedelta(days=365))
        ]

    def fetch(self, stream: StreamSpec, request: FetchRequest, http: HttpClient) -> FetchResult:
        start = datetime.fromisoformat(request.params["start"])
        end = datetime.fromisoformat(request.params["end"])
        parts = []
        for series in self._lookup(http):
            if series.last < start or series.first >= end:
                continue  # no values in this window
            r = http.get(
                f"{BASE_URL}/timeseries/{series.id}/getData", {"timespan": timespan(start, end)}
            )
            parts.append(parse_values(series, r.json(), start, end))
        table = pa.concat_tables(parts) if parts else SCHEMA.empty_table()
        return FetchResult(table.sort_by([("station", "ascending"), ("pollutant", "ascending"),
                                          ("time", "ascending")]))  # fmt: skip

    def next_watermark(
        self, stream: StreamSpec, current: Watermark | None, table: pa.Table
    ) -> Watermark | None:
        if table.num_rows == 0:
            return None
        return Watermark.timestamp(pc.max(table["time"]).as_py())

    def validate(self, stream: StreamSpec, table: pa.Table) -> list[str]:
        if table.num_rows == 0:
            return []
        minutes = pc.unique(pc.minute(table["time"])).to_pylist()
        if minutes != [0]:
            return [f"timestamps not on the hour (minutes {sorted(minutes)})"]
        return []

    def _lookup(self, http: HttpClient) -> list[Series]:
        if self._series is None:
            self._series = []
            for pollutant, phenomenon in POLLUTANTS.items():
                r = http.get(
                    f"{BASE_URL}/timeseries", {"phenomenon": phenomenon, "expanded": "true"}
                )
                self._series.extend(select_series(pollutant, r.json()))
            found = {(s.station, s.pollutant) for s in self._series}
            log.info("irceline: %d series at %d stations", len(self._series),
                     len({s.station for s in self._series}))  # fmt: skip
            missing = [s.code for s in STATIONS if not any(f[0] == s.code for f in found)]
            if missing:
                log.warning("irceline: no series found for stations %s", missing)
        return self._series


def timespan(start: datetime, end: datetime) -> str:
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    return f"{start.astimezone(UTC):{fmt}}/{end.astimezone(UTC):{fmt}}"


def select_series(pollutant: str, body: Any) -> list[Series]:
    """The curated stations' series of one pollutant from `/timeseries?expanded=true`."""
    if not isinstance(body, list):
        raise ValueError(f"expected a list of timeseries, got {str(body)[:200]}")
    codes = {s.code for s in STATIONS}
    out = []
    for item in body:
        label = item["station"]["properties"]["label"]
        code = label.split("-")[0].strip()  # "42R801 - Borgerhout", also "44EG08- Evergem"
        if code not in codes or "firstValue" not in item:
            continue
        if item.get("uom") != UNIT:
            raise ValueError(f"{label} {pollutant}: unexpected unit {item.get('uom')!r}")
        lon, lat = item["station"]["geometry"]["coordinates"][:2]
        out.append(
            Series(
                id=str(item["id"]),
                station=code,
                station_label=label,
                latitude=float(lat),
                longitude=float(lon),
                pollutant=pollutant,
                first=_time(item["firstValue"]["timestamp"]),
                last=_time(item["lastValue"]["timestamp"]),
            )
        )
    return out


def parse_values(series: Series, body: Any, start: datetime, end: datetime) -> pa.Table:
    """One series' `getData` response as rows inside [start, end), without the null hours."""
    if not isinstance(body, dict) or not isinstance(body.get("values"), list):
        raise ValueError(f"series {series.id}: unexpected response {str(body)[:200]}")
    times, values = [], []
    for v in body["values"]:
        t = _time(v["timestamp"])
        if v["value"] is not None and start <= t < end:
            times.append(t)
            values.append(float(v["value"]))
    n = len(times)
    return pa.table(
        {
            "station": [series.station] * n,
            "station_label": [series.station_label] * n,
            "latitude": [series.latitude] * n,
            "longitude": [series.longitude] * n,
            "pollutant": [series.pollutant] * n,
            "series_id": [series.id] * n,
            "time": times,
            "value": values,
        },
        schema=SCHEMA,
    )


def _time(epoch_ms: int) -> datetime:
    return datetime.fromtimestamp(epoch_ms / 1000, UTC)


__all__ = ["POLLUTANTS", "STATIONS", "IrcelineConnector", "parse_values", "select_series"]
