"""Open-Meteo historical weather (https://open-meteo.com): ERA5 reanalysis, hourly, 7 points.

Facts verified 2026-10-19 (see docs/decisions/0008-open-meteo-weather.md):

- `GET https://archive-api.open-meteo.com/v1/archive`, no key. Free for non-commercial use under
  CC-BY 4.0 (credit Open-Meteo and Copernicus ERA5). Limits: 600 calls/minute, 5,000/hour,
  10,000/day, where a request costs one call per location per 2 weeks of data (and more above 10
  variables). One yearly request for all 7 locations therefore costs ~182 calls.
- ERA5 only (`models=era5`, 0.25° grid, covers the sea): one consistent model since 1940. It
  runs ~5-6 days behind; hours it doesn't cover yet come back as nulls, not as an error.
- `start_date` / `end_date` are inclusive dates and must not be after today (else HTTP 400).
- Several locations per request: comma-separated coordinates, the response is a list in request
  order. Each point snaps to its nearest grid cell (returned as `latitude` / `longitude`).
- Times are requested in GMT. Most values are instantaneous at `time`; `precipitation` (sum) and
  `shortwave_radiation` (mean) cover the hour *before* `time`. Raw keeps the API's timestamps;
  the alignment to Elia's interval starts happens in the marts (ADR 0008).
"""

import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

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

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
MODEL = "era5"


@dataclass(frozen=True)
class Location:
    name: str
    latitude: float
    longitude: float


# Each lands on its own ERA5 cell (checked 2026-10-19; the cell is stored per row in raw).
LOCATIONS = (
    Location("antwerp", 51.22, 4.40),
    Location("brussels", 50.85, 4.35),
    Location("ghent", 51.05, 3.72),
    Location("liege", 50.63, 5.57),
    Location("ostend", 51.23, 2.92),
    Location("libramont", 49.92, 5.38),  # Ardennes: higher, cooler, different sun
    Location("offshore", 51.60, 2.90),  # Belgian offshore wind zone; snaps to a sea cell
)

# Name -> unit as the API reports it with `wind_speed_unit=ms`. Kept at 10 or fewer variables,
# so a request isn't charged extra for them.
VARIABLES = {
    "temperature_2m": "°C",
    "relative_humidity_2m": "%",
    "precipitation": "mm",  # sum over the preceding hour
    "cloud_cover": "%",
    "pressure_msl": "hPa",
    "wind_speed_10m": "m/s",
    "wind_speed_100m": "m/s",
    "wind_direction_100m": "°",
    "shortwave_radiation": "W/m²",  # mean over the preceding hour
}

SCHEMA = pa.schema(
    [
        pa.field("location", pa.string()),
        pa.field("grid_latitude", pa.float64()),
        pa.field("grid_longitude", pa.float64()),
        pa.field("time", pa.timestamp("s", tz="UTC")),
        *(pa.field(name, pa.float64()) for name in VARIABLES),
    ]
)


class OpenMeteoConnector(Connector):
    spec = SourceSpec(
        name="openmeteo",
        # One yearly request costs ~182 calls (7 locations x 26 two-week units). One request per
        # 25 s keeps a backfill at ~440 calls/minute (limit 600) and its ~2,200 calls in ~5 min
        # (limit 5,000/hour). An incremental run is one request of ~10 calls.
        http=HttpPolicy(requests_per_second=1 / 25, timeout=timedelta(seconds=120)),
        streams=(
            StreamSpec(
                name="hourly",
                schedule=DailySchedule(at=time(12, 0)),
                watermark=WatermarkKind.TIMESTAMP,
                natural_key=("location", "time"),
                revision_window=timedelta(days=14),
                # Provisional: Copernicus is said to replace preliminary ERA5 (ERA5T) data with
                # final values after 2-3 months; Open-Meteo doesn't document it. revision_lag
                # will show whether it happens.
                deep_refetch=DeepRefetch(every=timedelta(days=30), window=timedelta(days=120)),
                # A day before Elia's first interval (2014-12-31 23:00 UTC), on a UTC-midnight
                # grid so every window is a whole number of API dates.
                backfill_start=datetime(2014, 12, 31, tzinfo=UTC),
            ),
        ),
    )

    def plan(self, stream: StreamSpec, state: StreamState, ctx: RunContext) -> list[FetchRequest]:
        today = ctx.now.astimezone(UTC).date()
        requests = []
        for window in time_window_plan(stream, state, ctx, chunk=timedelta(days=365)):
            first, last = _dates(window)
            last = min(last, today)  # the API refuses dates after today
            if first > last:
                continue
            requests.append(
                FetchRequest(
                    window.key,
                    {
                        "start": window.start.isoformat(),
                        "end": window.end.isoformat(),
                        "start_date": first.isoformat(),
                        "end_date": last.isoformat(),
                    },
                )
            )
        return requests

    def fetch(self, stream: StreamSpec, request: FetchRequest, http: HttpClient) -> FetchResult:
        p = request.params
        r = http.get(BASE_URL, query(p["start_date"], p["end_date"]))
        table = parse(r.json())
        # Rows outside the window would overlap the neighbouring request (and a backfill range
        # needn't start at midnight); hours ERA5 doesn't cover yet carry no information.
        start, end = datetime.fromisoformat(p["start"]), datetime.fromisoformat(p["end"])
        inside = pc.and_(
            pc.greater_equal(table["time"], pa.scalar(start, SCHEMA.field("time").type)),
            pc.less(table["time"], pa.scalar(end, SCHEMA.field("time").type)),
        )
        table = table.filter(pc.and_(inside, _any_value(table)))
        return FetchResult(table.sort_by([("location", "ascending"), ("time", "ascending")]))

    def next_watermark(
        self, stream: StreamSpec, current: Watermark | None, table: pa.Table
    ) -> Watermark | None:
        if table.num_rows == 0:
            return None
        return Watermark.timestamp(pc.max(table["time"]).as_py())

    def validate(self, stream: StreamSpec, table: pa.Table) -> list[str]:
        if table.num_rows == 0:
            return []
        failures = []
        cells = table.group_by("location").aggregate(
            [("grid_latitude", "max"), ("grid_longitude", "max")]
        )
        coords = list(zip(cells["grid_latitude_max"].to_pylist(),
                          cells["grid_longitude_max"].to_pylist(), strict=True))  # fmt: skip
        if len(set(coords)) < len(coords):
            failures.append(f"locations share an ERA5 grid cell: {sorted(coords)}")
        minutes = pc.unique(pc.minute(table["time"])).to_pylist()
        if minutes != [0]:
            failures.append(f"timestamps not on the hour (minutes {sorted(minutes)})")
        return failures


def query(start_date: str, end_date: str) -> dict[str, str]:
    return {
        "latitude": ",".join(f"{loc.latitude:.2f}" for loc in LOCATIONS),
        "longitude": ",".join(f"{loc.longitude:.2f}" for loc in LOCATIONS),
        "hourly": ",".join(VARIABLES),
        "models": MODEL,
        "wind_speed_unit": "ms",
        "timezone": "GMT",
        "start_date": start_date,
        "end_date": end_date,
    }


def parse(body: Any) -> pa.Table:
    """The archive's JSON (one object per location, in request order) as one long table."""
    if not isinstance(body, list) or len(body) != len(LOCATIONS):
        raise ValueError(f"expected a list of {len(LOCATIONS)} locations, got {str(body)[:200]}")
    parts = []
    for loc, item in zip(LOCATIONS, body, strict=True):
        if item.get("utc_offset_seconds") != 0:
            raise ValueError(f"{loc.name}: times are not in GMT ({item.get('timezone')})")
        units = item["hourly_units"]
        wrong = {k: units.get(k) for k, unit in VARIABLES.items() if units.get(k) != unit}
        if wrong:
            raise ValueError(f"{loc.name}: unexpected units {wrong}")
        hourly = item["hourly"]
        times = [datetime.fromisoformat(t).replace(tzinfo=UTC) for t in hourly["time"]]
        n = len(times)
        parts.append(
            pa.table(
                {
                    "location": [loc.name] * n,
                    "grid_latitude": [float(item["latitude"])] * n,
                    "grid_longitude": [float(item["longitude"])] * n,
                    "time": times,
                    **{k: [None if v is None else float(v) for v in hourly[k]] for k in VARIABLES},
                },
                schema=SCHEMA,
            )
        )
    return pa.concat_tables(parts)


def _dates(window: TimeWindow) -> tuple[date, date]:
    """The inclusive API dates that cover [start, end)."""
    first = window.start.astimezone(UTC).date()
    last = (window.end.astimezone(UTC) - timedelta(microseconds=1)).date()
    return first, last


def _any_value(table: pa.Table) -> pa.ChunkedArray:
    mask = pc.is_valid(table[next(iter(VARIABLES))])
    for name in VARIABLES:
        mask = pc.or_(mask, pc.is_valid(table[name]))
    return mask


__all__ = ["LOCATIONS", "VARIABLES", "OpenMeteoConnector", "parse", "query"]
