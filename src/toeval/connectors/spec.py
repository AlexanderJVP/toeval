"""Declarative source configuration. See docs/decisions/0004-connector-interface.md.

A *source* is one provider (e.g. Elia); a *stream* is one dataset of it (e.g. ods001) with its own
schedule, watermark and state. Defaults live in code next to each connector; operational knobs can
be overridden per source/stream from `sources.toml`.
"""

import re
import tomllib
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Annotated, Any, Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, model_validator

from toeval.state import WatermarkKind

BRUSSELS = ZoneInfo("Europe/Brussels")

_DURATION_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h|d|w)\s*$")
_UNITS = {
    "ms": timedelta(milliseconds=1),
    "s": timedelta(seconds=1),
    "m": timedelta(minutes=1),
    "h": timedelta(hours=1),
    "d": timedelta(days=1),
    "w": timedelta(weeks=1),
}


def parse_duration(value: Any) -> Any:
    """Accept "500ms", "30s", "5m", "6h", "14d", "2w" in addition to anything pydantic accepts."""
    if isinstance(value, str) and (match := _DURATION_RE.match(value)):
        amount, unit = match.groups()
        return float(amount) * _UNITS[unit]
    return value


Duration = Annotated[timedelta, BeforeValidator(parse_duration)]


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


# -- schedules ----------------------------------------------------------------------------------


class IntervalSchedule(_Frozen):
    kind: Literal["interval"] = "interval"
    every: Duration

    def next_after(self, t: datetime) -> datetime:
        return t + self.every


class DailySchedule(_Frozen):
    """Once a day at a Europe/Brussels wall-clock time."""

    kind: Literal["daily"] = "daily"
    at: time

    def next_after(self, t: datetime) -> datetime:
        local = t.astimezone(BRUSSELS)
        candidate = datetime.combine(local.date(), self.at, BRUSSELS)
        if candidate <= local:
            candidate = datetime.combine(local.date() + timedelta(days=1), self.at, BRUSSELS)
        return candidate.astimezone(UTC)


class MonthlySchedule(_Frozen):
    """Once a month on a given day at a Europe/Brussels wall-clock time."""

    kind: Literal["monthly"] = "monthly"
    day: int = Field(ge=1, le=28)
    at: time = time(9, 0)

    def next_after(self, t: datetime) -> datetime:
        local = t.astimezone(BRUSSELS)
        candidate = datetime.combine(date(local.year, local.month, self.day), self.at, BRUSSELS)
        if candidate <= local:
            year, month = (
                (local.year + 1, 1) if local.month == 12 else (local.year, local.month + 1)
            )
            candidate = datetime.combine(date(year, month, self.day), self.at, BRUSSELS)
        return candidate.astimezone(UTC)


Schedule = Annotated[
    IntervalSchedule | DailySchedule | MonthlySchedule, Field(discriminator="kind")
]


def is_due(schedule: Schedule, last_success: datetime | None, now: datetime) -> bool:
    """Due once the first scheduled moment after the last success has passed.

    A run missed while the machine was off is therefore picked up at the next check.
    """
    return last_success is None or now >= schedule.next_after(last_success)


# -- auth ---------------------------------------------------------------------------------------


class NoAuth(_Frozen):
    kind: Literal["none"] = "none"


class ApiKeyAuth(_Frozen):
    """API key read from an environment variable, sent as a header or a query parameter."""

    kind: Literal["api_key"] = "api_key"
    env_var: str
    header: str | None = None
    query_param: str | None = None

    @model_validator(mode="after")
    def _exactly_one_location(self) -> "ApiKeyAuth":
        if (self.header is None) == (self.query_param is None):
            raise ValueError("api_key auth needs exactly one of `header` or `query_param`")
        return self


class BearerAuth(_Frozen):
    kind: Literal["bearer"] = "bearer"
    env_var: str


Auth = Annotated[NoAuth | ApiKeyAuth | BearerAuth, Field(discriminator="kind")]


# -- http policy --------------------------------------------------------------------------------


class HttpPolicy(_Frozen):
    requests_per_second: float = Field(default=1.0, gt=0)
    burst: int = Field(default=1, ge=1)
    max_retries: int = Field(default=5, ge=0)
    backoff_base: Duration = timedelta(seconds=1)
    backoff_max: Duration = timedelta(seconds=60)
    timeout: Duration = timedelta(seconds=60)
    retry_statuses: frozenset[int] = frozenset({429, 500, 502, 503, 504})


# -- streams and sources ------------------------------------------------------------------------


class DeepRefetch(_Frozen):
    """Periodically look further back than the normal revision window."""

    every: Duration
    window: Duration


class StreamSpec(_Frozen):
    name: str
    schedule: Schedule
    watermark: WatermarkKind
    natural_key: tuple[str, ...]
    revision_window: Duration | None = None
    deep_refetch: DeepRefetch | None = None
    # Earliest data the source can serve by query. None: no queryable history, so backfill is
    # refused (live-only sources, or files that always contain the full history anyway).
    backfill_start: datetime | None = None

    @model_validator(mode="after")
    def _consistent(self) -> "StreamSpec":
        if not self.natural_key:
            raise ValueError(f"stream {self.name!r}: natural_key must not be empty")
        if self.backfill_start is not None and self.backfill_start.tzinfo is None:
            raise ValueError(f"stream {self.name!r}: backfill_start must be timezone-aware")
        if self.watermark is WatermarkKind.NONE and (
            self.revision_window is not None or self.deep_refetch is not None
        ):
            raise ValueError(
                f"stream {self.name!r}: revision windows need a watermark to be relative to"
            )
        if self.deep_refetch is not None and self.revision_window is None:
            raise ValueError(f"stream {self.name!r}: deep_refetch requires a revision_window")
        return self


class SourceSpec(_Frozen):
    name: str
    auth: Auth = NoAuth()
    http: HttpPolicy = HttpPolicy()
    streams: tuple[StreamSpec, ...]

    @model_validator(mode="after")
    def _unique_streams(self) -> "SourceSpec":
        names = [s.name for s in self.streams]
        if len(names) != len(set(names)):
            raise ValueError(f"source {self.name!r}: duplicate stream names")
        return self

    def stream(self, name: str) -> StreamSpec:
        for s in self.streams:
            if s.name == name:
                return s
        raise KeyError(f"source {self.name!r} has no stream {name!r}")

    def with_overrides(self, overrides: dict[str, Any]) -> "SourceSpec":
        """Return a copy with `[<source>]` overrides from sources.toml applied and re-validated."""
        data = self.model_dump(mode="python")
        stream_overrides: dict[str, Any] = overrides.get("streams", {})
        unknown = set(stream_overrides) - {s["name"] for s in data["streams"]}
        if unknown:
            raise ValueError(f"sources.toml: source {self.name!r} has no streams {sorted(unknown)}")
        for key, value in overrides.items():
            if key == "streams":
                continue
            data[key] = _merge(data.get(key), value)
        data["streams"] = [_merge(s, stream_overrides.get(s["name"], {})) for s in data["streams"]]
        return SourceSpec.model_validate(data)


def _merge(base: Any, override: Any) -> Any:
    if isinstance(base, dict) and isinstance(override, dict):
        # Switching a tagged union (e.g. schedule daily -> interval) replaces it wholesale.
        if override.get("kind", base.get("kind")) != base.get("kind"):
            return override
        return {**base, **{k: _merge(base.get(k), v) for k, v in override.items()}}
    return override


def load_overrides(path: Path) -> dict[str, dict[str, Any]]:
    """Read sources.toml; a missing file means no overrides."""
    if not path.exists():
        return {}
    with path.open("rb") as f:
        return tomllib.load(f)
