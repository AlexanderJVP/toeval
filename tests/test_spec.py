from datetime import UTC, datetime, time, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from fakes import FakeLiveConnector, FakeSeriesConnector
from toeval.connectors.spec import (
    ApiKeyAuth,
    DailySchedule,
    DeepRefetch,
    IntervalSchedule,
    MonthlySchedule,
    StreamSpec,
    is_due,
    load_overrides,
    parse_duration,
)
from toeval.state import WatermarkKind


@pytest.mark.parametrize(
    ("text", "expected"),
    [("500ms", timedelta(milliseconds=500)), ("5m", timedelta(minutes=5)), ("14d", timedelta(14))],
)
def test_parse_duration(text: str, expected: timedelta) -> None:
    assert parse_duration(text) == expected


def test_daily_schedule_uses_brussels_wall_clock_across_dst() -> None:
    daily = DailySchedule(at=time(6, 30))
    winter = datetime(2026, 1, 15, 12, tzinfo=UTC)
    summer = datetime(2026, 7, 15, 12, tzinfo=UTC)

    assert daily.next_after(winter) == datetime(2026, 1, 16, 5, 30, tzinfo=UTC)  # CET = UTC+1
    assert daily.next_after(summer) == datetime(2026, 7, 16, 4, 30, tzinfo=UTC)  # CEST = UTC+2


def test_daily_schedule_on_the_dst_change_day() -> None:
    # 2026-03-29: clocks jump from 02:00 to 03:00 in Brussels.
    daily = DailySchedule(at=time(6, 30))
    before = datetime(2026, 3, 28, 12, tzinfo=UTC)

    assert daily.next_after(before) == datetime(2026, 3, 29, 4, 30, tzinfo=UTC)


def test_monthly_schedule_rolls_over_the_year() -> None:
    monthly = MonthlySchedule(day=5, at=time(9, 0))

    assert monthly.next_after(datetime(2026, 12, 6, tzinfo=UTC)) == datetime(
        2027, 1, 5, 8, tzinfo=UTC
    )


def test_is_due_catches_up_after_missed_slots() -> None:
    daily = DailySchedule(at=time(6, 30))
    last = datetime(2026, 10, 1, 5, 0, tzinfo=UTC)

    assert is_due(daily, None, last)
    assert not is_due(daily, last, datetime(2026, 10, 1, 20, tzinfo=UTC))
    assert is_due(daily, last, datetime(2026, 10, 18, 20, tzinfo=UTC))  # laptop was off for weeks


def test_interval_schedule_for_polling() -> None:
    every5 = IntervalSchedule(every=timedelta(minutes=5))
    last = datetime(2026, 10, 18, 8, 0, tzinfo=UTC)

    assert not is_due(every5, last, last + timedelta(minutes=4))
    assert is_due(every5, last, last + timedelta(minutes=5))


def test_live_streams_cannot_have_revision_windows() -> None:
    with pytest.raises(ValidationError, match="need a watermark"):
        StreamSpec(
            name="x",
            schedule=IntervalSchedule(every="5m"),
            watermark=WatermarkKind.NONE,
            natural_key=("id",),
            revision_window="1d",
        )


def test_deep_refetch_requires_a_revision_window() -> None:
    with pytest.raises(ValidationError, match="requires a revision_window"):
        StreamSpec(
            name="x",
            schedule=IntervalSchedule(every="1d"),
            watermark=WatermarkKind.TIMESTAMP,
            natural_key=("t",),
            deep_refetch=DeepRefetch(every="30d", window="90d"),
        )


def test_api_key_needs_exactly_one_location() -> None:
    with pytest.raises(ValidationError, match="exactly one"):
        ApiKeyAuth(env_var="K")
    with pytest.raises(ValidationError, match="exactly one"):
        ApiKeyAuth(env_var="K", header="X", query_param="k")


def test_overrides_from_toml_change_operational_knobs(tmp_path: Path) -> None:
    toml = tmp_path / "sources.toml"
    toml.write_text(
        """
[series.http]
requests_per_second = 0.5

[series.streams.hourly]
revision_window = "21d"
schedule = { kind = "interval", every = "12h" }
""",
        encoding="utf-8",
    )

    spec = FakeSeriesConnector.spec.with_overrides(load_overrides(toml)["series"])

    stream = spec.stream("hourly")
    assert spec.http.requests_per_second == 0.5
    assert spec.http.max_retries == FakeSeriesConnector.spec.http.max_retries  # untouched
    assert stream.revision_window == timedelta(days=21)
    assert stream.schedule == IntervalSchedule(every=timedelta(hours=12))
    assert stream.natural_key == ("datetime",)


def test_overrides_are_validated(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="no streams"):
        FakeSeriesConnector.spec.with_overrides({"streams": {"typo": {}}})
    with pytest.raises(ValidationError, match="need a watermark"):
        FakeLiveConnector.spec.with_overrides(
            {"streams": {"departures": {"revision_window": "1d"}}}
        )


def test_missing_overrides_file_means_no_overrides(tmp_path: Path) -> None:
    assert load_overrides(tmp_path / "absent.toml") == {}
