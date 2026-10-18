from datetime import UTC, datetime, timedelta
from itertools import pairwise

from fakes import ORIGIN, FakeSeriesConnector
from toeval.connectors.base import RunContext, RunMode
from toeval.connectors.planning import TimeWindow, time_window_plan
from toeval.state import StreamState, Watermark

STREAM = FakeSeriesConnector.spec.stream("hourly")  # revision window 2d, deep 10d, origin Jan 1
WEEK = timedelta(days=7)


def plan(state: StreamState, ctx: RunContext) -> list[TimeWindow]:
    return time_window_plan(STREAM, state, ctx, chunk=WEEK)


def state_with(watermark: datetime | None) -> StreamState:
    return StreamState("series", "hourly", Watermark.timestamp(watermark) if watermark else None)


def test_windows_are_contiguous_and_on_the_grid() -> None:
    windows = plan(
        state_with(None), RunContext(datetime(2026, 1, 20, tzinfo=UTC), RunMode.INCREMENTAL)
    )

    assert windows[0].start == ORIGIN
    assert all(a.end == b.start for a, b in pairwise(windows))
    assert [w.end for w in windows] == [ORIGIN + WEEK, ORIGIN + 2 * WEEK, ORIGIN + 3 * WEEK]


def test_rerun_produces_identical_request_keys() -> None:
    state = state_with(datetime(2026, 1, 20, tzinfo=UTC))
    morning = plan(state, RunContext(datetime(2026, 1, 20, 6, tzinfo=UTC), RunMode.INCREMENTAL))
    evening = plan(state, RunContext(datetime(2026, 1, 20, 18, tzinfo=UTC), RunMode.INCREMENTAL))

    assert [w.key for w in morning] == [w.key for w in evening]


def test_start_never_precedes_backfill_start() -> None:
    state = state_with(ORIGIN + timedelta(hours=5))

    windows = plan(state, RunContext(ORIGIN + timedelta(days=1), RunMode.INCREMENTAL))

    assert windows[0].start == ORIGIN


def test_backfill_uses_requested_range() -> None:
    ctx = RunContext(
        datetime(2026, 6, 1, tzinfo=UTC),
        RunMode.BACKFILL,
        backfill_from=datetime(2026, 2, 1, tzinfo=UTC),
        backfill_to=datetime(2026, 2, 10, tzinfo=UTC),
    )

    windows = plan(state_with(datetime(2026, 5, 31, tzinfo=UTC)), ctx)

    assert windows[0].start == datetime(2026, 2, 1, tzinfo=UTC)
    assert windows[-1].end == datetime(2026, 2, 10, tzinfo=UTC)


def test_deep_refetch_widens_the_window() -> None:
    wm = datetime(2026, 3, 1, tzinfo=UTC)
    normal = plan(state_with(wm), RunContext(wm, RunMode.INCREMENTAL))
    deep = plan(state_with(wm), RunContext(wm, RunMode.INCREMENTAL, deep_refetch=True))

    assert normal[0].start == wm - timedelta(days=2)
    assert deep[0].start == wm - timedelta(days=10)
