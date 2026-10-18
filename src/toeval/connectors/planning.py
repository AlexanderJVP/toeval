"""Planning helpers for time-series streams; connectors without time windows ignore this."""

from dataclasses import dataclass
from datetime import datetime, timedelta

from toeval.connectors.base import RunContext, RunMode
from toeval.connectors.spec import StreamSpec
from toeval.state import StreamState, WatermarkKind


@dataclass(frozen=True)
class TimeWindow:
    start: datetime  # inclusive
    end: datetime  # exclusive

    @property
    def key(self) -> str:
        return f"{self.start.isoformat()}/{self.end.isoformat()}"


def time_window_plan(
    stream: StreamSpec,
    state: StreamState,
    ctx: RunContext,
    *,
    chunk: timedelta,
    horizon: timedelta = timedelta(0),
) -> list[TimeWindow]:
    """Windows to fetch for a timestamp-watermarked stream (docs/decisions/0002, 0004).

    - backfill: the requested range (default: whole history up to now + horizon)
    - first incremental run: from `backfill_start`
    - later incremental runs: from `watermark - revision_window` (or the deep window), so missed
      runs are caught up and recent revisions re-fetched
    Window boundaries (including the end of an incremental run, rounded up) sit on a fixed grid
    anchored at `backfill_start`. Re-runs therefore produce the same request keys, so unchanged
    content is recognised and not written again.
    """
    if stream.watermark is not WatermarkKind.TIMESTAMP:
        raise ValueError(f"stream {stream.name!r} is not timestamp-watermarked")
    if stream.backfill_start is None:
        raise ValueError(f"stream {stream.name!r} needs backfill_start for time-window planning")
    if chunk <= timedelta(0):
        raise ValueError("chunk must be positive")

    origin = stream.backfill_start
    end = _ceil_to_grid(ctx.now + horizon, origin, chunk)

    if ctx.mode is RunMode.BACKFILL:
        start = ctx.backfill_from or origin
        end = ctx.backfill_to or end
    elif state.watermark is None:
        start = origin
    else:
        assert isinstance(state.watermark.value, datetime)
        window = stream.revision_window or timedelta(0)
        if ctx.deep_refetch and stream.deep_refetch is not None:
            window = max(window, stream.deep_refetch.window)
        start = state.watermark.value - window

    start = max(start, origin)
    if start >= end:
        return []

    windows = []
    cursor = start
    while cursor < end:
        boundary = origin + ((cursor - origin) // chunk + 1) * chunk
        nxt = min(boundary, end)
        windows.append(TimeWindow(cursor, nxt))
        cursor = nxt
    return windows


def _ceil_to_grid(t: datetime, origin: datetime, chunk: timedelta) -> datetime:
    steps, rest = divmod(t - origin, chunk)
    return origin + (steps + (1 if rest else 0)) * chunk
