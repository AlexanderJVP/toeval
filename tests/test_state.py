from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest

from toeval.state import (
    RawFileEntry,
    RunStats,
    StateStore,
    StreamState,
    Watermark,
    WatermarkKind,
)

T0 = datetime(2026, 10, 18, 6, 30, tzinfo=UTC)


@pytest.fixture
def store(tmp_path: Path) -> StateStore:
    with StateStore(tmp_path / "state.duckdb") as s:
        yield s


def test_unknown_stream_has_empty_state(store: StateStore) -> None:
    state = store.get_stream_state("elia", "ods001")

    assert state.watermark is None
    assert state.last_success_at is None
    assert state.http_validators == {}


@pytest.mark.parametrize(
    "watermark",
    [Watermark.timestamp(datetime(2026, 10, 17, 21, 45, tzinfo=UTC)), Watermark.period("2026-09")],
)
def test_successful_run_persists_watermark_and_run(store: StateStore, watermark: Watermark) -> None:
    run_id = store.start_run("src", "stream", "incremental", T0)
    state = StreamState(
        "src",
        "stream",
        watermark=watermark,
        last_success_at=T0 + timedelta(seconds=5),
        http_validators={"k": {"etag": '"abc"'}},
    )

    store.finish_run_success(run_id, state, RunStats(requests=2, rows_fetched=10), T0, None)

    assert store.get_stream_state("src", "stream") == state
    run = store.get_run(run_id)
    assert run["status"] == "success"
    assert run["rows_fetched"] == 10
    assert run["watermark_after"] == watermark.serialize()


def test_failed_run_leaves_stream_state_untouched(store: StateStore) -> None:
    ok = store.start_run("src", "stream", "incremental", T0)
    good = StreamState("src", "stream", watermark=Watermark.timestamp(T0), last_success_at=T0)
    store.finish_run_success(ok, good, RunStats(), T0, None)

    failed = store.start_run("src", "stream", "incremental", T0 + timedelta(days=1))
    store.finish_run_failed(failed, RunStats(requests=1), T0, good.watermark, "boom")

    assert store.get_stream_state("src", "stream") == good
    assert store.get_run(failed)["error"] == "boom"


def test_timestamps_round_trip_as_utc_regardless_of_input_offset(store: StateStore) -> None:
    brussels_summer = timezone(timedelta(hours=2))
    local = datetime(2026, 7, 1, 8, 0, tzinfo=brussels_summer)
    run_id = store.start_run("src", "s", "incremental", local)
    state = StreamState("src", "s", watermark=Watermark.timestamp(local), last_success_at=local)
    store.finish_run_success(run_id, state, RunStats(), local, None)

    loaded = store.get_stream_state("src", "s")

    assert loaded.last_success_at == datetime(2026, 7, 1, 6, 0, tzinfo=UTC)
    assert loaded.watermark == Watermark.timestamp(datetime(2026, 7, 1, 6, 0, tzinfo=UTC))


def test_naive_timestamps_are_rejected(store: StateStore) -> None:
    with pytest.raises(ValueError, match="naive"):
        store.start_run("src", "s", "incremental", datetime(2026, 1, 1))  # noqa: DTZ001


def test_watermark_validates_kind_and_value() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        Watermark(WatermarkKind.TIMESTAMP, "2026-01-01")
    with pytest.raises(ValueError, match="timezone-aware"):
        Watermark(WatermarkKind.TIMESTAMP, datetime(2026, 1, 1))  # noqa: DTZ001
    with pytest.raises(ValueError, match="no watermark"):
        Watermark(WatermarkKind.NONE, "x")


def test_manifest_tracks_content_hashes(store: StateStore) -> None:
    entry = RawFileEntry("raw/a.parquet", "src", "s", "run1", "req", T0, "hash1", 3)
    store.add_raw_file(entry)

    assert store.has_content_hash("src", "s", "hash1")
    assert not store.has_content_hash("src", "s", "hash2")
    assert not store.has_content_hash("src", "other", "hash1")
    assert store.raw_files("src", "s") == [entry]
