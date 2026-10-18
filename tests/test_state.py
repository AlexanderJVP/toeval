from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pytest

from toeval.state import (
    MIGRATIONS,
    SCHEMA_VERSION,
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


def test_failed_run_only_counts_the_failure(store: StateStore) -> None:
    ok = store.start_run("src", "stream", "incremental", T0)
    good = StreamState("src", "stream", watermark=Watermark.timestamp(T0), last_success_at=T0)
    store.finish_run_success(ok, good, RunStats(), T0, None)

    for day in (1, 2):
        failed = store.start_run("src", "stream", "incremental", T0 + timedelta(days=day))
        store.finish_run_failed(
            failed, RunStats(requests=1), T0 + timedelta(days=day), good.watermark, "boom"
        )

    assert store.get_stream_state("src", "stream") == replace(
        good, consecutive_failures=2, last_failure_at=T0 + timedelta(days=2)
    )
    assert store.get_run(failed)["error"] == "boom"


def test_failed_backfill_does_not_count_towards_the_retry_backoff(store: StateStore) -> None:
    failed = store.start_run("src", "stream", "backfill", T0)
    store.finish_run_failed(failed, RunStats(), T0, None, "boom")

    assert store.get_stream_state("src", "stream") == StreamState("src", "stream")
    assert store.get_run(failed)["status"] == "failed"


def test_first_ever_run_failing_creates_the_stream_state(store: StateStore) -> None:
    failed = store.start_run("src", "stream", "incremental", T0)
    store.finish_run_failed(failed, RunStats(), T0, None, "boom")

    state = store.get_stream_state("src", "stream")
    assert (state.watermark, state.consecutive_failures, state.last_failure_at) == (None, 1, T0)


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


def _file(name: str, run: str, key: str, at: datetime, digest: str) -> RawFileEntry:
    return RawFileEntry(f"raw/{name}.parquet", "src", "s", run, key, at, digest, 1)


def test_unchanged_batch_for_same_key_is_recognised(store: StateStore) -> None:
    entry = _file("a", "run1", "K1", T0, "A")
    store.add_raw_file(entry)

    assert store.is_unchanged("src", "s", "K1", "A", current_run="run2")
    assert not store.is_unchanged("src", "s", "K1", "B", current_run="run2")
    assert not store.is_unchanged("src", "s", "K2", "A", current_run="run2")
    assert store.raw_files("src", "s") == [entry]


def test_a_b_a_is_not_deduplicated(store: StateStore) -> None:
    # K1 stores A, an overlapping window K2 later stores B for the same rows, then K1 sees A again.
    # Skipping that last A would leave staging on the stale B.
    store.add_raw_file(_file("a", "run1", "K1", T0, "A"))
    store.add_raw_file(_file("b", "run2", "K2", T0 + timedelta(hours=1), "B"))

    assert not store.is_unchanged("src", "s", "K1", "A", current_run="run3")


def test_files_from_the_current_run_do_not_block_dedup(store: StateStore) -> None:
    store.add_raw_file(_file("a", "run1", "K1", T0, "A"))
    store.add_raw_file(_file("b", "run2", "K2", T0 + timedelta(hours=1), "B"))

    assert store.is_unchanged("src", "s", "K1", "A", current_run="run2")


# -- migrations and orphans ------------------------------------------------------------------


def test_database_from_before_versioning_is_migrated(tmp_path: Path) -> None:
    path = tmp_path / "state.duckdb"
    con = duckdb.connect(str(path))
    con.execute(MIGRATIONS[0][2])  # the b3350b1 schema: no failure columns, no version table
    con.execute(
        "INSERT INTO stream_state (source, stream, watermark_kind, watermark_value)"
        " VALUES ('elia', 'load', 'timestamp', '2026-10-18T08:00:00+00:00')"
    )
    con.close()

    with StateStore(path) as store:
        state = store.get_stream_state("elia", "load")
        assert store.schema_version() == SCHEMA_VERSION
        assert state.watermark == Watermark.timestamp(datetime(2026, 10, 18, 8, tzinfo=UTC))
        assert (state.consecutive_failures, state.last_failure_at) == (0, None)
        assert store.transform_status().last_attempt_at is None

    with StateStore(path) as store:  # reopening is a no-op
        assert store.schema_version() == SCHEMA_VERSION


def test_database_newer_than_the_code_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "state.duckdb"
    with StateStore(path):
        pass
    con = duckdb.connect(str(path))
    con.execute("UPDATE schema_version SET version = version + 1")
    con.close()

    with pytest.raises(RuntimeError, match="newer than this code"):
        StateStore(path)


def test_orphaned_runs_and_transforms_are_marked_failed(store: StateStore) -> None:
    incremental = store.start_run("src", "stream", "incremental", T0)
    backfill = store.start_run("src", "other", "backfill", T0)
    transform = store.start_transform(T0)
    later = T0 + timedelta(hours=3)

    assert store.abort_orphaned(later) == 3

    for run_id in (incremental, backfill):
        run = store.get_run(run_id)
        assert run["status"] == "failed"
        assert "killed" in str(run["error"])
    assert store.transform_status().last_status == "failed"
    assert store.get_stream_state("src", "stream").consecutive_failures == 1  # counts
    assert store.get_stream_state("src", "other").consecutive_failures == 0  # backfill doesn't
    assert store.abort_orphaned(later) == 0
    assert transform
