"""Command-line entry point: `uv run toeval ...` (or `python -m toeval ...`).

Exit codes: 0 ok, 1 at least one stream run failed, 2 usage error, 3 another run holds the lock.
"""

import logging
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Annotated

import duckdb
import typer

from toeval import __version__
from toeval.config import Settings, get_settings
from toeval.connectors.base import RunMode, StreamRunResult, UnsupportedModeError
from toeval.connectors.spec import BRUSSELS
from toeval.lock import FileLock, LockHeldError
from toeval.log import setup_logging
from toeval.pipeline import Pipeline
from toeval.state import SCHEMA_VERSION, StateStore

log = logging.getLogger(__name__)

app = typer.Typer(help="Toeval data platform.", no_args_is_help=True)

EXIT_FAILED = 1
EXIT_USAGE = 2
EXIT_LOCKED = 3


@app.callback()
def main() -> None:
    """Toeval data platform."""


@app.command()
def version() -> None:
    """Print the installed version."""
    typer.echo(__version__)


@app.command()
def ingest(
    source: str,
    streams: Annotated[list[str] | None, typer.Argument(help="Default: all streams.")] = None,
) -> None:
    """Run incremental ingestion now, whether or not the streams are due; then transform."""
    with _pipeline() as pipeline:
        _ingest_then_transform(pipeline, lambda: pipeline.run(source, streams))


@app.command()
def backfill(
    source: str,
    stream: str,
    from_: Annotated[
        str | None,
        typer.Option("--from", help="ISO date/time; without offset it is Brussels time."),
    ] = None,
    to: Annotated[str | None, typer.Option(help="Exclusive end, same format.")] = None,
) -> None:
    """Re-fetch a historical range (default: the stream's whole history); then transform."""
    start, end = _parse_time(from_), _parse_time(to)
    with _pipeline() as pipeline:
        _ingest_then_transform(
            pipeline,
            lambda: pipeline.run(
                source, [stream], RunMode.BACKFILL, backfill_from=start, backfill_to=end
            ),
        )


@app.command("run-due")
def run_due() -> None:
    """Run every stream that is due (the scheduled task calls this every 5 minutes)."""
    # A held lock means the previous scheduled run is still busy: skip quietly, exit 0.
    with _pipeline(busy_ok=True) as pipeline:
        _ingest_then_transform(pipeline, pipeline.run_due)


@app.command()
def transform() -> None:
    """Rebuild staging and marts from raw, then run the quality checks."""
    with _pipeline() as pipeline:
        _finish([], _transform(pipeline))


@app.command()
def status() -> None:
    """Show each stream's watermark, last success, failures and next due time."""
    settings = _setup()
    if not settings.state_db_path.exists():
        typer.echo("No state yet: nothing has been ingested.")
        return
    try:
        store = StateStore(settings.state_db_path, read_only=True)
    except duckdb.IOException:
        typer.echo("An ingest run is in progress (state DB is locked); try again later.")
        raise typer.Exit(EXIT_LOCKED) from None
    with store:
        if (version := store.schema_version()) < SCHEMA_VERSION:
            typer.echo(
                f"The state DB has schema version {version}; this code expects {SCHEMA_VERSION}. "
                "The next run (scheduled, or `toeval run-due`) upgrades it."
            )
            return
        for s in Pipeline(settings, store).status():
            wm = s.state.watermark.serialize() if s.state.watermark else "-"
            failures = (
                f", {s.state.consecutive_failures} failed run(s)"
                if s.state.consecutive_failures
                else ""
            )
            typer.echo(
                f"{s.source}/{s.stream}: watermark {wm}, last success "
                f"{_fmt(s.state.last_success_at)}{failures}, next due {_fmt(s.next_due) or 'now'}"
            )
        t = store.transform_status()
        if t.last_attempt_at is not None:
            line = f"transform: last success {_fmt(t.last_success_started_at) or 'never'}"
            if t.consecutive_failures:
                line += (
                    f"; {t.consecutive_failures} failed attempt(s) since, last at "
                    f"{_fmt(t.last_failure_at)}: {t.last_error}"
                )
            typer.echo(line)
        quality = store.latest_quality_results()
        if quality:
            warnings = [q for q in quality if not q.ok]
            typer.echo(
                f"quality at {_fmt(quality[0].checked_at)}: "
                f"{len(quality) - len(warnings)} ok, {len(warnings)} warnings"
            )
            for q in warnings:
                typer.echo(f"  {q.check} {q.subject}: {q.summary}")


# -- helpers -------------------------------------------------------------------------------------


def _setup() -> Settings:
    settings = get_settings()
    setup_logging(settings.log_level, settings.log_path)
    return settings


@contextmanager
def _pipeline(*, busy_ok: bool = False) -> Iterator[Pipeline]:
    """Settings, logging, the single-writer lock and the state store, in that order."""
    settings = _setup()
    lock = FileLock(settings.lock_path)
    try:
        lock.acquire()
    except LockHeldError as exc:
        if busy_ok:
            log.info("skipping: %s", exc)
            raise typer.Exit(0) from None
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(EXIT_LOCKED) from None
    try:
        with StateStore(settings.state_db_path) as store:
            # Holding the lock means nothing else runs: anything still 'running' was killed.
            if aborted := store.abort_orphaned(datetime.now(UTC)):
                log.warning("marked %d run(s)/transform(s) of a killed process as failed", aborted)
            try:
                yield Pipeline(settings, store)
            except (KeyError, ValueError) as exc:  # unknown source/stream, bad sources.toml
                typer.echo(f"error: {exc}", err=True)
                raise typer.Exit(EXIT_USAGE) from None
    finally:
        lock.release()


def _run(action: Callable[[], Sequence[StreamRunResult]]) -> Sequence[StreamRunResult]:
    try:
        return action()
    except UnsupportedModeError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(EXIT_USAGE) from None


def _ingest_then_transform(
    pipeline: Pipeline, action: Callable[[], Sequence[StreamRunResult]]
) -> None:
    results = _run(action)
    transformed = _transform(pipeline) if pipeline.needs_transform(results) else True
    _finish(results, transformed)


def _transform(pipeline: Pipeline) -> bool:
    """Run the transform; False if it failed (logged, and reported through the exit code)."""
    try:
        result = pipeline.transform()
    except Exception:
        log.exception("transform failed")
        typer.echo("transform: FAILED (see data/logs/toeval.log)", err=True)
        return False
    typer.echo(f"transform: built {', '.join(result.built) or 'nothing'}")
    if result.skipped:
        typer.echo(f"transform: skipped {', '.join(result.skipped)} (inputs not ingested yet)")
    ok = len(result.quality) - len(result.warnings)
    typer.echo(f"quality: {ok} ok, {len(result.warnings)} warnings")
    for q in result.warnings:
        typer.echo(f"  {q.check} {q.subject}: {q.summary}")
    return True


def _finish(results: Sequence[StreamRunResult], transformed: bool = True) -> None:
    for r in results:
        if r.succeeded:
            wm = r.watermark_after.serialize() if r.watermark_after else "-"
            typer.echo(
                f"{r.source}/{r.stream}: ok, {r.stats.requests} requests, "
                f"{r.stats.rows_fetched} rows, {r.stats.files_written} files, watermark {wm}"
            )
        else:
            typer.echo(f"{r.source}/{r.stream}: FAILED: {r.error}", err=True)
    if any(not r.succeeded for r in results) or not transformed:
        raise typer.Exit(EXIT_FAILED)


def _parse_time(value: str | None) -> datetime | None:
    if value is None:
        return None
    try:
        t = datetime.fromisoformat(value)
    except ValueError:
        typer.echo(f"error: not an ISO date/time: {value!r}", err=True)
        raise typer.Exit(EXIT_USAGE) from None
    return t if t.tzinfo else t.replace(tzinfo=BRUSSELS)


def _fmt(t: datetime | None) -> str:
    return t.astimezone(BRUSSELS).strftime("%Y-%m-%d %H:%M %Z") if t else ""
