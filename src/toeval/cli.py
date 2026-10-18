"""Command-line entry point: `uv run toeval ...` (or `python -m toeval ...`).

Exit codes: 0 ok, 1 at least one stream run failed, 2 usage error, 3 another run holds the lock.
"""

import logging
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from datetime import datetime
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
from toeval.state import StateStore

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
    """Run incremental ingestion now, whether or not the streams are due."""
    with _pipeline() as pipeline:
        _finish(_run(lambda: pipeline.run(source, streams)))


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
    """Re-fetch a historical range (default: the stream's whole history)."""
    start, end = _parse_time(from_), _parse_time(to)
    with _pipeline() as pipeline:
        _finish(
            _run(
                lambda: pipeline.run(
                    source, [stream], RunMode.BACKFILL, backfill_from=start, backfill_to=end
                )
            )
        )


@app.command("run-due")
def run_due() -> None:
    """Run every stream that is due (the scheduled task calls this every 5 minutes)."""
    # A held lock means the previous scheduled run is still busy: skip quietly, exit 0.
    with _pipeline(busy_ok=True) as pipeline:
        _finish(_run(pipeline.run_due))


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


def _finish(results: Sequence[StreamRunResult]) -> None:
    for r in results:
        if r.succeeded:
            wm = r.watermark_after.serialize() if r.watermark_after else "-"
            typer.echo(
                f"{r.source}/{r.stream}: ok, {r.stats.requests} requests, "
                f"{r.stats.rows_fetched} rows, {r.stats.files_written} files, watermark {wm}"
            )
        else:
            typer.echo(f"{r.source}/{r.stream}: FAILED: {r.error}", err=True)
    if any(not r.succeeded for r in results):
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
