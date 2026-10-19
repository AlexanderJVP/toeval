"""Progress events: what a run is doing right now, for the admin console (ADR 0007).

The state DB is locked for the whole duration of a run, so the console can't read progress from
it. Instead the process holding the ingest lock appends one JSON object per line to
`data/state/events.jsonl`; the console tails that file. Events are informational only: the state
DB stays the source of truth, and a failure to write an event never fails a run.

Each file starts with a `log_started` line carrying a random `epoch`. A cursor is
`<epoch>:<byte offset>`, so a reader notices when the file was rotated (renamed to
`events.1.jsonl`) and finishes reading the old file before continuing in the new one.
"""

import json
import logging
import os
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

log = logging.getLogger(__name__)

MAX_BYTES = 1_000_000  # rotate at the start of a process once the file is this big
TAIL = 100


class EventSink(Protocol):
    def emit(self, type: str, **fields: Any) -> None: ...


class NullSink:
    def emit(self, type: str, **fields: Any) -> None:
        pass


NULL_SINK = NullSink()


def _utcnow() -> datetime:
    return datetime.now(UTC)


class EventLog:
    """Appends events to a JSON-lines file. Only the process holding the ingest lock writes."""

    def __init__(
        self,
        path: Path,
        *,
        clock: Callable[[], datetime] = _utcnow,
        max_bytes: int = MAX_BYTES,
    ) -> None:
        self.path = path
        self._clock = clock
        self.emitted = 0
        self._broken = False
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            if path.exists() and path.stat().st_size >= max_bytes:
                try:
                    os.replace(path, rotated(path))
                except OSError:  # the console has it open right now (Windows); next time
                    log.debug("could not rotate %s", path)
            if not path.exists():
                self._append({"type": "log_started", "epoch": uuid.uuid4().hex})
        except OSError:
            self._fail()

    def emit(self, type: str, **fields: Any) -> None:
        if self._broken:
            return
        try:
            self._append({"at": self._clock().isoformat(), "type": type, **fields})
        except (OSError, TypeError, ValueError):
            self._fail()
            return
        self.emitted += 1

    def _append(self, event: dict[str, Any]) -> None:
        line = json.dumps(event, ensure_ascii=False, default=str) + "\n"
        with self.path.open("a", encoding="utf-8", newline="\n") as f:
            f.write(line)

    def _fail(self) -> None:
        log.warning("cannot write progress events to %s; continuing without", self.path)
        self._broken = True


def rotated(path: Path) -> Path:
    return path.with_name(path.stem + ".1" + path.suffix)


def read_events(path: Path, cursor: str | None, *, tail: int = TAIL) -> tuple[list[dict], str]:
    """Events after `cursor`, and the cursor to continue from.

    Without a cursor, returns the last `tail` events (history for a page that just opened).
    Only complete lines are returned: the writer may be halfway through one.
    """
    epoch, start = _header(path)
    if epoch is None:
        return [], ""
    if cursor is None:
        events, end = _read(path, start)
        return events[-tail:], f"{epoch}:{end}"

    seen_epoch, _, offset_text = cursor.partition(":")
    offset = int(offset_text) if offset_text.isdigit() else start
    events: list[dict] = []
    if seen_epoch != epoch:
        # Rotated since the last read: finish the old file if it is the one we were reading.
        old = rotated(path)
        if _header(old)[0] == seen_epoch:
            events, _ = _read(old, offset)
        offset = start
    new, end = _read(path, max(offset, start))
    return events + new, f"{epoch}:{end}"


def _header(path: Path) -> tuple[str | None, int]:
    try:
        with path.open("rb") as f:
            first = f.readline()
    except OSError:
        return None, 0
    try:
        header = json.loads(first)
    except ValueError:
        return None, 0
    if not first.endswith(b"\n") or header.get("type") != "log_started":
        return None, 0
    return str(header["epoch"]), len(first)


def _read(path: Path, offset: int) -> tuple[list[dict], int]:
    try:
        with path.open("rb") as f:
            f.seek(offset)
            data = f.read()
    except OSError:
        return [], offset
    complete = data[: data.rfind(b"\n") + 1]
    events = []
    for line in complete.splitlines():
        try:
            events.append(json.loads(line))
        except ValueError:
            log.debug("skipping a malformed event line in %s", path)
    return events, offset + len(complete)


__all__ = ["NULL_SINK", "EventLog", "EventSink", "NullSink", "read_events"]
