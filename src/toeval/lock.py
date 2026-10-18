"""Single-writer lock so overlapping scheduled runs never write the warehouse concurrently."""

import json
import logging
import os
import time
import uuid
from pathlib import Path
from types import TracebackType
from typing import Self

log = logging.getLogger(__name__)


class LockHeldError(RuntimeError):
    pass


class FileLock:
    """Exclusive lock file. A lock older than `stale_after` seconds (crashed run) is taken over.

    Takeover is best effort: there is one scheduler entry, so contention is rare. Stale detection
    re-reads the file before deleting it, and release never deletes someone else's lock.
    """

    def __init__(self, path: Path, stale_after: float = 6 * 3600) -> None:
        self._path = path
        self._stale_after = stale_after
        self._token: str | None = None

    def acquire(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        for _ in range(3):
            try:
                fd = os.open(self._path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError:
                try:
                    age = time.time() - self._path.stat().st_mtime
                except FileNotFoundError:  # holder released between open() and stat()
                    continue
                seen = self._read()
                if age < self._stale_after:
                    raise LockHeldError(
                        f"another run holds {self._path} ({seen}); age {age:.0f}s"
                    ) from None
                # Only remove the lock we judged stale; if someone replaced it meanwhile, retry.
                if self._read() == seen:
                    log.warning("taking over stale lock %s (%s, age %.0fs)", self._path, seen, age)
                    self._path.unlink(missing_ok=True)
                continue
            token = uuid.uuid4().hex
            with os.fdopen(fd, "w") as f:
                json.dump({"pid": os.getpid(), "since": time.time(), "token": token}, f)
            self._token = token
            return
        raise LockHeldError(f"could not acquire {self._path}")

    def release(self) -> None:
        """Remove the lock file, but only if it is still ours (it may have been taken over)."""
        if self._token is None:
            return
        try:
            owner = json.loads(self._path.read_text(encoding="utf-8")).get("token")
        except (OSError, ValueError):
            owner = None
        if owner == self._token:
            self._path.unlink(missing_ok=True)
        else:
            log.warning("lock %s was taken over by another run; leaving it in place", self._path)
        self._token = None

    def _read(self) -> str:
        try:
            return self._path.read_text(encoding="utf-8")
        except OSError:
            return "unreadable"

    def __enter__(self) -> Self:
        self.acquire()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.release()
