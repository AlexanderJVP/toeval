"""Single-writer lock so overlapping scheduled runs never write the warehouse concurrently."""

import json
import logging
import os
import time
from pathlib import Path
from types import TracebackType
from typing import Self

log = logging.getLogger(__name__)


class LockHeldError(RuntimeError):
    pass


class FileLock:
    """Exclusive lock file. A lock older than `stale_after` seconds (crashed run) is taken over."""

    def __init__(self, path: Path, stale_after: float = 6 * 3600) -> None:
        self._path = path
        self._stale_after = stale_after
        self._held = False

    def acquire(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        for _ in range(2):
            try:
                fd = os.open(self._path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError:
                age = time.time() - self._path.stat().st_mtime
                if age < self._stale_after:
                    raise LockHeldError(
                        f"another run holds {self._path} ({self._read()}); age {age:.0f}s"
                    ) from None
                log.warning(
                    "taking over stale lock %s (%s, age %.0fs)", self._path, self._read(), age
                )
                self._path.unlink(missing_ok=True)
                continue
            with os.fdopen(fd, "w") as f:
                json.dump({"pid": os.getpid(), "since": time.time()}, f)
            self._held = True
            return
        raise LockHeldError(f"could not acquire {self._path}")

    def release(self) -> None:
        if self._held:
            self._path.unlink(missing_ok=True)
            self._held = False

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
