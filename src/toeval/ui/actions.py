"""Actions the console can start (ADR 0007): only the normal CLI commands, as separate processes.

A started command takes the ingest lock and logs like any other run, so the pipeline stays the
only writer. The console keeps each job's output in `data/logs/console/` and reports its exit
code (0 ok, 1 failed, 2 usage error, 3 another run held the lock). Jobs outlive the console: they
run in their own process group, so Ctrl+C on `toeval ui` doesn't stop a fetch halfway.
"""

import os
import subprocess
import sys
import threading
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from toeval.config import Settings

KEEP_LOGS = 50
TAIL_LINES = 12
OUTCOMES = {0: "ok", 1: "failed", 2: "usage error", 3: "another run held the lock"}


class BusyError(RuntimeError):
    """A run is in progress; starting another would only wait for (or lose) the lock."""


def _utcnow() -> datetime:
    return datetime.now(UTC)


@dataclass
class Job:
    id: int
    label: str
    args: tuple[str, ...]
    started_at: datetime
    log_path: Path
    process: Any  # subprocess.Popen
    finished_at: datetime | None = None
    exit_code: int | None = None

    def refresh(self, now: datetime) -> None:
        if self.exit_code is None and (code := self.process.poll()) is not None:
            self.exit_code = code
            self.finished_at = now

    def as_json(self) -> dict[str, Any]:
        running = self.exit_code is None
        return {
            "id": self.id,
            "label": self.label,
            "command": "toeval " + " ".join(self.args),
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "status": "running" if running else ("ok" if self.exit_code == 0 else "failed"),
            "exit_code": self.exit_code,
            "outcome": None if running else OUTCOMES.get(self.exit_code or 0, "failed"),
            "output": _tail(self.log_path),
        }


@dataclass
class JobRunner:
    settings: Settings
    # The command a job runs; tests replace it with a harmless one.
    command: Sequence[str] = (sys.executable, "-m", "toeval")
    clock: Callable[[], datetime] = _utcnow
    jobs: list[Job] = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def start(self, label: str, args: Sequence[str]) -> Job:
        with self._lock:
            now = self.clock()
            for job in self.jobs:
                job.refresh(now)
                if job.exit_code is None:
                    raise BusyError(f"'{job.label}' is still running")
            log_dir = self.settings.log_path / "console"
            log_dir.mkdir(parents=True, exist_ok=True)
            _prune(log_dir)
            log_path = log_dir / f"{now:%Y%m%d-%H%M%S}-{'-'.join(args)}.log"
            env = os.environ | {
                "TOEVAL_DATA_DIR": str(self.settings.data_path),
                "TOEVAL_SOURCES_FILE": str(self.settings.sources_path),
                "PYTHONIOENCODING": "utf-8",
            }
            flags = 0
            if sys.platform == "win32":
                flags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW
            with log_path.open("wb") as out:
                process = subprocess.Popen(
                    [*self.command, *args],
                    stdin=subprocess.DEVNULL,
                    stdout=out,
                    stderr=subprocess.STDOUT,
                    env=env,
                    creationflags=flags,
                )
            job = Job(len(self.jobs) + 1, label, tuple(args), now, log_path, process)
            self.jobs.append(job)
            return job

    def running(self) -> Job | None:
        now = self.clock()
        for job in self.jobs:
            job.refresh(now)
            if job.exit_code is None:
                return job
        return None

    def recent(self, limit: int = 8) -> list[dict[str, Any]]:
        now = self.clock()
        for job in self.jobs:
            job.refresh(now)
        return [job.as_json() for job in reversed(self.jobs[-limit:])]


def _tail(path: Path) -> list[str]:
    try:
        data = path.read_bytes()[-8000:]
    except OSError:
        return []
    return data.decode("utf-8", errors="replace").splitlines()[-TAIL_LINES:]


def _prune(log_dir: Path) -> None:
    logs = sorted(log_dir.glob("*.log"))
    for old in logs[: max(0, len(logs) - KEEP_LOGS + 1)]:
        old.unlink(missing_ok=True)


__all__ = ["BusyError", "Job", "JobRunner"]
