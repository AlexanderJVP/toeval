"""The Windows scheduled task that runs `toeval run-due` (scripts/scheduled-task.ps1)."""

import json
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

TASK_NAME = "toeval run-due"

# Task Scheduler result codes that aren't failures (winerror.h SCHED_S_*).
RESULTS = {
    0: ("ok", "last run succeeded"),
    0x41301: ("ok", "running now"),
    0x41303: ("ok", "has not run yet"),
}

_QUERY = """
$ErrorActionPreference = 'Stop'
try { $t = Get-ScheduledTask -TaskName '{name}' } catch { '{"installed": false}'; exit 0 }
$i = $t | Get-ScheduledTaskInfo
$fmt = 'yyyy-MM-ddTHH:mm:sszzz'
$last = $i.LastRunTime
$next = $i.NextRunTime
[pscustomobject]@{
    installed = $true
    state = [string]$t.State
    last_run = if ($last -and $last.Year -gt 2000) { $last.ToString($fmt) } else { $null }
    last_result = $i.LastTaskResult
    next_run = if ($next) { $next.ToString($fmt) } else { $null }
} | ConvertTo-Json -Compress
"""


@dataclass(frozen=True)
class TaskInfo:
    installed: bool
    state: str | None = None  # Ready | Running | Disabled | ...
    last_run_at: datetime | None = None
    last_result: int | None = None
    next_run_at: datetime | None = None

    def as_json(self) -> dict[str, Any]:
        level, text = describe_result(self.last_result)
        return {
            "installed": self.installed,
            "state": self.state,
            "last_run_at": self.last_run_at.isoformat() if self.last_run_at else None,
            "next_run_at": self.next_run_at.isoformat() if self.next_run_at else None,
            "result": text,
            "result_level": level,
        }


def describe_result(code: int | None) -> tuple[str, str]:
    if code is None:
        return "ok", "unknown"
    if code in RESULTS:
        return RESULTS[code]
    if code == 1:
        return "error", "a stream or the transform failed (exit 1; see the log)"
    return "error", f"failed with code {code:#x} ({code})"


Runner = Callable[..., subprocess.CompletedProcess[str]]


def query_task(name: str = TASK_NAME, runner: Runner = subprocess.run) -> TaskInfo | None:
    """None when the state can't be queried (not Windows, PowerShell unavailable)."""
    if sys.platform != "win32":
        return None
    try:
        result = runner(
            ["powershell", "-NoProfile", "-NonInteractive", "-Command",
             _QUERY.replace("{name}", name.replace("'", "''"))],
            capture_output=True, text=True, timeout=15,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )  # fmt: skip
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    return parse_task(result.stdout)


def parse_task(output: str) -> TaskInfo | None:
    try:
        data = json.loads(output.strip().splitlines()[-1])
    except (ValueError, IndexError):
        return None
    if not data.get("installed"):
        return TaskInfo(installed=False)
    return TaskInfo(
        installed=True,
        state=data.get("state"),
        last_run_at=_parse(data.get("last_run")),
        last_result=data.get("last_result"),
        next_run_at=_parse(data.get("next_run")),
    )


def _parse(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None
