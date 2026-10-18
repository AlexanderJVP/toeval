import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from toeval.lock import FileLock, LockHeldError, pid_alive


def test_second_holder_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "ingest.lock"
    with FileLock(path), pytest.raises(LockHeldError, match="another run"):
        FileLock(path).acquire()
    assert not path.exists()


def test_stale_lock_is_taken_over(tmp_path: Path) -> None:
    path = tmp_path / "ingest.lock"
    path.write_text("{}", encoding="utf-8")
    old = time.time() - 7 * 3600
    os.utime(path, (old, old))

    with FileLock(path, stale_after=6 * 3600):
        assert path.exists()
    assert not path.exists()


def test_release_leaves_a_lock_that_was_taken_over(tmp_path: Path) -> None:
    path = tmp_path / "ingest.lock"
    lock = FileLock(path)
    lock.acquire()
    path.write_text('{"token": "someone-else"}', encoding="utf-8")  # simulated takeover

    lock.release()

    assert path.exists()


def test_lock_of_a_dead_process_is_taken_over_immediately(tmp_path: Path) -> None:
    dead = subprocess.Popen([sys.executable, "-c", "pass"])
    dead.wait()
    path = tmp_path / "ingest.lock"
    path.write_text(json.dumps({"pid": dead.pid, "since": time.time(), "token": "x"}), "utf-8")

    with FileLock(path):  # fresh lock, but its holder is gone
        assert json.loads(path.read_text("utf-8"))["pid"] == os.getpid()


def test_lock_of_a_live_process_is_respected(tmp_path: Path) -> None:
    path = tmp_path / "ingest.lock"
    path.write_text(json.dumps({"pid": os.getpid(), "since": time.time(), "token": "x"}), "utf-8")

    with pytest.raises(LockHeldError):
        FileLock(path).acquire()


def test_pid_alive() -> None:
    assert pid_alive(os.getpid())
    assert not pid_alive(0)
