import os
import time
from pathlib import Path

import pytest

from toeval.lock import FileLock, LockHeldError


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
