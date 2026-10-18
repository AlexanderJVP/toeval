import logging
from collections.abc import Iterator
from pathlib import Path

import pytest

from toeval.log import setup_logging


@pytest.fixture(autouse=True)
def restore_root_logger() -> Iterator[None]:
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    yield
    for h in list(root.handlers):
        root.removeHandler(h)
        h.close()
    for h in handlers:
        root.addHandler(h)
    root.setLevel(level)


def test_writes_to_rotating_file_and_is_idempotent(tmp_path: Path) -> None:
    setup_logging("INFO", tmp_path)
    setup_logging("INFO", tmp_path)  # second call must not duplicate handlers

    logging.getLogger("toeval.test").info("hello")
    logging.getLogger("toeval.test").debug("hidden")
    for h in logging.getLogger().handlers:
        h.flush()

    text = (tmp_path / "toeval.log").read_text(encoding="utf-8")
    assert text.count("hello") == 1
    assert "hidden" not in text
    assert len(logging.getLogger().handlers) == 2
