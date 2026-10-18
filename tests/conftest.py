import logging
from collections.abc import Iterator
from pathlib import Path

import pytest

from toeval.config import Settings
from toeval.state import StateStore


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    # sources_file explicitly in tmp: never pick up the repo's own sources.toml.
    return Settings(
        _env_file=None, data_dir=tmp_path / "data", sources_file=tmp_path / "sources.toml"
    )


@pytest.fixture
def store(settings: Settings) -> Iterator[StateStore]:
    with StateStore(settings.state_db_path) as s:
        yield s


@pytest.fixture
def restore_logging() -> Iterator[None]:
    """Undo `setup_logging` (it replaces the root logger's handlers)."""
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    yield
    for h in list(root.handlers):
        if h not in handlers:
            root.removeHandler(h)
            h.close()
    for h in handlers:
        if h not in root.handlers:
            root.addHandler(h)
    root.setLevel(level)
