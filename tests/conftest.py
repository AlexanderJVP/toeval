from collections.abc import Iterator
from pathlib import Path

import pytest

from toeval.config import Settings
from toeval.state import StateStore


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(_env_file=None, data_dir=tmp_path / "data")


@pytest.fixture
def store(settings: Settings) -> Iterator[StateStore]:
    with StateStore(settings.state_db_path) as s:
        yield s
