from pathlib import Path

import pytest

from toeval.config import REPO_ROOT, Settings


def test_relative_data_dir_resolves_from_repo_root(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TOEVAL_DATA_DIR", "somewhere/data")

    settings = Settings(_env_file=None)

    assert settings.data_path == REPO_ROOT / "somewhere" / "data"
    assert settings.state_db_path == REPO_ROOT / "somewhere" / "data" / "state" / "state.duckdb"


def test_absolute_data_dir_is_kept(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TOEVAL_DATA_DIR", str(tmp_path))

    assert Settings(_env_file=None).raw_path == tmp_path / "raw"
