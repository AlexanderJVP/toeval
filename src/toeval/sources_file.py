"""Programmatic edits of sources.toml (`toeval pause/resume`, the admin console's pause and
schedule editor).

Edits keep the owner's comments and formatting (tomlkit), are validated exactly like a run
validates the file (`pipeline.resolve_specs`) before anything is written, and replace the file
atomically, so a scheduled run never reads a half-written or invalid file.
"""

import os
import time
import tomllib
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import tomlkit
from tomlkit.items import Table

from toeval.connectors import registry
from toeval.connectors.base import Connector
from toeval.pipeline import resolve_specs


def set_stream_option(
    path: Path,
    source: str,
    stream: str,
    key: str,
    value: Any,
    *,
    connectors: Mapping[str, type[Connector]] | None = None,
) -> None:
    """Set `[<source>.streams.<stream>] key = value`; `None` removes the key (back to the
    connector's default) and drops tables left empty."""
    set_stream_options(path, source, stream, {key: value}, connectors=connectors)


def set_stream_options(
    path: Path,
    source: str,
    stream: str,
    values: Mapping[str, Any],
    *,
    connectors: Mapping[str, type[Connector]] | None = None,
) -> None:
    """Several `set_stream_option`s in one validated write. A dict value (e.g. a schedule) is
    written as an inline table and replaces the old value wholesale."""
    connectors = registry.CONNECTORS if connectors is None else connectors
    if source not in connectors:
        raise KeyError(f"unknown source {source!r}; known: {sorted(connectors)}")
    connectors[source].spec.stream(stream)  # KeyError for an unknown stream

    doc = tomlkit.parse(path.read_text(encoding="utf-8")) if path.exists() else tomlkit.document()
    src = _table(doc, source)
    streams = _table(src, "streams")
    options = _table(streams, stream, super_table=False)
    for key, value in values.items():
        if value is None:
            options.pop(key, None)
        elif isinstance(value, Mapping):
            table = tomlkit.inline_table()
            table.update(value)
            options[key] = table
        else:
            options[key] = value
    for parent, name, table in ((streams, stream, options), (src, "streams", streams)):
        if not table:
            del parent[name]
    if not src:
        del doc[source]

    text = tomlkit.dumps(doc)
    resolve_specs(tomllib.loads(text), connectors, path)  # ValueError: nothing is written
    _write_atomic(path, text)


def _table(parent: Any, name: str, *, super_table: bool = True) -> Table:
    existing = parent.get(name)
    if existing is None:
        existing = tomlkit.table(is_super_table=super_table)
        parent[name] = existing
    if not isinstance(existing, Mapping):
        raise ValueError(f"sources.toml: {name!r} must be a table")
    return existing  # type: ignore[return-value]


def _write_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8", newline="\n")
    try:
        for attempt in range(10):
            try:
                os.replace(tmp, path)
                return
            except PermissionError:  # Windows: a run is reading it this very moment
                if attempt == 9:
                    raise
                time.sleep(0.05)
    finally:
        tmp.unlink(missing_ok=True)


__all__ = ["set_stream_option", "set_stream_options"]
