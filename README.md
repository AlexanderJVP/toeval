# toeval

*Toeval* is Dutch for "coincidence".

A personal data platform that continuously ingests Belgian and Antwerp open data into a local DuckDB + Parquet warehouse. Planned on top of it: a tracker that hunts for absurd correlations and watches them fall apart, and a self-supervised JEPA model for unlabeled time series.

Work in progress (Phase 1: foundation + Elia grid data). The full README follows once Phase 1 works end to end.

## Development

```powershell
uv sync
uv run pre-commit install
uv run pytest
uv run ruff check .
```
