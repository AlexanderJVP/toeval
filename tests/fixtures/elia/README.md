# Elia fixtures

Recorded `/exports/parquet` responses from Elia Open Data (https://opendata.elia.be), Elia Open
Data Licence. Downloaded 2026-10-18 and trimmed to a few days so the tests stay small:

| File | Dataset | Range (UTC) | Rows | Notes |
|---|---|---|---|---|
| `ods001.parquet` | load | 2026-10-17 22:00 – 2026-10-20 22:00 | 288 | measured up to 2026-10-18 08:00, then forecast rows (`totalload` null) |
| `ods031.parquet` | wind | 2026-10-16 22:00 – 2026-10-17 22:00 | 480 | 5 parts × 96 quarter-hours |
| `ods032.parquet` | solar | 2026-10-16 22:00 – 2026-10-17 22:00 | 1344 | Belgium + 3 regions + 10 provinces × 96 |

To re-record, download with
`GET https://opendata.elia.be/api/explore/v2.1/catalog/datasets/<id>/exports/parquet?where=datetime >= '<start>' and datetime < '<end>'`
and keep the same ranges (tests assert on them).
