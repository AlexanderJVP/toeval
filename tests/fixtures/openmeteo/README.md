# Open-Meteo fixture

`archive-2026-10-12_2026-10-19.json`: a live response of the historical weather API, Open-Meteo / Copernicus ERA5, CC-BY 4.0. Recorded 2026-10-19:

`GET https://archive-api.open-meteo.com/v1/archive` with the connector's `query("2026-10-12", "2026-10-19")` (7 locations, 9 variables, `models=era5`, `wind_speed_unit=ms`, `timezone=GMT`).

- A list of 7 objects in request order (antwerp … offshore), 192 hourly rows each.
- ERA5 values run up to **2026-10-13 23:00**. The rows after that are nulls: ERA5's ~5-day delay. The tests rely on this.
- Grid cells as returned: offshore is `51.5, 3.0` at elevation 0 (sea).

To re-record, request the same dates and keep the file name; the tests assert on these facts.
