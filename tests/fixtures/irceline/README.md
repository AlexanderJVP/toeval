# IRCEL-CELINE fixtures

Live responses of the IRCEL-CELINE timeseries API (`https://geo.irceline.be/sos/api/v1`), CC BY 4.0, recorded 2026-10-19 23:21 UTC:

- `timeseries.json`: `GET /timeseries?phenomenon=<id>&expanded=true` for the four pollutants (keys are the phenomenon ids 8, 5, 6001, 7), **cut down to the 15 curated stations** in `connectors/irceline.py`: 54 series.
- `getData-2026-10-17_2026-10-20.json`: `GET /timeseries/<id>/getData?timespan=2026-10-17T00:00:00Z/2026-10-20T00:00:00Z` for each of those series, keyed by series id.

Facts the tests rely on:

- Both ends of a timespan are included: 73 hourly timestamps per series, from 10-17 00:00 to 10-20 00:00 UTC.
- 217 values are `null`. Every series ends in null hours that aren't published yet; the latest value is at **2026-10-19 22:00 UTC**, about 1.5 hours before recording.
- 3,725 values are non-null before 10-20 00:00.
- Series 10761 (42R804 Antwerpen Ring, NO2) has its first value in 2017.

To re-record, request the same timespan for the curated stations and update the counts in `tests/test_irceline.py`.
