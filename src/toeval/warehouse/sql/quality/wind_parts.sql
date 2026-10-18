-- Quarter-hours where wind has some but not all 5 parts measured (wind_mw is then NULL), or
-- more than 5 (Elia added a part; docs/decisions/0003). value = number of such quarter-hours.
SELECT
    'elia/wind' AS subject,
    count(*)::DOUBLE AS value,
    format('parts present: {}', string_agg(DISTINCT wind_parts_present::VARCHAR, ', ')) AS details
FROM mart_energy_15min
WHERE wind_parts_present NOT IN (0, 5)
