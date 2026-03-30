SELECT 
    dividends.secid AS secid
    , dividends.registryclosedate AS registryclosedate
    , toTimeZone(dividends.registryclosedate, 'Europe/Moscow') AS registryclosedate__tz_mos
    , CASE
        WHEN toDayOfWeek(registryclosedate__tz_mos) = 6 THEN dividends.registryclosedate + INTERVAL 2 DAY
        WHEN toDayOfWeek(registryclosedate__tz_mos) = 7 THEN dividends.registryclosedate + INTERVAL 1 DAY
        ELSE dividends.registryclosedate
    END AS adjusted_date
    , dividends.value AS value

FROM
    {{ ref('stg_moex__dividends') }} AS dividends