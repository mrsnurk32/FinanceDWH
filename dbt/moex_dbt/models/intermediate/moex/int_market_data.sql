{{ config(
    materialized='table',
    order_by=['trading_dt__start', 'secid']
) }}

SELECT 
    assumeNotNull(market_data.secid) AS secid
    , assumeNotNull(market_data.trading_dt__start) AS trading_dt__start
    , market_data.open AS open
    , market_data.close AS close
    , market_data.high AS high
    , market_data.low AS low
    , market_data.value AS value
    , market_data.volume AS volume

FROM 
	{{ ref('stg_moex__market_data') }} AS market_data

WHERE True
    AND toDayOfWeek(toTimeZone(market_data.trading_dt__start, 'Europe/Moscow')) not in (6,7)