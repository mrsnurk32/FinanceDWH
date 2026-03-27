{{ config(
    materialized='table',
    schema='staging'
) }}


SELECT
    {{ adapter.quote("secid") }} AS secid
    , {{ adapter.quote("open") }} AS open
    , {{ adapter.quote("close") }} AS close
    , {{ adapter.quote("high") }} AS high
    , {{ adapter.quote("low") }} AS low
    , {{ adapter.quote("value") }} AS value
    , {{ adapter.quote("volume") }} AS volume
    , {{ adapter.quote("begin") }} AS trading_dt__start
    , {{ adapter.quote("end") }} AS trading_dt__end
    
FROM
    {{ source('moex', 'market_data') }}
