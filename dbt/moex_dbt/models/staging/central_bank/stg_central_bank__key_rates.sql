{{ config(
    materialized = 'table',
    schema = 'staging'
) }}


SELECT
    {{ adapter.quote('date') }} AS period,
    {{ adapter.quote('obs_val') }},

FROM 
    {{ source('moex', 'centralbank_rates_ru') }} FINAL