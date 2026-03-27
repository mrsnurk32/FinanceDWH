{{ config(
    materialized = 'table',
    schema = 'staging'
) }}

-- #################################################
-- Dividends payouts
-- registryclosedate - date when payout happened
-- value - payout amount
-- #################################################

SELECT
    {{ adapter.quote('secid') }},
    {{ adapter.quote('isin') }},
    {{ adapter.quote('registryclosedate') }},
    {{ adapter.quote('value') }},
    {{ adapter.quote('currencyid') }},
    {{ adapter.quote('version') }}
FROM 
    {{ source('moex', 'dividends') }} FINAL