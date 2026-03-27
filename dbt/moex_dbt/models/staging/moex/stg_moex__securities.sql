{{ config(
    materialized = 'table',
    schema = 'staging'
) }}

SELECT
    {{ adapter.quote('secid') }},
    {{ adapter.quote('boardid') }},
    {{ adapter.quote('shortname') }},
    {{ adapter.quote('prevprice') }},
    {{ adapter.quote('lotsize') }},
    {{ adapter.quote('facevalue') }},
    {{ adapter.quote('status') }},
    {{ adapter.quote('boardname') }},
    {{ adapter.quote('decimals') }},
    {{ adapter.quote('secname') }},
    {{ adapter.quote('remarks') }},
    {{ adapter.quote('marketcode') }},
    {{ adapter.quote('instrid') }},
    {{ adapter.quote('sectorid') }},
    {{ adapter.quote('minstep') }},
    {{ adapter.quote('prevwaprice') }},
    {{ adapter.quote('faceunit') }},
    {{ adapter.quote('prevdate') }},
    {{ adapter.quote('issuesize') }},
    {{ adapter.quote('isin') }},
    {{ adapter.quote('latname') }},
    {{ adapter.quote('regnumber') }},
    {{ adapter.quote('prevlegalcloseprice') }},
    {{ adapter.quote('currencyid') }},
    {{ adapter.quote('sectype') }},
    {{ adapter.quote('listlevel') }},
    {{ adapter.quote('settledate') }},
    {{ adapter.quote('version') }}

FROM 
    {{ source('moex', 'securities') }} FINAL