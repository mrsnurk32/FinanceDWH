{{ config(
    materialized='table',
    order_by=['trading_dt__start', 'secid']
) }}

SELECT
    assumeNotNull(market_data.secid) AS secid
    , assumeNotNull(market_data.trading_dt__start) AS trading_dt__start
    , dividends.value AS dividend_value
    , SUM(dividends.value) OVER (
        PARTITION BY market_data.secid 
        ORDER BY market_data.trading_dt__start 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS dividends__cumm_sum
    , SUM(dividends.value) OVER (
        PARTITION BY market_data.secid 
        ORDER BY market_data.trading_dt__start 
        ROWS BETWEEN 252 PRECEDING AND CURRENT ROW
    ) AS annualized_dividends__cumm_sum
    , market_data.close + dividends__cumm_sum AS adjusted_close
    , market_data.close AS close

FROM
    {{ ref('int_market_data') }} AS market_data

LEFT JOIN
    {{ ref('int_dividends') }} AS dividends
    ON dividends.secid = market_data.secid
    AND date_trunc('day', dividends.adjusted_date) = market_data.trading_dt__start