{{ config(
    materialized='table',
    order_by=['trading_dt__start', 'secid']
) }}


SELECT
    assumeNotNull(market_data.secid) AS secid
    , assumeNotNull(market_data.trading_dt__start) AS trading_dt__start
        -- 1M Momentum (21 days)
    , market_data.adjusted_close / any(market_data.adjusted_close) OVER (
        PARTITION BY secid 
        ORDER BY market_data.trading_dt__start 
        ROWS BETWEEN 21 PRECEDING AND 21 PRECEDING
    ) - 1 AS mom_1m

    -- 3M Momentum (63 days)
    , market_data.adjusted_close / any(market_data.adjusted_close) OVER (
        PARTITION BY secid 
        ORDER BY market_data.trading_dt__start 
        ROWS BETWEEN 63 PRECEDING AND 63 PRECEDING
    ) - 1 AS mom_3m

    -- 6M Momentum (126 days)
    , market_data.adjusted_close / any(market_data.adjusted_close) OVER (
        PARTITION BY secid 
        ORDER BY market_data.trading_dt__start 
        ROWS BETWEEN 126 PRECEDING AND 126 PRECEDING
    ) - 1 AS mom_6m

    -- 12M Momentum (252 days)
    , market_data.adjusted_close / any(market_data.adjusted_close) OVER (
        PARTITION BY secid 
        ORDER BY market_data.trading_dt__start 
        ROWS BETWEEN 252 PRECEDING AND 252 PRECEDING
    ) - 1 AS mom_12m

    -- 12M Momentum excluding last month
    , (
        any(market_data.adjusted_close) OVER (
            PARTITION BY secid 
            ORDER BY market_data.trading_dt__start 
            ROWS BETWEEN 21 PRECEDING AND 21 PRECEDING
        )
        /
        any(market_data.adjusted_close) OVER (
            PARTITION BY secid 
            ORDER BY market_data.trading_dt__start 
            ROWS BETWEEN 252 PRECEDING AND 252 PRECEDING
        )
    ) - 1 AS mom_12m_ex_1m

FROM
    {{ ref('int_returns') }} AS market_data