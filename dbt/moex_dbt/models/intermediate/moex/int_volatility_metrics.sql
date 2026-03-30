{{ config(
    materialized='table',
    order_by=['trading_dt__start', 'secid']
) }}


SELECT
    assumeNotNull(market_data.secid) AS secid
    , assumeNotNull(market_data.trading_dt__start) AS trading_dt__start

    -- 30D Volatility (annualized)
    , stddevSamp(yield) OVER (
        PARTITION BY secid
        ORDER BY market_data.trading_dt__start
        ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
    ) * sqrt(252) AS vol_30d

    -- 90D Volatility (annualized)
    , stddevSamp(yield) OVER (
        PARTITION BY secid
        ORDER BY market_data.trading_dt__start
        ROWS BETWEEN 90 PRECEDING AND CURRENT ROW
    ) * sqrt(252) AS vol_90d

    -- Downside Volatility (30D)
    , stddevSamp(
        if(yield < 0, yield, NULL)
    ) OVER (
        PARTITION BY secid
        ORDER BY market_data.trading_dt__start
        ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
    ) * sqrt(252) AS downside_vol_30d

    -- Downside Volatility (90D)
    , stddevSamp(
        if(yield < 0, yield, NULL)
    ) OVER (
        PARTITION BY secid
        ORDER BY market_data.trading_dt__start
        ROWS BETWEEN 90 PRECEDING AND CURRENT ROW
    ) * sqrt(252) AS downside_vol_90d

    -- Rolling Mean Return (for Sharpe)
    , avg(adjusted_yield) OVER (
        PARTITION BY secid
        ORDER BY market_data.trading_dt__start
        ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
    ) * 252 AS mean_return_30d

    -- Sharpe Ratio (252D, annualized)
    , (
        avg(adjusted_yield) OVER (
            PARTITION BY secid
            ORDER BY market_data.trading_dt__start
            ROWS BETWEEN 252 PRECEDING AND CURRENT ROW
        )
        /
        stddevSamp(adjusted_yield) OVER (
            PARTITION BY secid
            ORDER BY market_data.trading_dt__start
            ROWS BETWEEN 252 PRECEDING AND CURRENT ROW
        )
    ) * sqrt(252) AS sharpe_30d

    -- Sortino Ratio (30D)
    , (
        avg(adjusted_yield) OVER (
            PARTITION BY secid
            ORDER BY market_data.trading_dt__start
            ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
        )
        /
        stddevSamp(
            if(adjusted_yield < 0, adjusted_yield, NULL)
        ) OVER (
            PARTITION BY secid
            ORDER BY market_data.trading_dt__start
            ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
        )
    ) * sqrt(252) AS sortino_30d
    

FROM
    {{ ref('int_market_data') }} AS market_data