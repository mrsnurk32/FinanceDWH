{{ config(
    materialized='table',
    order_by=['trading_dt__start', 'secid']
) }}


SELECT
    assumeNotNull(market_data.secid) AS secid
    , assumeNotNull(market_data.trading_dt__start) AS trading_dt__start
    , market_data.adjusted_yield AS adjusted_yield
    , market_data.annual_rate AS annual_rate
    , market_data.annualized_dividends__cumm_sum AS annualized_dividends__cumm_sum

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
    , avg(market_data.adjusted_yield) OVER (
        PARTITION BY secid
        ORDER BY market_data.trading_dt__start
        ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
    ) * 252 AS mean_return_30d

    , avg(market_data.adjusted_yield) OVER (
        PARTITION BY secid
        ORDER BY market_data.trading_dt__start
        ROWS BETWEEN 252 PRECEDING AND CURRENT ROW
    ) * 252 AS expeted_annual_yeild

    , stddevSamp(market_data.adjusted_yield) OVER (
            PARTITION BY secid
            ORDER BY market_data.trading_dt__start
            ROWS BETWEEN 252 PRECEDING AND CURRENT ROW
        ) AS std_annualized

    , CASE WHEN market_data.annual_rate > 0
        THEN 
            market_data.annualized_dividends__cumm_sum / 
                ( market_data.annual_rate - std_annualized ) 
        ELSE NULL
    END AS fair_value_estimate
    -- , market_data.close * (1 + alpha) AS fair_value_estimate
    -- Sharpe Ratio (252D, annualized)
    , (
        avg(risk_free_adjusted_yield) OVER (
            PARTITION BY secid
            ORDER BY market_data.trading_dt__start
            ROWS BETWEEN 252 PRECEDING AND CURRENT ROW
        )
        /
        stddevSamp(risk_free_adjusted_yield) OVER (
            PARTITION BY secid
            ORDER BY market_data.trading_dt__start
            ROWS BETWEEN 252 PRECEDING AND CURRENT ROW
        )
    ) * sqrt(252) AS sharpe_30d

    -- Sortino Ratio (30D)
    , (
        avg(market_data.adjusted_yield) OVER (
            PARTITION BY secid
            ORDER BY market_data.trading_dt__start
            ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
        )
        /
        stddevSamp(
            if(market_data.adjusted_yield < 0, market_data.adjusted_yield, NULL)
        ) OVER (
            PARTITION BY secid
            ORDER BY market_data.trading_dt__start
            ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
        )
    ) * sqrt(252) AS sortino_30d
    

FROM
    {{ ref('int_returns') }} AS market_data