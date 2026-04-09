{{ config(
    materialized='table',
    order_by=['trading_dt__start', 'secid']
) }}


SELECT
    assumeNotNull(market_data.secid) AS secid
    , assumeNotNull(market_data.trading_dt__start) AS trading_dt__start
    
    -- ######################################
    -- Market data
    -- Open, High, Low, Close + Volume
    -- ######################################
    , market_data.open AS open
    , market_data.close AS close
    , market_data.high AS high
    , market_data.low AS low
    , market_data.value AS value
    , market_data.volume AS volume
    
    , returns.yield AS yield
    , returns.adjusted_close AS adjusted_close

    -- ######################################
    -- Price momentum
    -- ######################################
    , price_momentum.mom_1m AS mom_1m
    , price_momentum.mom_3m AS mom_3m
    , price_momentum.mom_6m AS mom_6m
    , price_momentum.mom_12m AS mom_12m
    , price_momentum.mom_12m_ex_1m AS mom_12m_ex_1m

    -- ######################################
    -- Volatility
    -- ######################################
    , volatility_metrics.vol_30d AS vol_30d
    , volatility_metrics.vol_90d AS vol_90d
    , volatility_metrics.downside_vol_30d AS downside_vol_30d
    , volatility_metrics.downside_vol_90d AS downside_vol_90d
    , volatility_metrics.mean_return_30d AS mean_return_30d
    , volatility_metrics.sharpe_30d AS sharpe_30d
    , volatility_metrics.sortino_30d AS sortino_30d
    , volatility_metrics.fair_value_estimate AS fair_value_estimate
    , volatility_metrics.annual_rate AS annual_rate
    , volatility_metrics.annualized_dividends__cumm_sum AS annualized_dividends__cumm_sum


    -- ######################################
    -- Measures to impement:
    -- - Z-score for losing money (expected return < 0)
    -- - Alpha (excess return over risk-free rate)
    -- - Fair Value Estimate (close price adjusted by alpha)
    -- - Beta (correlation with market)
    -- - Max Drawdown (over 1Y)
    -- ######################################

FROM
    {{ ref('int_market_data') }} AS market_data

LEFT JOIN 
    {{ ref('int_price_momentum') }} AS price_momentum
    ON price_momentum.secid = market_data.secid
    AND price_momentum.trading_dt__start = market_data.trading_dt__start

LEFT JOIN
    {{ ref('int_volatility_metrics') }} AS volatility_metrics
    ON volatility_metrics.secid = market_data.secid
    AND volatility_metrics.trading_dt__start = market_data.trading_dt__start

LEFT JOIN
    {{ ref('int_returns') }} AS returns
    ON returns.secid = market_data.secid
    AND returns.trading_dt__start = market_data.trading_dt__start