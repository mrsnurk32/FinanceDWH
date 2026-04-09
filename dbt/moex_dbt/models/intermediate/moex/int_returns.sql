{{ config(
    materialized='table',
    order_by=['trading_dt__start', 'secid']
) }}

SELECT
    assumeNotNull(adjusted_close.secid) AS secid
    , assumeNotNull(adjusted_close.trading_dt__start) AS trading_dt__start
    , adjusted_close.adjusted_close AS adjusted_close
    , adjusted_close.close AS close

    , any(adjusted_close.close) OVER (
		PARTITION BY adjusted_close.secid 
		ORDER BY adjusted_close.trading_dt__start ASC 
		ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
	) AS prev_close

    , any(adjusted_close.adjusted_close) OVER (
		PARTITION BY adjusted_close.secid 
		ORDER BY adjusted_close.trading_dt__start ASC 
		ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
	) AS prev_adjusted_close

    , CASE WHEN prev_close > 0 
		THEN ( adjusted_close.close / prev_close ) - 1 
		ELSE null END
	AS yield

    , CASE WHEN prev_adjusted_close > 0 
		THEN ( adjusted_close.adjusted_close / prev_adjusted_close ) - 1 
		ELSE null END
	AS adjusted_yield
    , adjusted_close.annualized_dividends__cumm_sum AS annualized_dividends__cumm_sum
    , (key_rates.obs_val / 100) AS annual_rate
    , POWER(1 + annual_rate, 1.0 / 252) - 1 AS daily_rate
    , adjusted_yield - daily_rate AS risk_free_adjusted_yield

FROM
    {{ ref('int_adjusted_close') }} AS adjusted_close

LEFT JOIN
    {{ ref('stg_central_bank__key_rates') }} AS key_rates
    ON key_rates.period = date_trunc('month', adjusted_close.trading_dt__start)