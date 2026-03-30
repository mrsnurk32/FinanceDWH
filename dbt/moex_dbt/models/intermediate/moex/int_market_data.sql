{{ config(
    materialized='table',
    order_by=['trading_dt__start', 'secid']
) }}

SELECT 
    assumeNotNull(market_data.secid) AS secid
    , market_data.open AS open
    , market_data.close AS close
    , market_data.high AS high
    , market_data.low AS low
    , market_data.value AS value
    , market_data.volume AS volume
    , assumeNotNull(market_data.trading_dt__start) AS trading_dt__start
    , market_data.trading_dt__end AS trading_dt__end
    
    , any(market_data.close) OVER (
		PARTITION BY market_data.secid 
		ORDER BY market_data.trading_dt__start ASC 
		ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
	) AS prev_close
    
    , CASE WHEN prev_close > 0 
		THEN ( market_data.close / prev_close ) - 1 
		ELSE null END
	AS yield
    , adjusted_close.adjusted_close AS adjusted_close
    , adjusted_close.dividends__cumm_sum AS dividends__cumm_sum
    , any(adjusted_close.adjusted_close) OVER (
		PARTITION BY market_data.secid 
		ORDER BY market_data.trading_dt__start ASC 
		ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
	) AS prev_adjusted_close

    , CASE WHEN prev_adjusted_close > 0 
		THEN ( adjusted_close.adjusted_close / prev_adjusted_close ) - 1 
		ELSE null END
	AS adjusted_yield

FROM 
	{{ ref('stg_moex__market_data') }} AS market_data

LEFT JOIN
    {{ ref('int_adjusted_close') }} AS adjusted_close
    ON adjusted_close.secid = market_data.secid
    AND adjusted_close.trading_dt__start = market_data.trading_dt__start

WHERE True
    AND toDayOfWeek(toTimeZone(market_data.trading_dt__start, 'Europe/Moscow')) not in (6,7)