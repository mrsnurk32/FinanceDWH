CREATE TABLE source.securities_last_records
(
    secid LowCardinality(String),
    last_security_record AggregateFunction(max, DateTime)
)
ENGINE = AggregatingMergeTree
ORDER BY secid;


CREATE MATERIALIZED VIEW source.mv_securities_last_records
TO source.securities_last_records
AS
SELECT
    secid,
    maxState(begin) AS last_security_record
FROM source.market_data
GROUP BY secid;