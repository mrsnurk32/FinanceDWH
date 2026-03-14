CREATE TABLE source.security_registry_events
(
    secid              LowCardinality(String),
    isin               String,
    registryclosedate  Date,
    value              Float64,
    currencyid         LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (secid, registryclosedate);