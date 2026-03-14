CREATE TABLE source.securities
(
    secid      LowCardinality(String),
    shortname  String,
    isin       String,
    lotsize    UInt32
)
ENGINE = MergeTree
ORDER BY secid;