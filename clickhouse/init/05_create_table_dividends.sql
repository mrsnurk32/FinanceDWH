CREATE TABLE source.dividends (
    secid               LowCardinality(String),
    isin                String,
    registryclosedate   DateTime('Europe/Moscow'),
    value               Float64,
    currencyid          LowCardinality(String),
    version             DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(version)
ORDER BY (secid, registryclosedate, value);