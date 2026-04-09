CREATE TABLE source.centralbank_rates_ru
(
    periodicity     LowCardinality(String),
    obs_val         Float64,
    date            DateTime('Europe/Moscow'),
    version         DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(version)
ORDER BY (periodicity, date);