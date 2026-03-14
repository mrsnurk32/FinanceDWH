CREATE TABLE source.market_data
(
    secid   LowCardinality(String),

    open    Float64,
    close   Float64,
    high    Float64,
    low     Float64,
    value   Float64,
    volume  UInt64,

    begin   DateTime('Europe/Moscow'),
    end     DateTime('Europe/Moscow')
)
ENGINE = MergeTree
ORDER BY (secid, begin);