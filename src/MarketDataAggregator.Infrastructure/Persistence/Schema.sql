CREATE TABLE IF NOT EXISTS market_ticks (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    ticker TEXT NOT NULL,
    price NUMERIC(28, 10) NOT NULL,
    volume NUMERIC(28, 10) NOT NULL,
    exchange_timestamp TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    raw_payload JSONB NOT NULL,
    dedup_hash TEXT NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_market_ticks_dedup UNIQUE (dedup_hash)
);
