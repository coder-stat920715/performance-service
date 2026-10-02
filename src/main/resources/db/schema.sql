CREATE TABLE IF NOT EXISTS transactions (
    id          UUID           PRIMARY KEY,
    reference   VARCHAR(64)    NOT NULL,
    account_id  VARCHAR(64)    NOT NULL,
    amount      NUMERIC(19, 4) NOT NULL CHECK (amount > 0),
    currency    VARCHAR(3)     NOT NULL,
    type        VARCHAR(16)    NOT NULL,
    status      VARCHAR(16)    NOT NULL,
    description VARCHAR(255),
    created_at  TIMESTAMPTZ    NOT NULL,
    updated_at  TIMESTAMPTZ    NOT NULL,
    version     BIGINT         NOT NULL DEFAULT 0,
    CONSTRAINT uq_transactions_reference UNIQUE (reference)
);

-- Keyset pagination per account: O(log N + k)
CREATE INDEX IF NOT EXISTS idx_transactions_account_created
    ON transactions (account_id, created_at DESC);

-- Small partial index for backlog sweeps of unprocessed rows
CREATE INDEX IF NOT EXISTS idx_transactions_pending
    ON transactions (created_at)
    WHERE status = 'PENDING';
