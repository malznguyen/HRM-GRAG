-- Claim/backoff/poison cho qdrant_outbox processor.
-- next_attempt_at: delay retry (backoff) + lease tạm khi worker claim row.
-- DEAD: poison message (payload hỏng hoặc hết max retries) — không retry tự động.

ALTER TABLE qdrant_outbox
    ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;

-- Mở rộng CHECK status: thêm DEAD (PostgreSQL không cho ALTER CHECK trực tiếp).
ALTER TABLE qdrant_outbox DROP CONSTRAINT IF EXISTS qdrant_outbox_status_check;
ALTER TABLE qdrant_outbox
    ADD CONSTRAINT qdrant_outbox_status_check
    CHECK (status IN ('PENDING', 'PROCESSED', 'FAILED', 'DEAD'));

-- Index claim path: status retryable + next_attempt_at + FIFO created_at.
DROP INDEX IF EXISTS qdrant_outbox_status_retry_created_idx;
CREATE INDEX IF NOT EXISTS qdrant_outbox_claim_idx
    ON qdrant_outbox (status, next_attempt_at, created_at)
    WHERE status IN ('PENDING', 'FAILED');
