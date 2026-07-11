-- Outbox recovery cho S3/MinIO object cleanup (tách khỏi authz_outbox / qdrant_outbox).
-- Claim lease + backoff + DEAD theo convention outbox dùng chung (LIFE-003).
-- event_type: delete_object | delete_prefix

CREATE TABLE IF NOT EXISTS storage_outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'PROCESSED', 'FAILED', 'DEAD')),
    retry_count INT NOT NULL DEFAULT 0,
    error_message TEXT,
    next_attempt_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Claim path: status retryable + next_attempt_at + FIFO created_at.
CREATE INDEX IF NOT EXISTS storage_outbox_claim_idx
    ON storage_outbox (status, next_attempt_at, created_at)
    WHERE status IN ('PENDING', 'FAILED');
