ALTER TABLE documents
    ADD COLUMN IF NOT EXISTS failure_code TEXT NULL,
    ADD COLUMN IF NOT EXISTS failure_message TEXT NULL,
    ADD COLUMN IF NOT EXISTS failed_at TIMESTAMP NULL;

CREATE TABLE ingestion_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id UUID NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    workspace_id UUID NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    status VARCHAR(50) NOT NULL DEFAULT 'QUEUED',
    attempt_count INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 5,
    available_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    lease_expires_at TIMESTAMP NULL,
    claimed_by TEXT NULL,
    claim_token UUID NULL,
    failure_code TEXT NULL,
    failure_message TEXT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP NULL,
    completed_at TIMESTAMP NULL,
    CONSTRAINT ingestion_jobs_status_check
        CHECK (status IN ('QUEUED', 'PROCESSING', 'SUCCEEDED', 'FAILED', 'DEAD')),
    CONSTRAINT ingestion_jobs_attempts_check CHECK (attempt_count >= 0),
    CONSTRAINT ingestion_jobs_max_attempts_check CHECK (max_attempts > 0)
);

CREATE UNIQUE INDEX ingestion_jobs_one_active_per_document_idx
    ON ingestion_jobs (document_id)
    WHERE status IN ('QUEUED', 'PROCESSING');

CREATE INDEX ingestion_jobs_claim_idx
    ON ingestion_jobs (status, available_at, lease_expires_at, created_at)
    WHERE status IN ('QUEUED', 'PROCESSING');

CREATE INDEX ingestion_jobs_document_idx ON ingestion_jobs (document_id);
CREATE INDEX ingestion_jobs_lease_expiry_idx
    ON ingestion_jobs (lease_expires_at)
    WHERE status = 'PROCESSING';
