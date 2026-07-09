CREATE TABLE IF NOT EXISTS audit_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    actor_user_id TEXT,
    tenant_id UUID,
    workspace_id UUID,
    document_id UUID,
    event_type TEXT NOT NULL,
    target_type TEXT,
    target_id TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS audit_events_created_at_idx
    ON audit_events (created_at DESC);

CREATE INDEX IF NOT EXISTS audit_events_event_type_idx
    ON audit_events (event_type, created_at DESC);

CREATE INDEX IF NOT EXISTS authz_outbox_status_retry_created_idx
    ON authz_outbox (status, retry_count, created_at);
