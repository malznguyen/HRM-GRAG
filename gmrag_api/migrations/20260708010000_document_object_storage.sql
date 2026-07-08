ALTER TABLE documents
    ADD COLUMN IF NOT EXISTS object_key TEXT,
    ADD COLUMN IF NOT EXISTS bucket TEXT,
    ADD COLUMN IF NOT EXISTS content_type TEXT,
    ADD COLUMN IF NOT EXISTS size_bytes BIGINT,
    ADD COLUMN IF NOT EXISTS checksum_sha256 TEXT,
    ADD COLUMN IF NOT EXISTS storage_etag TEXT,
    ADD COLUMN IF NOT EXISTS uploaded_by TEXT;

UPDATE documents
SET
    object_key = COALESCE(
        object_key,
        FORMAT('legacy/workspaces/%s/documents/%s/original.pdf', workspace_id, id)
    ),
    bucket = COALESCE(bucket, 'legacy-local-disk'),
    uploaded_by = COALESCE(uploaded_by, owner_id, 'legacy-uploader')
WHERE object_key IS NULL OR bucket IS NULL OR uploaded_by IS NULL;

ALTER TABLE documents
    ALTER COLUMN object_key SET NOT NULL,
    ALTER COLUMN bucket SET NOT NULL,
    ALTER COLUMN uploaded_by SET NOT NULL;
