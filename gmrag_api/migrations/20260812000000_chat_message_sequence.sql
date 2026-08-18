-- PHASE 16: reserve two message positions atomically for every chat turn.
--
-- `message_sequence` is the ordering key for messages in one session. Existing
-- rows are backfilled with their observable order only; this does not recover
-- an assistant/user relationship that was lost because of the old race.

ALTER TABLE chat_sessions
    ADD COLUMN IF NOT EXISTS next_message_sequence BIGINT NOT NULL DEFAULT 1;

ALTER TABLE chat_messages
    ADD COLUMN IF NOT EXISTS message_sequence BIGINT;

-- Idempotent backfill. A rerun never rewrites an already assigned sequence.
-- The ROW_NUMBER order is (created_at, id), exactly the historical order that
-- the pre-Phase-16 API exposed. It is not a semantic conversation backfill.
WITH ranked AS (
    SELECT
        id,
        ROW_NUMBER() OVER (
            PARTITION BY session_id
            ORDER BY created_at ASC, id ASC
        )::bigint AS sequence_value
    FROM chat_messages
)
UPDATE chat_messages AS cm
SET message_sequence = ranked.sequence_value
FROM ranked
WHERE cm.id = ranked.id
  AND cm.message_sequence IS NULL;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM chat_messages
        WHERE message_sequence IS NULL
    ) THEN
        RAISE EXCEPTION 'chat_messages.message_sequence backfill left NULL rows';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'chat_messages'::regclass
          AND conname = 'chat_messages_session_message_sequence_key'
    ) THEN
        ALTER TABLE chat_messages
            ADD CONSTRAINT chat_messages_session_message_sequence_key
            UNIQUE (session_id, message_sequence);
    END IF;
END
$$;

ALTER TABLE chat_messages
    ALTER COLUMN message_sequence SET NOT NULL;

-- Keep the per-session allocator ahead of both the migration backfill and any
-- already-present rows. GREATEST makes this safe to rerun after new writes.
UPDATE chat_sessions AS cs
SET next_message_sequence = GREATEST(
    cs.next_message_sequence,
    COALESCE(
        (
            SELECT MAX(cm.message_sequence) + 1
            FROM chat_messages AS cm
            WHERE cm.session_id = cs.id
        ),
        1
    )
);
