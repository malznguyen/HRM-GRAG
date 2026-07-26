-- Index tra cứu multi-tenant cho documents và chat_sessions.
-- FK REFERENCES không tự tạo index trong PostgreSQL, nên hai bảng này đang seq scan
-- toàn bộ dữ liệu của mọi tenant trên hot path (RAG context + documents list + chat sidebar).
-- CREATE INDEX lock bảng lúc build — chạy off-peak nếu DB đã lớn.
-- Không dùng CONCURRENTLY: sqlx chạy mỗi migration trong một transaction.

-- Phục vụ fetch_completed_workspace_document_acl_rows (hot path mỗi lượt chat)
-- và các query documents directory (leading column workspace_id).
CREATE INDEX IF NOT EXISTS documents_workspace_status_stage_idx
    ON documents (workspace_id, status, processing_stage);

-- Phục vụ list chat sessions: WHERE workspace_id = $1 AND user_id = $2 ORDER BY created_at DESC.
CREATE INDEX IF NOT EXISTS chat_sessions_workspace_user_created_idx
    ON chat_sessions (workspace_id, user_id, created_at DESC);
