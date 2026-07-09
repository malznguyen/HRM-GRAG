-- HNSW L2 vì retrieval graph dùng operator <-> (chat/retrieval.rs), không phải cosine <=>.
-- Không dùng CONCURRENTLY: sqlx migrate bọc migration trong transaction, CONCURRENTLY không chạy được.
-- CREATE INDEX thường có thể lock graph_nodes lúc build — khuyến nghị chạy off-peak trên DB lớn (xem RUNBOOK §9).
CREATE INDEX IF NOT EXISTS graph_nodes_embedding_hnsw_idx
    ON graph_nodes USING hnsw (embedding vector_l2_ops);
