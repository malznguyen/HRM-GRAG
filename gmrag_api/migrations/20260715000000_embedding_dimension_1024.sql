DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM document_chunks LIMIT 1)
        OR EXISTS (SELECT 1 FROM graph_nodes LIMIT 1)
        OR EXISTS (SELECT 1 FROM graph_node_sources LIMIT 1)
    THEN
        RAISE EXCEPTION '1024-d embedding migration requires empty document_chunks, graph_nodes, and graph_node_sources tables';
    END IF;
END
$$;

DROP INDEX IF EXISTS document_chunks_embedding_idx;
DROP INDEX IF EXISTS graph_nodes_embedding_hnsw_idx;
DROP INDEX IF EXISTS graph_node_sources_embedding_hnsw_idx;

ALTER TABLE document_chunks
    ALTER COLUMN embedding TYPE vector(1024);
ALTER TABLE graph_nodes
    ALTER COLUMN embedding TYPE vector(1024);
ALTER TABLE graph_node_sources
    ALTER COLUMN embedding TYPE vector(1024);

CREATE INDEX document_chunks_embedding_idx
    ON document_chunks USING hnsw (embedding vector_cosine_ops);
CREATE INDEX graph_nodes_embedding_hnsw_idx
    ON graph_nodes USING hnsw (embedding vector_l2_ops);
CREATE INDEX graph_node_sources_embedding_hnsw_idx
    ON graph_node_sources USING hnsw (embedding vector_l2_ops);
