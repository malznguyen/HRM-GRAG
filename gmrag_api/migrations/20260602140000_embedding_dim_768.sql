-- nomic-embed-text produces 768-dimensional vectors; align schema with Ollama model output.
DROP INDEX IF EXISTS document_chunks_embedding_idx;

ALTER TABLE document_chunks
    ALTER COLUMN embedding TYPE vector(768);

ALTER TABLE graph_nodes
    ALTER COLUMN embedding TYPE vector(768);

CREATE INDEX ON document_chunks USING hnsw (embedding vector_cosine_ops);
