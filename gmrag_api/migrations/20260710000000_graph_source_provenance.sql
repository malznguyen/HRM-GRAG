ALTER TABLE graph_node_sources
    ADD COLUMN entity_type VARCHAR(100),
    ADD COLUMN description TEXT,
    ADD COLUMN embedding vector(768);

ALTER TABLE graph_edge_sources
    ADD COLUMN relationship TEXT,
    ADD COLUMN description TEXT;
