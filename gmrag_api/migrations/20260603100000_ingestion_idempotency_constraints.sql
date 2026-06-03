CREATE UNIQUE INDEX IF NOT EXISTS document_chunks_workspace_document_chunk_key
    ON document_chunks (workspace_id, document_id, chunk_index);

CREATE UNIQUE INDEX IF NOT EXISTS graph_nodes_workspace_entity_name_key
    ON graph_nodes (workspace_id, lower(entity_name));

CREATE UNIQUE INDEX IF NOT EXISTS graph_edges_workspace_pair_relationship_key
    ON graph_edges (workspace_id, source_node_id, target_node_id, lower(relationship));
