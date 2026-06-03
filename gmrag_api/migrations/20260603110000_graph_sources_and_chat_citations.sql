CREATE TABLE graph_node_sources (
    graph_node_id UUID REFERENCES graph_nodes(id) ON DELETE CASCADE,
    document_id UUID REFERENCES documents(id) ON DELETE CASCADE,
    workspace_id UUID REFERENCES workspaces(id) ON DELETE CASCADE,
    PRIMARY KEY (graph_node_id, document_id)
);

CREATE INDEX graph_node_sources_workspace_document_idx
    ON graph_node_sources (workspace_id, document_id);

CREATE TABLE graph_edge_sources (
    graph_edge_id UUID REFERENCES graph_edges(id) ON DELETE CASCADE,
    document_id UUID REFERENCES documents(id) ON DELETE CASCADE,
    workspace_id UUID REFERENCES workspaces(id) ON DELETE CASCADE,
    PRIMARY KEY (graph_edge_id, document_id)
);

CREATE INDEX graph_edge_sources_workspace_document_idx
    ON graph_edge_sources (workspace_id, document_id);

ALTER TABLE chat_messages
    ADD COLUMN citations JSONB NOT NULL DEFAULT '[]'::jsonb;
