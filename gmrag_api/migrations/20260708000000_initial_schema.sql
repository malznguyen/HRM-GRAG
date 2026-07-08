-- Kích hoạt tiện ích mở rộng vector cho embeddings
CREATE EXTENSION IF NOT EXISTS vector;

-- Bảng Tenants
CREATE TABLE tenants (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bảng Users (ID là Keycloak sub dạng VARCHAR)
CREATE TABLE users (
    id VARCHAR(255) PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bảng Thành viên Tenant
CREATE TABLE tenant_members (
    tenant_id UUID REFERENCES tenants(id) ON DELETE CASCADE,
    user_id VARCHAR(255) REFERENCES users(id) ON DELETE CASCADE,
    -- CẢNH BÁO BẢO MẬT: Cột 'role' này CHỈ được dùng để hiển thị trên giao diện (UI).
    -- TUYỆT ĐỐI KHÔNG dùng cột này để kiểm tra hoặc thực thi quyền (enforce authorization)
    -- ở bất kỳ route handler hay middleware nào trong backend.
    -- Mọi quy trình check quyền bắt buộc phải sử dụng AuthzClient để gọi sang OpenFGA.
    role VARCHAR(50) NOT NULL CHECK (role IN ('OWNER')),
    joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, user_id)
);

CREATE INDEX ON tenant_members (user_id);

-- Bảng Workspaces
CREATE TABLE workspaces (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID REFERENCES tenants(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bảng Thành viên Workspace
CREATE TABLE workspace_members (
    workspace_id UUID REFERENCES workspaces(id) ON DELETE CASCADE,
    user_id VARCHAR(255) REFERENCES users(id) ON DELETE CASCADE,
    -- CẢNH BÁO BẢO MẬT: Cột 'role' này CHỈ được dùng để hiển thị trên giao diện (UI).
    -- TUYỆT ĐỐI KHÔNG dùng cột này để kiểm tra hoặc thực thi quyền (enforce authorization)
    -- ở bất kỳ route handler hay middleware nào trong backend.
    -- Mọi quy trình check quyền bắt buộc phải sử dụng AuthzClient để gọi sang OpenFGA.
    role VARCHAR(50) NOT NULL CHECK (role IN ('ADMIN', 'MEMBER')),
    joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (workspace_id, user_id)
);

CREATE INDEX ON workspace_members (user_id);

-- Bảng Documents (Bổ dung owner_id và access_mode)
CREATE TABLE documents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id UUID REFERENCES workspaces(id) ON DELETE CASCADE,
    owner_id VARCHAR(255) REFERENCES users(id) ON DELETE SET NULL,
    filename VARCHAR(255) NOT NULL,
    status VARCHAR(50) DEFAULT 'PROCESSING',
    processing_stage VARCHAR(50) NOT NULL DEFAULT 'QUEUED',
    access_mode VARCHAR(50) NOT NULL DEFAULT 'workspace_default' CHECK (access_mode IN ('workspace_default', 'restricted')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bảng Document Shares (Lưu trữ danh sách chia sẻ explicit_viewer để hiển thị UI)
CREATE TABLE document_shares (
    document_id UUID REFERENCES documents(id) ON DELETE CASCADE,
    user_id VARCHAR(255) REFERENCES users(id) ON DELETE CASCADE,
    shared_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (document_id, user_id)
);

-- Bảng Vector Chunks (Kích thước vector là 768 theo mô hình embedding tiếng Việt)
CREATE TABLE document_chunks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id UUID REFERENCES documents(id) ON DELETE CASCADE,
    workspace_id UUID REFERENCES workspaces(id) ON DELETE CASCADE,
    chunk_index INT NOT NULL,
    original_text TEXT NOT NULL,
    embedding vector(768)
);

-- Index cho tìm kiếm vector HNSW
CREATE INDEX ON document_chunks USING hnsw (embedding vector_cosine_ops);

-- Index Unique để đảm bảo tính bất biến (idempotency) khi nạp document chunk
CREATE UNIQUE INDEX document_chunks_workspace_document_chunk_key 
    ON document_chunks (workspace_id, document_id, chunk_index);

-- Bảng Knowledge Graph Nodes
CREATE TABLE graph_nodes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id UUID REFERENCES workspaces(id) ON DELETE CASCADE,
    entity_name VARCHAR(255) NOT NULL,
    entity_type VARCHAR(100),
    description TEXT,
    embedding vector(768)
);

CREATE UNIQUE INDEX graph_nodes_workspace_entity_name_key 
    ON graph_nodes (workspace_id, lower(entity_name));

-- Bảng Knowledge Graph Edges
CREATE TABLE graph_edges (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id UUID REFERENCES workspaces(id) ON DELETE CASCADE,
    source_node_id UUID REFERENCES graph_nodes(id) ON DELETE CASCADE,
    target_node_id UUID REFERENCES graph_nodes(id) ON DELETE CASCADE,
    relationship TEXT NOT NULL,
    description TEXT,
    weight FLOAT DEFAULT 1.0
);

CREATE UNIQUE INDEX graph_edges_workspace_pair_relationship_key 
    ON graph_edges (workspace_id, source_node_id, target_node_id, lower(relationship));

-- Bảng Nguồn của Nodes/Edges
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

-- Bảng Chat Sessions
CREATE TABLE chat_sessions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id UUID REFERENCES workspaces(id) ON DELETE CASCADE,
    user_id VARCHAR(255) REFERENCES users(id) ON DELETE CASCADE,
    title VARCHAR(255) DEFAULT 'New Chat',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bảng Chat Messages
CREATE TABLE chat_messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id UUID REFERENCES chat_sessions(id) ON DELETE CASCADE,
    role VARCHAR(50) CHECK (role IN ('user', 'assistant')),
    content TEXT NOT NULL,
    citations JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX ON chat_messages(session_id);

-- Bảng Outbox cho đồng bộ OpenFGA
CREATE TABLE authz_outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'PROCESSED', 'FAILED')),
    retry_count INT NOT NULL DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
