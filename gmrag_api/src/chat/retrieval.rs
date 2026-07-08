use chrono::NaiveDateTime;
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, sqlx::FromRow)]
pub struct RetrievedChunk {
    pub id: Uuid,
    pub document_id: Uuid,
    pub access_mode: String,
    pub original_text: String,
    pub document_filename: String,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GraphNodeRow {
    pub id: Uuid,
    pub entity_name: String,
    pub entity_type: Option<String>,
    pub description: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GraphEdgeRow {
    pub id: Uuid,
    pub source_name: String,
    pub target_name: String,
    pub relationship: String,
    pub description: Option<String>,
}

#[derive(Debug, Default)]
pub struct GraphContext {
    pub nodes: Vec<GraphNodeRow>,
    pub edges: Vec<GraphEdgeRow>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct ChatHistoryMessage {
    pub role: String,
    pub content: String,
}

#[derive(Debug, sqlx::FromRow)]
pub struct StoredChatMessage {
    pub id: Uuid,
    pub role: String,
    pub content: String,
    pub citations: sqlx::types::Json<Vec<Uuid>>,
    pub created_at: NaiveDateTime,
}

pub async fn fetch_chunks_by_ids(
    pool: &PgPool,
    workspace_id: Uuid,
    chunk_ids: &[Uuid],
) -> Result<Vec<RetrievedChunk>, sqlx::Error> {
    if chunk_ids.is_empty() {
        return Ok(Vec::new());
    }

    tracing::info!(
        %workspace_id,
        chunk_count = chunk_ids.len(),
        "RAG retrieval: loading SQL chunk content by Qdrant-ranked chunk ids"
    );

    sqlx::query_as(
        r#"
        SELECT
            dc.id,
            dc.document_id,
            d.access_mode,
            dc.original_text,
            d.filename AS document_filename
        FROM document_chunks dc
        INNER JOIN documents d
            ON d.id = dc.document_id
            AND d.workspace_id = dc.workspace_id
        WHERE dc.workspace_id = $1
          AND dc.id = ANY($2)
        ORDER BY array_position($2::uuid[], dc.id)
        "#,
    )
    .bind(workspace_id)
    .bind(chunk_ids)
    .fetch_all(pool)
    .await
}

pub async fn fetch_graph_context(
    pool: &PgPool,
    workspace_id: Uuid,
    embedding_literal: &str,
    user_message: &str,
    visible_document_ids: &[Uuid],
) -> Result<GraphContext, sqlx::Error> {
    if visible_document_ids.is_empty() {
        return Ok(GraphContext::default());
    }

    tracing::info!(%workspace_id, "RAG retrieval: running graph node vector search (top 5)");

    let mut nodes: Vec<GraphNodeRow> = sqlx::query_as(
        r#"
        SELECT id, entity_name, entity_type, description
        FROM graph_nodes
        WHERE workspace_id = $1
          AND embedding IS NOT NULL
          AND EXISTS (
            SELECT 1
            FROM graph_node_sources source
            WHERE source.graph_node_id = graph_nodes.id
              AND source.workspace_id = $1
              AND source.document_id = ANY($3)
          )
        ORDER BY embedding <-> $2::vector
        LIMIT 5
        "#,
    )
    .bind(workspace_id)
    .bind(embedding_literal)
    .bind(visible_document_ids)
    .fetch_all(pool)
    .await?;

    if nodes.is_empty() {
        tracing::info!(
            %workspace_id,
            "RAG retrieval: no embedded graph nodes; falling back to ILIKE entity search"
        );
        let pattern = format!("%{}%", user_message.trim());
        nodes = sqlx::query_as(
            r#"
            SELECT id, entity_name, entity_type, description
            FROM graph_nodes
            WHERE workspace_id = $1
              AND EXISTS (
                SELECT 1
                FROM graph_node_sources source
                WHERE source.graph_node_id = graph_nodes.id
                  AND source.workspace_id = $1
                  AND source.document_id = ANY($3)
              )
              AND (
                entity_name ILIKE $2
                OR COALESCE(description, '') ILIKE $2
              )
            LIMIT 5
            "#,
        )
        .bind(workspace_id)
        .bind(pattern)
        .bind(visible_document_ids)
        .fetch_all(pool)
        .await?;
    }

    if nodes.is_empty() {
        return Ok(GraphContext::default());
    }

    let node_ids: Vec<Uuid> = nodes.iter().map(|n| n.id).collect();

    tracing::info!(
        %workspace_id,
        matched_nodes = node_ids.len(),
        "RAG retrieval: loading graph edges for matched nodes"
    );

    let edges = sqlx::query_as(
        r#"
        SELECT
            e.id,
            sn.entity_name AS source_name,
            tn.entity_name AS target_name,
            e.relationship,
            e.description
        FROM graph_edges e
        INNER JOIN graph_nodes sn
            ON sn.id = e.source_node_id AND sn.workspace_id = $1
        INNER JOIN graph_nodes tn
            ON tn.id = e.target_node_id AND tn.workspace_id = $1
        WHERE e.workspace_id = $1
          AND EXISTS (
            SELECT 1
            FROM graph_edge_sources source
            WHERE source.graph_edge_id = e.id
              AND source.workspace_id = $1
              AND source.document_id = ANY($3)
          )
          AND (e.source_node_id = ANY($2) OR e.target_node_id = ANY($2))
        "#,
    )
    .bind(workspace_id)
    .bind(&node_ids)
    .bind(visible_document_ids)
    .fetch_all(pool)
    .await?;

    Ok(GraphContext { nodes, edges })
}

pub async fn fetch_chat_history(
    pool: &PgPool,
    session_id: Uuid,
    workspace_id: Uuid,
) -> Result<Vec<ChatHistoryMessage>, sqlx::Error> {
    let rows: Vec<ChatHistoryMessage> = sqlx::query_as(
        r#"
        SELECT cm.role, cm.content
        FROM chat_messages cm
        INNER JOIN chat_sessions cs ON cs.id = cm.session_id
        WHERE cm.session_id = $1
          AND cs.workspace_id = $2
        ORDER BY cm.created_at DESC
        LIMIT 5
        "#,
    )
    .bind(session_id)
    .bind(workspace_id)
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().rev().collect())
}

pub async fn fetch_session_chat_messages(
    pool: &PgPool,
    session_id: Uuid,
    workspace_id: Uuid,
    user_id: &str,
) -> Result<Vec<StoredChatMessage>, sqlx::Error> {
    sqlx::query_as(
        r#"
        SELECT cm.id, cm.role, cm.content, cm.citations, cm.created_at
        FROM chat_messages cm
        INNER JOIN chat_sessions cs ON cs.id = cm.session_id
        WHERE cm.session_id = $1
          AND cs.workspace_id = $2
          AND cs.user_id = $3
        ORDER BY cm.created_at ASC
        "#,
    )
    .bind(session_id)
    .bind(workspace_id)
    .bind(user_id)
    .fetch_all(pool)
    .await
}
