pub mod deepseek;
pub mod retrieval;

use std::collections::HashSet;
use std::sync::LazyLock;
use std::time::Instant;

use chrono::NaiveDateTime;
use regex::Regex;
use serde::Serialize;
use sqlx::PgPool;
use uuid::Uuid;

use crate::auth::authz::AuthzClient;
use crate::auth::document_acl::{
    DocumentAccessMode, DocumentAclError, can_user_view_document, collect_viewable_document_ids,
    fetch_completed_workspace_document_acl_rows,
};
use crate::ingestion::embedding::{EmbedError, embed_text, format_pgvector};
use crate::retrieval::{RetrievalClient, RetrievalError};
use crate::telemetry;

use self::deepseek::{ChatMessage, DeepseekStreamError, stream_chat_completion};
use self::retrieval::{
    GraphContext, RetrievedChunk, fetch_chat_history, fetch_chunks_by_ids, fetch_graph_context,
};

pub const CITATION_INSTRUCTION: &str = "When citing information from a chunk, you MUST append [chunk:1], [chunk:2], etc., at the exact end of the sentence based on its CHUNK INDEX number. Do not output raw UUIDs under any circumstance.";

static CHUNK_CITATION_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\[chunk:([0-9a-fA-F\-]{36})\]").expect("chunk citation regex must compile")
});

static CHUNK_INDEX_CITATION_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\[chunk:(\d+)\]").expect("chunk index citation regex must compile")
});

/// Replaces `[chunk:<index>]` markers with `[chunk:<uuid>]` and returns deduplicated citations.
pub fn resolve_chunk_index_citations(text: &str, chunk_ids: &[Uuid]) -> (String, Vec<Uuid>) {
    let mut seen = HashSet::new();
    let mut citations = Vec::new();

    let resolved = CHUNK_INDEX_CITATION_RE.replace_all(text, |caps: &regex::Captures| {
        let Some(index_match) = caps.get(1) else {
            return caps[0].to_string();
        };
        let Ok(index) = index_match.as_str().parse::<usize>() else {
            return caps[0].to_string();
        };
        if index == 0 || index > chunk_ids.len() {
            return caps[0].to_string();
        }

        let uuid = chunk_ids[index - 1];
        if seen.insert(uuid) {
            citations.push(uuid);
        }
        format!("[chunk:{uuid}]")
    });

    for uuid in extract_chunk_citations(&resolved) {
        if seen.insert(uuid) {
            citations.push(uuid);
        }
    }

    (resolved.into_owned(), citations)
}

/// Extracts unique chunk UUIDs from `[chunk:<uuid>]` markers in assistant text.
pub fn extract_chunk_citations(text: &str) -> Vec<Uuid> {
    let mut seen = HashSet::new();
    let mut citations = Vec::new();

    for cap in CHUNK_CITATION_RE.captures_iter(text) {
        let Some(uuid_match) = cap.get(1) else {
            continue;
        };
        let Ok(uuid) = Uuid::parse_str(uuid_match.as_str()) else {
            continue;
        };
        if seen.insert(uuid) {
            citations.push(uuid);
        }
    }

    citations
}

#[derive(Debug)]
pub enum ChatPipelineError {
    Embed(EmbedError),
    Generation(DeepseekStreamError),
    Retrieval(RetrievalError),
    AccessControl(DocumentAclError),
    Database(sqlx::Error),
}

impl std::fmt::Display for ChatPipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChatPipelineError::Embed(e) => write!(f, "{e}"),
            ChatPipelineError::Generation(e) => write!(f, "{e}"),
            ChatPipelineError::Retrieval(e) => write!(f, "{e}"),
            ChatPipelineError::AccessControl(e) => write!(f, "{e}"),
            ChatPipelineError::Database(e) => write!(f, "database error: {e}"),
        }
    }
}

impl std::error::Error for ChatPipelineError {}

impl From<sqlx::Error> for ChatPipelineError {
    fn from(value: sqlx::Error) -> Self {
        ChatPipelineError::Database(value)
    }
}

impl From<RetrievalError> for ChatPipelineError {
    fn from(value: RetrievalError) -> Self {
        ChatPipelineError::Retrieval(value)
    }
}

impl From<DocumentAclError> for ChatPipelineError {
    fn from(value: DocumentAclError) -> Self {
        ChatPipelineError::AccessControl(value)
    }
}

pub struct ChatContext {
    pub system_prompt: String,
    pub messages: Vec<ChatMessage>,
    /// Ordered chunk UUIDs; index 1 in prompts maps to `chunk_ids[0]`.
    pub chunk_ids: Vec<Uuid>,
}

pub async fn build_chat_context(
    pool: &PgPool,
    retrieval: &RetrievalClient,
    authz_client: &AuthzClient,
    client: &reqwest::Client,
    workspace_id: Uuid,
    session_id: Uuid,
    user_id: &str,
    user_message: &str,
) -> Result<ChatContext, ChatPipelineError> {
    // ADR-21: cùng model với ingestion (`ollama_embed_model` / embed_text) — không hard-code model khác.
    tracing::info!(
        %workspace_id,
        %session_id,
        query_len = user_message.len(),
        "RAG pipeline: embedding user query via Ollama (shared ADR-21 model)"
    );

    let embedding_started = Instant::now();
    let embedding = embed_text(client, user_message)
        .await
        .map_err(ChatPipelineError::Embed)?;
    telemetry::record_model_latency(
        "ollama",
        "chat_query_embedding",
        embedding_started.elapsed(),
    );
    let embedding_literal = format_pgvector(&embedding);

    tracing::info!(
        %workspace_id,
        embedding_dims = embedding.len(),
        "RAG pipeline: query embedding complete"
    );

    let acl_rows = fetch_completed_workspace_document_acl_rows(pool, workspace_id).await?;
    let visible_document_ids = collect_viewable_document_ids(authz_client, user_id, &acl_rows)
        .await?
        .into_iter()
        .collect::<Vec<_>>();

    retrieval
        .ensure_workspace_backfilled(pool, workspace_id)
        .await?;

    let retrieved_chunk_ids = retrieval
        .search_chunk_ids(
            workspace_id,
            &visible_document_ids,
            &embedding,
            retrieval.top_k(),
        )
        .await?;

    let qdrant_chunks = fetch_chunks_by_ids(pool, workspace_id, &retrieved_chunk_ids).await?;
    let chunks = recheck_retrieved_chunks(authz_client, user_id, qdrant_chunks).await?;
    log_retrieved_chunks(workspace_id, &chunks);

    let graph = fetch_graph_context(
        pool,
        workspace_id,
        &embedding_literal,
        user_message,
        &visible_document_ids,
    )
    .await?;
    log_graph_context(workspace_id, &graph);

    let history = fetch_chat_history(pool, session_id, workspace_id).await?;
    tracing::info!(
        %workspace_id,
        %session_id,
        history_messages = history.len(),
        roles = ?history.iter().map(|m| m.role.as_str()).collect::<Vec<_>>(),
        "RAG pipeline: loaded chat history"
    );

    let chunk_ids: Vec<Uuid> = chunks.iter().map(|chunk| chunk.id).collect();
    let system_prompt = assemble_system_prompt(&chunks, &graph);
    tracing::info!(
        %workspace_id,
        system_prompt_chars = system_prompt.len(),
        chunk_count = chunk_ids.len(),
        "RAG pipeline: assembled augmented system prompt"
    );

    let mut messages = Vec::with_capacity(history.len() + 1);

    for msg in &history {
        messages.push(ChatMessage {
            role: msg.role.clone(),
            content: msg.content.clone(),
        });
    }

    if history.last().map(|m| m.role.as_str()) != Some("user")
        || history.last().map(|m| m.content.as_str()) != Some(user_message)
    {
        messages.push(ChatMessage {
            role: "user".to_string(),
            content: user_message.to_string(),
        });
    }

    Ok(ChatContext {
        system_prompt,
        messages,
        chunk_ids,
    })
}

pub async fn prepare_deepseek_stream(
    client: &reqwest::Client,
    context: &ChatContext,
) -> Result<reqwest::Response, ChatPipelineError> {
    tracing::info!(
        model = %deepseek::deepseek_chat_model(),
        url = %deepseek::deepseek_chat_url(),
        message_count = context.messages.len(),
        "RAG pipeline: starting DeepSeek text generation stream"
    );

    let generation_started = Instant::now();
    let response = stream_chat_completion(client, &context.system_prompt, &context.messages)
        .await
        .map_err(ChatPipelineError::Generation)?;

    telemetry::record_model_latency(
        "deepseek",
        "chat_stream_request",
        generation_started.elapsed(),
    );

    Ok(response)
}

#[derive(sqlx::FromRow)]
struct CitationChunkAclRow {
    chunk_id: Uuid,
    document_id: Uuid,
    access_mode: String,
}

pub async fn filter_citation_ids_for_user(
    pool: &PgPool,
    authz_client: &AuthzClient,
    workspace_id: Uuid,
    user_id: &str,
    citations: &[Uuid],
) -> Result<Vec<Uuid>, ChatPipelineError> {
    if citations.is_empty() {
        return Ok(Vec::new());
    }

    let rows: Vec<CitationChunkAclRow> = sqlx::query_as(
        r#"
        SELECT
            dc.id AS chunk_id,
            dc.document_id,
            d.access_mode
        FROM document_chunks dc
        INNER JOIN documents d
            ON d.id = dc.document_id
           AND d.workspace_id = dc.workspace_id
        WHERE dc.workspace_id = $1
          AND dc.id = ANY($2)
        "#,
    )
    .bind(workspace_id)
    .bind(citations)
    .fetch_all(pool)
    .await?;

    let row_by_chunk: std::collections::HashMap<Uuid, CitationChunkAclRow> =
        rows.into_iter().map(|row| (row.chunk_id, row)).collect();

    let mut allowed = Vec::new();
    for citation in citations {
        let Some(row) = row_by_chunk.get(citation) else {
            continue;
        };

        let access_mode = DocumentAccessMode::parse(&row.access_mode).ok_or_else(|| {
            ChatPipelineError::AccessControl(DocumentAclError::InvalidAccessMode {
                document_id: row.document_id,
                raw_mode: row.access_mode.clone(),
            })
        })?;

        if can_user_view_document(authz_client, user_id, row.document_id, access_mode).await? {
            allowed.push(*citation);
        }
    }

    Ok(allowed)
}

pub async fn filter_citations_for_user(
    pool: &PgPool,
    authz_client: &AuthzClient,
    workspace_id: Uuid,
    user_id: &str,
    assistant_text: &str,
    citations: &[Uuid],
) -> Result<(String, Vec<Uuid>), ChatPipelineError> {
    let allowed =
        filter_citation_ids_for_user(pool, authz_client, workspace_id, user_id, citations).await?;

    Ok(filter_citations_for_allowed_ids(
        assistant_text,
        citations,
        &allowed,
    ))
}

/// Applies the read-time ACL result to one persisted assistant message.
///
/// History callers pass the union of all citation IDs from the page to
/// `filter_citation_ids_for_user` once, then use this pure projection for each
/// message. This preserves the marker-removal behavior without issuing one
/// authorization query per message.
pub fn filter_citations_for_allowed_ids(
    assistant_text: &str,
    citations: &[Uuid],
    allowed: &[Uuid],
) -> (String, Vec<Uuid>) {
    let allowed_set: HashSet<Uuid> = allowed.iter().copied().collect();
    let mut sanitized = assistant_text.to_string();
    for citation in citations {
        if allowed_set.contains(citation) {
            continue;
        }

        let marker = format!("[chunk:{citation}]");
        sanitized = sanitized.replace(&marker, "");
    }

    let mut visible = Vec::new();
    let mut seen = HashSet::new();
    for citation in citations {
        if allowed_set.contains(citation) && seen.insert(*citation) {
            visible.push(*citation);
        }
    }

    (sanitized, visible)
}

async fn recheck_retrieved_chunks(
    authz_client: &AuthzClient,
    user_id: &str,
    chunks: Vec<RetrievedChunk>,
) -> Result<Vec<RetrievedChunk>, ChatPipelineError> {
    let mut allowed_chunks = Vec::with_capacity(chunks.len());

    for chunk in chunks {
        let access_mode = DocumentAccessMode::parse(&chunk.access_mode).ok_or_else(|| {
            ChatPipelineError::AccessControl(DocumentAclError::InvalidAccessMode {
                document_id: chunk.document_id,
                raw_mode: chunk.access_mode.clone(),
            })
        })?;

        if can_user_view_document(authz_client, user_id, chunk.document_id, access_mode).await? {
            allowed_chunks.push(chunk);
        }
    }

    Ok(allowed_chunks)
}

fn log_retrieved_chunks(workspace_id: Uuid, chunks: &[RetrievedChunk]) {
    if chunks.is_empty() {
        tracing::info!(%workspace_id, "RAG pipeline: no document chunks retrieved");
        return;
    }

    // Chỉ log metadata (id, độ dài) — không log nội dung chunk (xem ADR logging metadata-only)
    for chunk in chunks {
        tracing::info!(
            %workspace_id,
            chunk_id = %chunk.id,
            text_len = chunk.original_text.len(),
            "RAG pipeline: retrieved document chunk"
        );
    }

    tracing::info!(
        %workspace_id,
        chunk_count = chunks.len(),
        chunk_ids = ?chunks.iter().map(|c| c.id).collect::<Vec<_>>(),
        "RAG pipeline: vector search complete"
    );
}

fn log_graph_context(workspace_id: Uuid, graph: &GraphContext) {
    if graph.nodes.is_empty() && graph.edges.is_empty() {
        tracing::info!(%workspace_id, "RAG pipeline: no graph context retrieved");
        return;
    }

    // Chỉ log id/count/nullness — không log tên entity, mô tả, quan hệ
    for node in &graph.nodes {
        tracing::info!(
            %workspace_id,
            node_id = %node.id,
            has_description = node.description.is_some(),
            "RAG pipeline: retrieved graph node"
        );
    }

    for edge in &graph.edges {
        tracing::info!(
            %workspace_id,
            edge_id = %edge.id,
            "RAG pipeline: retrieved graph edge"
        );
    }

    tracing::info!(
        %workspace_id,
        node_count = graph.nodes.len(),
        edge_count = graph.edges.len(),
        "RAG pipeline: graph search complete"
    );
}

fn assemble_system_prompt(chunks: &[RetrievedChunk], graph: &GraphContext) -> String {
    let mut prompt = String::from(
        "You are a highly intelligent, analytical, and helpful assistant.\n\
Use the provided reference material to answer the user's question comprehensively.\n\
1. Provide detailed, well-structured answers.\n\
2. Use markdown formatting, bullet points, and bold text to organize complex information.\n\
3. When presenting rows and columns, use a valid GitHub-Flavored Markdown table with pipe separators and a header separator row. Never align table columns using spaces.\n\
4. If the context contains multiple aspects, explain all of them thoroughly.\n\
5. DO NOT give one-sentence answers unless explicitly asked for a summary.\n\
Do not use emoji or other pictographs in responses. Keep a professional tone.\n\
IMPORTANT: In your prose, never mention chunks, provided documents, context, a knowledge graph, nodes, edges, or how you retrieved or received information. Answer directly as if the knowledge is your own. If the user asks about sources, identify relevant document titles naturally and use the required citation markers; never describe the retrieval process or say that documents were provided.\n\
Never expose internal system vocabulary to the user. Do not use the words \"chunk\", \"chunks\", \"embedding\", \"vector\", \"retrieval\", \"index\", or any implementation detail in your answer. Refer to sources only as \"tài liệu\". Do not describe how you searched, what you retrieved, or how many passages you found, and do not narrate your own process.\n\
GROUNDING (STRICT — applies to every sentence you write):\n\
- Every substantive claim must be directly supported by a statement actually present in the reference material. Do not assert conclusions, causes, judgments, or outcomes that the material does not itself state, even when they seem to follow logically.\n\
- Do not infer who holds legal, administrative, disciplinary, or ultimate responsibility for any outcome from a person's job title, seniority, approval signature, or authorship. Signing a record, approving or editing a protocol, heading a department, or being named as an author does not establish who is responsible for an outcome. If the material does not explicitly assign such responsibility, do not assign it yourself.\n\
- If a question presupposes a fact, event, or outcome that the material does not contain (for example, an event that never occurred), state plainly that the material does not record it and do not answer as though the premise were true. Rejecting a false premise is not the same as lacking information; do not phrase it as a permission or restricted-document issue.\n\
- When the material contains facts related to the question but does not actually answer it, say what the material does cover and decline the part it does not. Do not bridge the gap with your own reasoning or inference.\n\
- When the material does contain the answer, answer it confidently and completely with the required citations. Do not refuse, hedge, or add disclaimers to a question the material actually supports.\n\
If the provided context does not contain the answer, reply in Vietnamese that you could not find the information in the documents the user has access to, for example: \"Tôi không tìm thấy thông tin này trong các tài liệu bạn có quyền truy cập.\" Do not name a specific document as the reason you cannot answer. Do not state or imply that any document is restricted, that the user lacks permission, or that a document exists but is inaccessible. Do not ask the user to paste or provide file contents. Treat missing information simply as something you do not have.\n\
6. When citing information from a chunk, you MUST append `[chunk:{index}]` at the exact end of the sentence.\n\n",
    );

    prompt.push_str("## Retrieved document chunks\n");
    if chunks.is_empty() {
        prompt.push_str("(none)\n");
    } else {
        for (index, chunk) in chunks.iter().enumerate() {
            let chunk_index = index + 1;
            prompt.push_str(&format!(
                "--- CHUNK INDEX {chunk_index} (Document: {}) ---\n{}\n\n",
                chunk.document_filename, chunk.original_text
            ));
        }
    }

    prompt.push_str("\n## Knowledge graph\n");
    if graph.nodes.is_empty() && graph.edges.is_empty() {
        prompt.push_str("(none)\n");
    } else {
        prompt.push_str("### Nodes\n");
        for node in &graph.nodes {
            prompt.push_str(&format!(
                "- node_id: {}\n  entity_name: {}\n  entity_type: {}\n  description: {}\n\n",
                node.id,
                node.entity_name,
                node.entity_type.as_deref().unwrap_or("(unspecified)"),
                node.description.as_deref().unwrap_or("(none)"),
            ));
        }

        prompt.push_str("### Edges\n");
        for edge in &graph.edges {
            prompt.push_str(&format!(
                "- edge_id: {}\n  source: {}\n  target: {}\n  relationship: {}\n  description: {}\n\n",
                edge.id,
                edge.source_name,
                edge.target_name,
                edge.relationship,
                edge.description.as_deref().unwrap_or("(none)"),
            ));
        }
    }

    prompt.push_str("\n## Citation rules\n");
    prompt.push_str(CITATION_INSTRUCTION);
    prompt.push('\n');

    prompt
}

#[derive(Debug)]
pub enum SessionError {
    Forbidden,
    Database(sqlx::Error),
}

const SESSION_TITLE_MAX_CHARS: usize = 40;

/// First-line session label from the user's opening message.
pub fn truncate_session_title(message: &str) -> String {
    let trimmed = message.trim();
    if trimmed.is_empty() {
        return "New Chat".to_string();
    }

    let char_count = trimmed.chars().count();
    if char_count <= SESSION_TITLE_MAX_CHARS {
        return trimmed.to_string();
    }

    let truncated: String = trimmed.chars().take(SESSION_TITLE_MAX_CHARS).collect();
    format!("{truncated}...")
}

pub async fn ensure_chat_session(
    pool: &PgPool,
    session_id: Uuid,
    workspace_id: Uuid,
    user_id: &str,
    title: &str,
) -> Result<(), SessionError> {
    let owner: Option<(String, Uuid)> = sqlx::query_as(
        r#"
        SELECT user_id, workspace_id
        FROM chat_sessions
        WHERE id = $1
        "#,
    )
    .bind(session_id)
    .fetch_optional(pool)
    .await
    .map_err(SessionError::Database)?;

    if let Some((existing_user, existing_workspace)) = owner {
        if existing_workspace == workspace_id && existing_user == user_id {
            return Ok(());
        }
        return Err(SessionError::Forbidden);
    }

    sqlx::query(
        r#"
        INSERT INTO chat_sessions (id, workspace_id, user_id, title)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (id) DO NOTHING
        "#,
    )
    .bind(session_id)
    .bind(workspace_id)
    .bind(user_id)
    .bind(title)
    .execute(pool)
    .await
    .map_err(SessionError::Database)?;

    let owner: Option<(String, Uuid)> =
        sqlx::query_as("SELECT user_id, workspace_id FROM chat_sessions WHERE id = $1")
            .bind(session_id)
            .fetch_optional(pool)
            .await
            .map_err(SessionError::Database)?;

    match owner {
        Some((existing_user, existing_workspace))
            if existing_workspace == workspace_id && existing_user == user_id =>
        {
            Ok(())
        }
        _ => Err(SessionError::Forbidden),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChatTurnSequence {
    pub user_sequence: i64,
    pub assistant_sequence: i64,
}

/// Atomically reserves the two message positions belonging to one turn and
/// inserts the user message. The session row is locked before reading/updating
/// the counter, so concurrent requests for one session receive disjoint turns.
/// The assistant position is intentionally reserved even if the stream later
/// fails or produces an empty buffer; gaps are acceptable and prevent a later
/// turn from being renumbered behind an already-started request.
pub async fn insert_user_chat_message_and_reserve_turn(
    pool: &PgPool,
    session_id: Uuid,
    content: &str,
) -> Result<ChatTurnSequence, sqlx::Error> {
    let mut transaction = pool.begin().await?;

    let stored_next: i64 = sqlx::query_scalar(
        r#"
        SELECT next_message_sequence
        FROM chat_sessions
        WHERE id = $1
        FOR UPDATE
        "#,
    )
    .bind(session_id)
    .fetch_one(&mut *transaction)
    .await?;

    let max_existing: Option<i64> =
        sqlx::query_scalar("SELECT MAX(message_sequence) FROM chat_messages WHERE session_id = $1")
            .bind(session_id)
            .fetch_one(&mut *transaction)
            .await?;

    let first_available = match max_existing {
        Some(maximum) if maximum == i64::MAX => {
            return Err(sqlx::Error::Protocol(
                "chat message sequence exhausted".to_string(),
            ));
        }
        Some(maximum) => maximum + 1,
        None => 1,
    };
    let user_sequence = stored_next.max(first_available);
    if user_sequence > i64::MAX - 2 {
        return Err(sqlx::Error::Protocol(
            "chat message sequence exhausted".to_string(),
        ));
    }
    let assistant_sequence = user_sequence + 1;
    let next_sequence = user_sequence + 2;

    sqlx::query(
        r#"
        UPDATE chat_sessions
        SET next_message_sequence = $2
        WHERE id = $1
        "#,
    )
    .bind(session_id)
    .bind(next_sequence)
    .execute(&mut *transaction)
    .await?;

    let citations_json = sqlx::types::Json(Vec::<Uuid>::new());
    sqlx::query(
        r#"
        INSERT INTO chat_messages (
            session_id, message_sequence, role, content, citations
        )
        VALUES ($1, $2, 'user', $3, $4)
        "#,
    )
    .bind(session_id)
    .bind(user_sequence)
    .bind(content)
    .bind(citations_json)
    .execute(&mut *transaction)
    .await?;

    transaction.commit().await?;

    Ok(ChatTurnSequence {
        user_sequence,
        assistant_sequence,
    })
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct ChatSessionSummary {
    pub id: Uuid,
    pub title: String,
    #[serde(serialize_with = "crate::utc_timestamp::serialize")]
    pub created_at: NaiveDateTime,
}

#[derive(Debug, Serialize)]
pub struct ChatSessionPage {
    pub sessions: Vec<ChatSessionSummary>,
    pub total: i64,
    pub limit: i64,
    pub offset: i64,
}

pub async fn list_user_chat_sessions(
    pool: &PgPool,
    workspace_id: Uuid,
    user_id: &str,
    limit: i64,
    offset: i64,
) -> Result<ChatSessionPage, sqlx::Error> {
    let total = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::bigint
        FROM chat_sessions
        WHERE workspace_id = $1 AND user_id = $2
        "#,
    )
    .bind(workspace_id)
    .bind(user_id)
    .fetch_one(pool)
    .await?;

    let sessions = sqlx::query_as(
        r#"
        SELECT id, title, created_at
        FROM chat_sessions
        WHERE workspace_id = $1 AND user_id = $2
        ORDER BY created_at DESC, id DESC
        LIMIT $3 OFFSET $4
        "#,
    )
    .bind(workspace_id)
    .bind(user_id)
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await?;

    Ok(ChatSessionPage {
        sessions,
        total,
        limit,
        offset,
    })
}

pub async fn verify_chat_session_owner(
    pool: &PgPool,
    session_id: Uuid,
    workspace_id: Uuid,
    user_id: &str,
) -> Result<bool, SessionError> {
    let owner: Option<String> = sqlx::query_scalar(
        r#"
        SELECT user_id
        FROM chat_sessions
        WHERE id = $1 AND workspace_id = $2
        "#,
    )
    .bind(session_id)
    .bind(workspace_id)
    .fetch_optional(pool)
    .await
    .map_err(SessionError::Database)?;

    match owner {
        None => Ok(false),
        Some(existing) if existing == user_id => Ok(true),
        Some(_) => Err(SessionError::Forbidden),
    }
}

pub async fn delete_chat_session(
    pool: &PgPool,
    session_id: Uuid,
    workspace_id: Uuid,
    user_id: &str,
) -> Result<bool, SessionError> {
    match verify_chat_session_owner(pool, session_id, workspace_id, user_id).await? {
        true => {}
        false => return Ok(false),
    }

    let result = sqlx::query(
        r#"
        DELETE FROM chat_sessions
        WHERE id = $1 AND workspace_id = $2 AND user_id = $3
        "#,
    )
    .bind(session_id)
    .bind(workspace_id)
    .bind(user_id)
    .execute(pool)
    .await
    .map_err(SessionError::Database)?;

    Ok(result.rows_affected() > 0)
}

pub async fn update_chat_session_title(
    pool: &PgPool,
    session_id: Uuid,
    workspace_id: Uuid,
    user_id: &str,
    title: &str,
) -> Result<Option<ChatSessionSummary>, SessionError> {
    match verify_chat_session_owner(pool, session_id, workspace_id, user_id).await? {
        true => {}
        false => return Ok(None),
    }

    sqlx::query_as(
        r#"
        UPDATE chat_sessions
        SET title = $4
        WHERE id = $1 AND workspace_id = $2 AND user_id = $3
        RETURNING id, title, created_at
        "#,
    )
    .bind(session_id)
    .bind(workspace_id)
    .bind(user_id)
    .bind(title)
    .fetch_optional(pool)
    .await
    .map_err(SessionError::Database)
}

pub async fn insert_chat_message(
    pool: &PgPool,
    session_id: Uuid,
    message_sequence: i64,
    role: &str,
    content: &str,
    citations: &[Uuid],
) -> Result<(), sqlx::Error> {
    let citations_json = sqlx::types::Json(citations);

    sqlx::query(
        r#"
        INSERT INTO chat_messages (
            session_id, message_sequence, role, content, citations
        )
        VALUES ($1, $2, $3, $4, $5)
        "#,
    )
    .bind(session_id)
    .bind(message_sequence)
    .bind(role)
    .bind(content)
    .bind(citations_json)
    .execute(pool)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_index_markers_to_uuids() {
        let u1 = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
        let u2 = Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
        let chunk_ids = vec![u1, u2];
        let text = "First fact.[chunk:1] Second fact.[chunk:2] Repeat.[chunk:1]";

        let (resolved, citations) = resolve_chunk_index_citations(text, &chunk_ids);

        assert_eq!(
            resolved,
            format!("First fact.[chunk:{u1}] Second fact.[chunk:{u2}] Repeat.[chunk:{u1}]")
        );
        assert_eq!(citations, vec![u1, u2]);
    }

    #[test]
    fn resolve_leaves_unknown_index_markers_unchanged() {
        let u1 = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
        let chunk_ids = vec![u1];
        let text = "Valid.[chunk:1] Invalid.[chunk:99] Zero.[chunk:0]";

        let (resolved, citations) = resolve_chunk_index_citations(text, &chunk_ids);

        assert_eq!(
            resolved,
            format!("Valid.[chunk:{u1}] Invalid.[chunk:99] Zero.[chunk:0]")
        );
        assert_eq!(citations, vec![u1]);
    }

    #[test]
    fn extract_chunk_citations_deduplicates_uuid_markers() {
        let u1 = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
        let text = format!("One.[chunk:{u1}] Two.[chunk:{u1}]");

        assert_eq!(extract_chunk_citations(&text), vec![u1]);
    }

    #[test]
    fn filter_citations_for_allowed_ids_removes_hidden_markers_and_deduplicates_output() {
        let visible = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
        let hidden = Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
        let text =
            format!("Visible.[chunk:{visible}] Hidden.[chunk:{hidden}] Again.[chunk:{visible}]");

        let (sanitized, citations) =
            filter_citations_for_allowed_ids(&text, &[visible, hidden, visible], &[visible]);

        assert_eq!(
            sanitized,
            format!("Visible.[chunk:{visible}] Hidden. Again.[chunk:{visible}]")
        );
        assert_eq!(citations, vec![visible]);
    }
}
