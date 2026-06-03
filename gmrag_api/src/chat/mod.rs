pub mod deepseek;
pub mod retrieval;

use sqlx::PgPool;
use uuid::Uuid;

use crate::ingestion::embedding::{EmbedError, embed_text, format_pgvector};

use self::deepseek::{ChatMessage, DeepseekStreamError, stream_chat_completion};
use self::retrieval::{
    GraphContext, RetrievedChunk, fetch_chat_history, fetch_graph_context, fetch_similar_chunks,
};

pub const CITATION_INSTRUCTION: &str = "You MUST cite your sources. When using information from a document chunk, append [chunk:<chunk_id>] to the end of the sentence, replacing <chunk_id> with the chunk's UUID.";

#[derive(Debug)]
pub enum ChatPipelineError {
    Embed(EmbedError),
    Generation(DeepseekStreamError),
    Database(sqlx::Error),
}

impl std::fmt::Display for ChatPipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChatPipelineError::Embed(e) => write!(f, "{e}"),
            ChatPipelineError::Generation(e) => write!(f, "{e}"),
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

pub struct ChatContext {
    pub system_prompt: String,
    pub messages: Vec<ChatMessage>,
}

pub async fn build_chat_context(
    pool: &PgPool,
    client: &reqwest::Client,
    workspace_id: Uuid,
    session_id: Uuid,
    user_message: &str,
) -> Result<ChatContext, ChatPipelineError> {
    tracing::info!(
        %workspace_id,
        %session_id,
        query_len = user_message.len(),
        "RAG pipeline: embedding user query via Ollama"
    );

    let embedding = embed_text(client, user_message)
        .await
        .map_err(ChatPipelineError::Embed)?;
    let embedding_literal = format_pgvector(&embedding);

    tracing::info!(
        %workspace_id,
        embedding_dims = embedding.len(),
        "RAG pipeline: query embedding complete"
    );

    let chunks = fetch_similar_chunks(pool, workspace_id, &embedding_literal).await?;
    log_retrieved_chunks(workspace_id, &chunks);

    let graph = fetch_graph_context(pool, workspace_id, &embedding_literal, user_message).await?;
    log_graph_context(workspace_id, &graph);

    let history = fetch_chat_history(pool, session_id, workspace_id).await?;
    tracing::info!(
        %workspace_id,
        %session_id,
        history_messages = history.len(),
        roles = ?history.iter().map(|m| m.role.as_str()).collect::<Vec<_>>(),
        "RAG pipeline: loaded chat history"
    );

    let system_prompt = assemble_system_prompt(&chunks, &graph);
    tracing::info!(
        %workspace_id,
        system_prompt_chars = system_prompt.len(),
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

    stream_chat_completion(client, &context.system_prompt, &context.messages)
        .await
        .map_err(ChatPipelineError::Generation)
}

fn log_retrieved_chunks(workspace_id: Uuid, chunks: &[RetrievedChunk]) {
    if chunks.is_empty() {
        tracing::info!(%workspace_id, "RAG pipeline: no document chunks retrieved");
        return;
    }

    for chunk in chunks {
        let preview: String = chunk.original_text.chars().take(120).collect();
        tracing::info!(
            %workspace_id,
            chunk_id = %chunk.id,
            preview = %preview,
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

    for node in &graph.nodes {
        tracing::info!(
            %workspace_id,
            node_id = %node.id,
            entity_name = %node.entity_name,
            entity_type = ?node.entity_type,
            description = ?node.description,
            "RAG pipeline: retrieved graph node"
        );
    }

    for edge in &graph.edges {
        tracing::info!(
            %workspace_id,
            edge_id = %edge.id,
            source = %edge.source_name,
            target = %edge.target_name,
            relationship = %edge.relationship,
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
        "You are a knowledgeable assistant for a workspace document and knowledge-graph corpus. \
Answer using only the retrieved context below when possible. Be concise and accurate.\n\n",
    );

    prompt.push_str("## Retrieved document chunks\n");
    if chunks.is_empty() {
        prompt.push_str("(none)\n");
    } else {
        for chunk in chunks {
            prompt.push_str(&format!(
                "- chunk_id: {}\n  original_text: {}\n\n",
                chunk.id, chunk.original_text
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

pub async fn ensure_chat_session(
    pool: &PgPool,
    session_id: Uuid,
    workspace_id: Uuid,
    user_id: &str,
) -> Result<(), SessionError> {
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

    if let Some(existing_user) = owner {
        if existing_user == user_id {
            return Ok(());
        }
        return Err(SessionError::Forbidden);
    }

    sqlx::query(
        r#"
        INSERT INTO chat_sessions (id, workspace_id, user_id, title)
        VALUES ($1, $2, $3, 'New Chat')
        "#,
    )
    .bind(session_id)
    .bind(workspace_id)
    .bind(user_id)
    .execute(pool)
    .await
    .map_err(SessionError::Database)?;

    Ok(())
}

pub async fn insert_chat_message(
    pool: &PgPool,
    session_id: Uuid,
    role: &str,
    content: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO chat_messages (session_id, role, content)
        VALUES ($1, $2, $3)
        "#,
    )
    .bind(session_id)
    .bind(role)
    .bind(content)
    .execute(pool)
    .await?;
    Ok(())
}
