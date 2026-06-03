use std::collections::{BTreeMap, HashMap};

use reqwest::Client;
use serde::Deserialize;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

pub const GRAPH_EXTRACTION_SYSTEM_PROMPT: &str = "Extract Knowledge Graph nodes and edges from this text. Return ONLY a valid JSON array of objects. Each object must have: `type` ('node' or 'edge'), `name`/`relationship`, `description`, and for edges, `source` and `target`.";

const DEFAULT_DEEPSEEK_URL: &str = "https://api.deepseek.com/chat/completions";
const DEFAULT_DEEPSEEK_MODEL: &str = "deepseek-v4-flash";

pub fn deepseek_chat_url() -> String {
    std::env::var("DEEPSEEK_API_URL").unwrap_or_else(|_| DEFAULT_DEEPSEEK_URL.to_string())
}

pub fn deepseek_model() -> String {
    std::env::var("DEEPSEEK_MODEL").unwrap_or_else(|_| DEFAULT_DEEPSEEK_MODEL.to_string())
}

pub async fn extract_graph_elements(
    client: &Client,
    chunk_text: &str,
) -> Result<Vec<GraphElement>, GraphError> {
    let api_key = std::env::var("DEEPSEEK_API_KEY").map_err(|_| GraphError::MissingApiKey)?;

    let body = DeepseekChatRequest {
        model: deepseek_model(),
        messages: vec![
            DeepseekMessage {
                role: "system",
                content: GRAPH_EXTRACTION_SYSTEM_PROMPT,
            },
            DeepseekMessage {
                role: "user",
                content: chunk_text,
            },
        ],
        temperature: 0.0,
    };

    let response = client
        .post(deepseek_chat_url())
        .bearer_auth(api_key)
        .json(&body)
        .send()
        .await
        .map_err(GraphError::Http)?
        .error_for_status()
        .map_err(GraphError::Http)?;

    let payload: DeepseekChatResponse = response.json().await.map_err(GraphError::Http)?;
    let content = payload
        .choices
        .into_iter()
        .next()
        .map(|choice| choice.message.content)
        .filter(|content| !content.trim().is_empty())
        .ok_or(GraphError::EmptyResponse)?;

    parse_graph_elements(&content)
}

pub async fn bulk_upsert_graph(
    tx: &mut Transaction<'_, Postgres>,
    workspace_id: Uuid,
    document_id: Uuid,
    batch: &GraphWriteBatch,
) -> Result<(), GraphError> {
    delete_untracked_graph_items(tx, workspace_id).await?;

    let node_ids = bulk_upsert_graph_nodes(tx, workspace_id, &batch.nodes).await?;
    insert_graph_node_sources(tx, workspace_id, document_id, node_ids.values()).await?;

    let edge_ids = bulk_upsert_graph_edges(tx, workspace_id, &node_ids, &batch.edges).await?;
    insert_graph_edge_sources(tx, workspace_id, document_id, edge_ids.iter()).await?;

    Ok(())
}

async fn delete_untracked_graph_items(
    tx: &mut Transaction<'_, Postgres>,
    workspace_id: Uuid,
) -> Result<(), GraphError> {
    sqlx::query(
        r#"
        DELETE FROM graph_edges edge
        WHERE edge.workspace_id = $1
          AND NOT EXISTS (
            SELECT 1
            FROM graph_edge_sources source
            WHERE source.graph_edge_id = edge.id
          )
        "#,
    )
    .bind(workspace_id)
    .execute(&mut **tx)
    .await
    .map_err(GraphError::Database)?;

    sqlx::query(
        r#"
        DELETE FROM graph_nodes node
        WHERE node.workspace_id = $1
          AND NOT EXISTS (
            SELECT 1
            FROM graph_node_sources source
            WHERE source.graph_node_id = node.id
          )
          AND NOT EXISTS (
            SELECT 1
            FROM graph_edges edge
            WHERE edge.source_node_id = node.id
               OR edge.target_node_id = node.id
          )
        "#,
    )
    .bind(workspace_id)
    .execute(&mut **tx)
    .await
    .map_err(GraphError::Database)?;

    Ok(())
}

#[derive(Debug, Default)]
pub struct GraphWriteBatch {
    nodes: Vec<NodeInput>,
    edges: Vec<EdgeInput>,
}

impl GraphWriteBatch {
    pub fn from_extractions(extractions: &[(i32, Vec<GraphElement>)]) -> Self {
        let mut nodes_by_key = BTreeMap::<String, NodeInput>::new();
        let mut edges_by_key = BTreeMap::<(String, String, String), EdgeInput>::new();

        for (_, elements) in extractions {
            for element in elements {
                match element {
                    GraphElement::Node {
                        name,
                        entity_type,
                        description,
                    } => {
                        merge_node(
                            &mut nodes_by_key,
                            name,
                            entity_type.as_deref(),
                            description.as_deref(),
                        );
                    }
                    GraphElement::Edge {
                        relationship,
                        source,
                        target,
                        description,
                    } => {
                        let source_key = normalize_key(source);
                        let target_key = normalize_key(target);
                        let relationship_key = normalize_key(relationship);

                        if source_key.is_empty()
                            || target_key.is_empty()
                            || relationship_key.is_empty()
                        {
                            continue;
                        }

                        merge_node(&mut nodes_by_key, source, None, None);
                        merge_node(&mut nodes_by_key, target, None, None);

                        edges_by_key
                            .entry((source_key, target_key, relationship_key))
                            .or_insert_with(|| EdgeInput {
                                source: source.trim().to_string(),
                                target: target.trim().to_string(),
                                relationship: relationship.trim().to_string(),
                                description: normalize_optional(description.as_deref()),
                            });
                    }
                }
            }
        }

        Self {
            nodes: nodes_by_key.into_values().collect(),
            edges: edges_by_key.into_values().collect(),
        }
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }
}

#[derive(Debug, Clone)]
struct NodeInput {
    name: String,
    entity_type: Option<String>,
    description: Option<String>,
}

#[derive(Debug, Clone)]
struct EdgeInput {
    source: String,
    target: String,
    relationship: String,
    description: Option<String>,
}

async fn bulk_upsert_graph_nodes(
    tx: &mut Transaction<'_, Postgres>,
    workspace_id: Uuid,
    nodes: &[NodeInput],
) -> Result<HashMap<String, Uuid>, GraphError> {
    if nodes.is_empty() {
        return Ok(HashMap::new());
    }

    let names: Vec<String> = nodes.iter().map(|node| node.name.clone()).collect();
    let entity_types: Vec<Option<String>> =
        nodes.iter().map(|node| node.entity_type.clone()).collect();
    let descriptions: Vec<Option<String>> =
        nodes.iter().map(|node| node.description.clone()).collect();

    let rows = sqlx::query_as::<_, (Uuid, String)>(
        r#"
        INSERT INTO graph_nodes (workspace_id, entity_name, entity_type, description)
        SELECT $1, node.entity_name, node.entity_type, node.description
        FROM UNNEST($2::text[], $3::text[], $4::text[])
            AS node(entity_name, entity_type, description)
        ON CONFLICT (workspace_id, lower(entity_name))
        DO UPDATE SET
            entity_type = COALESCE(EXCLUDED.entity_type, graph_nodes.entity_type),
            description = COALESCE(EXCLUDED.description, graph_nodes.description)
        RETURNING id, lower(entity_name)
        "#,
    )
    .bind(workspace_id)
    .bind(&names)
    .bind(&entity_types)
    .bind(&descriptions)
    .fetch_all(&mut **tx)
    .await
    .map_err(GraphError::Database)?;

    Ok(rows.into_iter().map(|(id, key)| (key, id)).collect())
}

async fn bulk_upsert_graph_edges(
    tx: &mut Transaction<'_, Postgres>,
    workspace_id: Uuid,
    node_ids: &HashMap<String, Uuid>,
    edges: &[EdgeInput],
) -> Result<Vec<Uuid>, GraphError> {
    if edges.is_empty() {
        return Ok(Vec::new());
    }

    let mut source_ids = Vec::with_capacity(edges.len());
    let mut target_ids = Vec::with_capacity(edges.len());
    let mut relationships = Vec::with_capacity(edges.len());
    let mut descriptions = Vec::with_capacity(edges.len());

    for edge in edges {
        let Some(source_id) = node_ids.get(&normalize_key(&edge.source)) else {
            continue;
        };
        let Some(target_id) = node_ids.get(&normalize_key(&edge.target)) else {
            continue;
        };

        source_ids.push(*source_id);
        target_ids.push(*target_id);
        relationships.push(edge.relationship.clone());
        descriptions.push(edge.description.clone());
    }

    if source_ids.is_empty() {
        return Ok(Vec::new());
    }

    let rows = sqlx::query_as::<_, (Uuid,)>(
        r#"
        INSERT INTO graph_edges (workspace_id, source_node_id, target_node_id, relationship, description)
        SELECT $1, edge.source_id, edge.target_id, edge.relationship, edge.description
        FROM UNNEST($2::uuid[], $3::uuid[], $4::text[], $5::text[])
            AS edge(source_id, target_id, relationship, description)
        ON CONFLICT (workspace_id, source_node_id, target_node_id, lower(relationship))
        DO UPDATE SET
            description = COALESCE(EXCLUDED.description, graph_edges.description)
        RETURNING id
        "#,
    )
    .bind(workspace_id)
    .bind(&source_ids)
    .bind(&target_ids)
    .bind(&relationships)
    .bind(&descriptions)
    .fetch_all(&mut **tx)
    .await
    .map_err(GraphError::Database)?;

    Ok(rows.into_iter().map(|(id,)| id).collect())
}

async fn insert_graph_node_sources<'a>(
    tx: &mut Transaction<'_, Postgres>,
    workspace_id: Uuid,
    document_id: Uuid,
    node_ids: impl Iterator<Item = &'a Uuid>,
) -> Result<(), GraphError> {
    let ids: Vec<Uuid> = node_ids.copied().collect();
    if ids.is_empty() {
        return Ok(());
    }

    sqlx::query(
        r#"
        INSERT INTO graph_node_sources (graph_node_id, document_id, workspace_id)
        SELECT node_id, $1, $2
        FROM UNNEST($3::uuid[]) AS source(node_id)
        ON CONFLICT (graph_node_id, document_id) DO NOTHING
        "#,
    )
    .bind(document_id)
    .bind(workspace_id)
    .bind(&ids)
    .execute(&mut **tx)
    .await
    .map_err(GraphError::Database)?;

    Ok(())
}

async fn insert_graph_edge_sources<'a>(
    tx: &mut Transaction<'_, Postgres>,
    workspace_id: Uuid,
    document_id: Uuid,
    edge_ids: impl Iterator<Item = &'a Uuid>,
) -> Result<(), GraphError> {
    let ids: Vec<Uuid> = edge_ids.copied().collect();
    if ids.is_empty() {
        return Ok(());
    }

    sqlx::query(
        r#"
        INSERT INTO graph_edge_sources (graph_edge_id, document_id, workspace_id)
        SELECT edge_id, $1, $2
        FROM UNNEST($3::uuid[]) AS source(edge_id)
        ON CONFLICT (graph_edge_id, document_id) DO NOTHING
        "#,
    )
    .bind(document_id)
    .bind(workspace_id)
    .bind(&ids)
    .execute(&mut **tx)
    .await
    .map_err(GraphError::Database)?;

    Ok(())
}

fn merge_node(
    nodes_by_key: &mut BTreeMap<String, NodeInput>,
    name: &str,
    entity_type: Option<&str>,
    description: Option<&str>,
) {
    let key = normalize_key(name);
    if key.is_empty() {
        return;
    }

    let input = nodes_by_key.entry(key).or_insert_with(|| NodeInput {
        name: name.trim().to_string(),
        entity_type: None,
        description: None,
    });

    if input.entity_type.is_none() {
        input.entity_type = normalize_optional(entity_type);
    }
    if input.description.is_none() {
        input.description = normalize_optional(description);
    }
}

fn normalize_key(value: &str) -> String {
    value.trim().to_lowercase()
}

fn normalize_optional(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn parse_graph_elements(raw: &str) -> Result<Vec<GraphElement>, GraphError> {
    let json_text = extract_json_array(raw);
    let raw_elements: Vec<RawGraphElement> =
        serde_json::from_str(&json_text).map_err(GraphError::InvalidJson)?;

    Ok(raw_elements
        .into_iter()
        .filter_map(normalize_graph_element)
        .collect())
}

fn extract_json_array(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.starts_with('[') && trimmed.ends_with(']') {
        return trimmed.to_string();
    }

    if let Some(start) = trimmed.find('[') {
        if let Some(end) = trimmed.rfind(']') {
            if end > start {
                return trimmed[start..=end].to_string();
            }
        }
    }

    trimmed.to_string()
}

fn normalize_graph_element(raw: RawGraphElement) -> Option<GraphElement> {
    match raw.element_type.as_deref() {
        Some("node") => {
            let name = raw.name?.trim().to_string();
            if name.is_empty() {
                return None;
            }
            Some(GraphElement::Node {
                name,
                entity_type: raw.entity_type,
                description: raw.description,
            })
        }
        Some("edge") => {
            let relationship = raw
                .relationship
                .or(raw.name)
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())?;
            let source = raw.source?.trim().to_string();
            let target = raw.target?.trim().to_string();
            if source.is_empty() || target.is_empty() {
                return None;
            }
            Some(GraphElement::Edge {
                relationship,
                source,
                target,
                description: raw.description,
            })
        }
        _ => None,
    }
}

#[derive(Debug, Clone)]
pub enum GraphElement {
    Node {
        name: String,
        entity_type: Option<String>,
        description: Option<String>,
    },
    Edge {
        relationship: String,
        source: String,
        target: String,
        description: Option<String>,
    },
}

#[derive(Debug, Deserialize)]
struct RawGraphElement {
    #[serde(rename = "type")]
    element_type: Option<String>,
    name: Option<String>,
    relationship: Option<String>,
    description: Option<String>,
    #[serde(rename = "entity_type")]
    entity_type: Option<String>,
    source: Option<String>,
    target: Option<String>,
}

#[derive(Debug, serde::Serialize)]
struct DeepseekChatRequest<'a> {
    model: String,
    messages: Vec<DeepseekMessage<'a>>,
    temperature: f32,
}

#[derive(Debug, serde::Serialize)]
struct DeepseekMessage<'a> {
    role: &'a str,
    content: &'a str,
}

#[derive(Debug, Deserialize)]
struct DeepseekChatResponse {
    choices: Vec<DeepseekChoice>,
}

#[derive(Debug, Deserialize)]
struct DeepseekChoice {
    message: DeepseekAssistantMessage,
}

#[derive(Debug, Deserialize)]
struct DeepseekAssistantMessage {
    content: String,
}

#[derive(Debug)]
pub enum GraphError {
    MissingApiKey,
    Http(reqwest::Error),
    EmptyResponse,
    InvalidJson(serde_json::Error),
    Database(sqlx::Error),
}

impl std::fmt::Display for GraphError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GraphError::MissingApiKey => write!(f, "DEEPSEEK_API_KEY is not set"),
            GraphError::Http(err) => write!(f, "deepseek request failed: {err}"),
            GraphError::EmptyResponse => write!(f, "deepseek returned no content"),
            GraphError::InvalidJson(err) => write!(f, "invalid graph JSON: {err}"),
            GraphError::Database(err) => write!(f, "graph database error: {err}"),
        }
    }
}

impl std::error::Error for GraphError {}
