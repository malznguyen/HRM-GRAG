use axum::{
    Json,
    extract::{Multipart, Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use chrono::NaiveDateTime;
use serde::Serialize;
use sqlx::PgPool;
use tracing::error;
use uuid::Uuid;

use crate::auth::authz::{Authz, Relation, Object};
use crate::ingestion::processor::spawn_document_processing;
use crate::state::AppState;

#[derive(Serialize)]
pub struct UploadedDocumentItem {
    pub document_id: Uuid,
    pub filename: String,
}

#[derive(Serialize)]
pub struct UploadDocumentsResponse {
    pub documents: Vec<UploadedDocumentItem>,
}

#[derive(Serialize, sqlx::FromRow)]
pub struct DocumentResponse {
    pub id: Uuid,
    pub filename: String,
    pub status: String,
    pub processing_stage: String,
    pub created_at: NaiveDateTime,
}

#[derive(Serialize, sqlx::FromRow)]
pub struct PreviewChunkRow {
    pub chunk_index: i32,
    pub original_text: String,
}

#[derive(Serialize)]
pub struct DocumentPreviewResponse {
    pub content: String,
    pub chunks: Vec<PreviewChunkItem>,
}

#[derive(Serialize)]
pub struct PreviewChunkItem {
    pub chunk_index: i32,
    pub text: String,
}

#[derive(Serialize, sqlx::FromRow)]
pub struct ChunkResponse {
    pub id: Uuid,
    pub original_text: String,
}

pub async fn get_document_chunk(
    State(state): State<AppState>,
    authz: Authz,
    Path((workspace_id, chunk_id)): Path<(Uuid, Uuid)>,
) -> impl IntoResponse {
    if let Err(err) = authz.require_relation(Relation::Member, &Object::Workspace(workspace_id)).await {
        return err.into_response();
    }

    match fetch_workspace_chunk(&state.pool, workspace_id, chunk_id).await {
        Ok(Some(chunk)) => Json(chunk).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(err) => {
            error!(
                error = %err,
                user_id = %authz.user_id,
                workspace_id = %workspace_id,
                chunk_id = %chunk_id,
                "Failed to fetch document chunk"
            );
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn fetch_workspace_chunk(
    pool: &PgPool,
    workspace_id: Uuid,
    chunk_id: Uuid,
) -> Result<Option<ChunkResponse>, sqlx::Error> {
    sqlx::query_as(
        r#"
        SELECT id, original_text
        FROM document_chunks
        WHERE id = $1 AND workspace_id = $2
        "#,
    )
    .bind(chunk_id)
    .bind(workspace_id)
    .fetch_optional(pool)
    .await
}

pub async fn get_document_preview(
    State(state): State<AppState>,
    authz: Authz,
    Path((workspace_id, document_id)): Path<(Uuid, Uuid)>,
) -> impl IntoResponse {
    if let Err(err) = authz.require_relation(Relation::Member, &Object::Workspace(workspace_id)).await {
        return err.into_response();
    }

    let document_status: Result<Option<String>, sqlx::Error> = sqlx::query_scalar(
        r#"
        SELECT status
        FROM documents
        WHERE id = $1 AND workspace_id = $2
        "#,
    )
    .bind(document_id)
    .bind(workspace_id)
    .fetch_optional(&state.pool)
    .await;

    match document_status {
        Ok(Some(status)) if status == "COMPLETED" => {}
        Ok(Some(_)) => return StatusCode::CONFLICT.into_response(),
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(err) => {
            error!(
                error = %err,
                user_id = %authz.user_id,
                workspace_id = %workspace_id,
                document_id = %document_id,
                "Failed to verify document before preview"
            );
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    }

    match fetch_document_preview(&state.pool, workspace_id, document_id).await {
        Ok(preview) => Json(preview).into_response(),
        Err(err) => {
            error!(
                error = %err,
                user_id = %authz.user_id,
                workspace_id = %workspace_id,
                document_id = %document_id,
                "Failed to fetch document preview"
            );
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

pub async fn list_documents(
    State(state): State<AppState>,
    authz: Authz,
    Path(workspace_id): Path<Uuid>,
) -> impl IntoResponse {
    if let Err(err) = authz.require_relation(Relation::Member, &Object::Workspace(workspace_id)).await {
        return err.into_response();
    }

    match fetch_workspace_documents(&state.pool, workspace_id).await {
        Ok(documents) => Json(documents).into_response(),
        Err(err) => {
            error!(
                error = %err,
                user_id = %authz.user_id,
                workspace_id = %workspace_id,
                "Failed to list documents"
            );
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

pub async fn delete_document(
    State(state): State<AppState>,
    authz: Authz,
    Path((workspace_id, document_id)): Path<(Uuid, Uuid)>,
) -> impl IntoResponse {
    if let Err(err) = authz.require_relation(Relation::Admin, &Object::Workspace(workspace_id)).await {
        return err.into_response();
    }

    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(err) => {
            error!(
                error = %err,
                user_id = %authz.user_id,
                workspace_id = %workspace_id,
                document_id = %document_id,
                "Failed to start document delete transaction"
            );
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let exists: Result<Option<Uuid>, sqlx::Error> = sqlx::query_scalar(
        r#"
        SELECT id
        FROM documents
        WHERE id = $1 AND workspace_id = $2
        "#,
    )
    .bind(document_id)
    .bind(workspace_id)
    .fetch_optional(&mut *tx)
    .await;

    match exists {
        Ok(Some(_)) => {}
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(err) => {
            error!(
                error = %err,
                user_id = %authz.user_id,
                workspace_id = %workspace_id,
                document_id = %document_id,
                "Failed to verify document before delete"
            );
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    }

    let result = async {
        sqlx::query(
            r#"
            DELETE FROM graph_edge_sources
            WHERE workspace_id = $1 AND document_id = $2
            "#,
        )
        .bind(workspace_id)
        .bind(document_id)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            DELETE FROM graph_node_sources
            WHERE workspace_id = $1 AND document_id = $2
            "#,
        )
        .bind(workspace_id)
        .bind(document_id)
        .execute(&mut *tx)
        .await?;

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
        .execute(&mut *tx)
        .await?;

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
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            DELETE FROM documents
            WHERE id = $1 AND workspace_id = $2
            "#,
        )
        .bind(document_id)
        .bind(workspace_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await
    }
    .await;

    match result {
        Ok(_) => {
            let file_path = state
                .upload_dir
                .join(workspace_id.to_string())
                .join(format!("{document_id}.pdf"));
            let _ = tokio::fs::remove_file(file_path).await;
            StatusCode::NO_CONTENT.into_response()
        }
        Err(err) => {
            error!(
                error = %err,
                user_id = %authz.user_id,
                workspace_id = %workspace_id,
                document_id = %document_id,
                "Failed to delete document and graph provenance"
            );
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

pub async fn retry_document_ingestion(
    State(state): State<AppState>,
    authz: Authz,
    Path((workspace_id, document_id)): Path<(Uuid, Uuid)>,
) -> impl IntoResponse {
    if let Err(err) = authz.require_relation(Relation::Admin, &Object::Workspace(workspace_id)).await {
        return err.into_response();
    }

    // Only documents stuck in FAILED can be re-queued; anything else would
    // race with an in-flight ingestion.
    let document_status: Result<Option<String>, sqlx::Error> = sqlx::query_scalar(
        r#"
        SELECT status
        FROM documents
        WHERE id = $1 AND workspace_id = $2
        "#,
    )
    .bind(document_id)
    .bind(workspace_id)
    .fetch_optional(&state.pool)
    .await;

    match document_status {
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Ok(Some(status)) if status == "FAILED" => {}
        Ok(Some(_)) => {
            return (
                StatusCode::CONFLICT,
                "Document is not in a failed state",
            )
                .into_response();
        }
        Err(err) => {
            error!(
                error = %err,
                user_id = %authz.user_id,
                workspace_id = %workspace_id,
                document_id = %document_id,
                "Failed to verify document before retry"
            );
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    }

    // The original PDF is kept on disk until the document is deleted, so a
    // FAILED document can be reprocessed from its existing upload.
    let file_path = state
        .upload_dir
        .join(workspace_id.to_string())
        .join(format!("{document_id}.pdf"));

    match tokio::fs::metadata(&file_path).await {
        Ok(_) => {}
        Err(_) => {
            return (
                StatusCode::GONE,
                "Document source file is missing",
            )
                .into_response();
        }
    }

    if let Err(err) = sqlx::query(
        r#"
        UPDATE documents
        SET status = 'PROCESSING', processing_stage = 'QUEUED'
        WHERE id = $1 AND workspace_id = $2
        "#,
    )
    .bind(document_id)
    .bind(workspace_id)
    .execute(&state.pool)
    .await
    {
        error!(
            error = %err,
            user_id = %authz.user_id,
            workspace_id = %workspace_id,
            document_id = %document_id,
            "Failed to reset document status before retry"
        );
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    // Ingestion upserts chunks/graph on conflict, so any partial data from
    // the failed run is overwritten rather than duplicated.
    spawn_document_processing(
        state.pool.clone(),
        workspace_id,
        document_id,
        file_path,
        state.ingestion_limiter.clone(),
    );

    StatusCode::ACCEPTED.into_response()
}

async fn fetch_workspace_documents(
    pool: &PgPool,
    workspace_id: Uuid,
) -> Result<Vec<DocumentResponse>, sqlx::Error> {
    sqlx::query_as(
        r#"
        SELECT id, filename, status, processing_stage, created_at
        FROM documents
        WHERE workspace_id = $1
        ORDER BY created_at DESC
        "#,
    )
    .bind(workspace_id)
    .fetch_all(pool)
    .await
}

async fn fetch_document_preview(
    pool: &PgPool,
    workspace_id: Uuid,
    document_id: Uuid,
) -> Result<DocumentPreviewResponse, sqlx::Error> {
    let rows: Vec<PreviewChunkRow> = sqlx::query_as(
        r#"
        SELECT chunk_index, original_text
        FROM document_chunks
        WHERE workspace_id = $1 AND document_id = $2
        ORDER BY chunk_index ASC
        "#,
    )
    .bind(workspace_id)
    .bind(document_id)
    .fetch_all(pool)
    .await?;

    let chunks: Vec<PreviewChunkItem> = rows
        .into_iter()
        .map(|row| PreviewChunkItem {
            chunk_index: row.chunk_index,
            text: row.original_text,
        })
        .collect();

    let content = chunks
        .iter()
        .map(|chunk| chunk.text.as_str())
        .collect::<Vec<_>>()
        .join("\n\n");

    Ok(DocumentPreviewResponse { content, chunks })
}

pub async fn upload_document(
    State(state): State<AppState>,
    authz: Authz,
    Path(workspace_id): Path<Uuid>,
    mut multipart: Multipart,
) -> impl IntoResponse {
    if let Err(err) = authz.require_relation(Relation::Admin, &Object::Workspace(workspace_id)).await {
        return err.into_response();
    }

    let workspace_dir = state.upload_dir.join(workspace_id.to_string());
    if tokio::fs::create_dir_all(&workspace_dir).await.is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    let mut uploaded: Vec<UploadedDocumentItem> = Vec::new();

    while let Ok(Some(field)) = multipart.next_field().await {
        if field.name() != Some("file") {
            continue;
        }

        let filename = field
            .file_name()
            .map(sanitize_filename)
            .unwrap_or_else(|| "upload.pdf".to_string());

        let pdf_bytes = match field.bytes().await {
            Ok(bytes) => bytes.to_vec(),
            Err(_) => continue,
        };

        if pdf_bytes.is_empty() || !is_pdf_upload(&filename, &pdf_bytes) {
            continue;
        }

        let document_id: Uuid = match sqlx::query_scalar(
            r#"
            INSERT INTO documents (workspace_id, owner_id, filename, status, processing_stage)
            VALUES ($1, $2, $3, 'PROCESSING', 'QUEUED')
            RETURNING id
            "#,
        )
        .bind(workspace_id)
        .bind(&authz.user_id)
        .bind(&filename)
        .fetch_one(&state.pool)
        .await
        {
            Ok(id) => id,
            Err(err) => {
                error!(
                    error = %err,
                    user_id = %authz.user_id,
                    workspace_id = %workspace_id,
                    filename = %filename,
                    "Failed to insert document row during batch upload"
                );
                continue;
            }
        };

        let file_path = workspace_dir.join(format!("{document_id}.pdf"));
        if tokio::fs::write(&file_path, &pdf_bytes).await.is_err() {
            let _ = mark_upload_failed(&state.pool, workspace_id, document_id).await;
            continue;
        }

        spawn_document_processing(
            state.pool.clone(),
            workspace_id,
            document_id,
            file_path,
            state.ingestion_limiter.clone(),
        );

        uploaded.push(UploadedDocumentItem {
            document_id,
            filename,
        });
    }

    if uploaded.is_empty() {
        return StatusCode::BAD_REQUEST.into_response();
    }

    (
        StatusCode::ACCEPTED,
        Json(UploadDocumentsResponse {
            documents: uploaded,
        }),
    )
        .into_response()
}

fn sanitize_filename(name: &str) -> String {
    let base = std::path::Path::new(name)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("upload.pdf");
    let trimmed: String = base
        .chars()
        .filter(|c| !matches!(c, '/' | '\\' | '\0'))
        .take(255)
        .collect();
    if trimmed.is_empty() {
        "upload.pdf".to_string()
    } else {
        trimmed
    }
}

fn is_pdf_upload(filename: &str, data: &[u8]) -> bool {
    let magic_ok = data.starts_with(b"%PDF");
    let ext_ok = filename.to_ascii_lowercase().ends_with(".pdf");
    magic_ok || ext_ok
}

async fn mark_upload_failed(
    pool: &sqlx::PgPool,
    workspace_id: Uuid,
    document_id: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE documents
        SET status = 'FAILED'
        WHERE id = $1 AND workspace_id = $2
        "#,
    )
    .bind(document_id)
    .bind(workspace_id)
    .execute(pool)
    .await?;
    Ok(())
}
