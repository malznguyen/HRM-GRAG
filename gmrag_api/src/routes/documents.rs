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

use crate::auth::extractor::AuthUser;
use crate::auth::rbac::{require_workspace_admin, require_workspace_member};
use crate::ingestion::processor::spawn_document_processing;
use crate::state::AppState;

#[derive(Serialize)]
pub struct UploadDocumentResponse {
    pub document_id: Uuid,
}

#[derive(Serialize, sqlx::FromRow)]
pub struct DocumentResponse {
    pub id: Uuid,
    pub filename: String,
    pub status: String,
    pub created_at: NaiveDateTime,
}

#[derive(Serialize, sqlx::FromRow)]
pub struct ChunkResponse {
    pub id: Uuid,
    pub original_text: String,
}

pub async fn get_document_chunk(
    State(state): State<AppState>,
    auth: AuthUser,
    Path((workspace_id, chunk_id)): Path<(Uuid, Uuid)>,
) -> impl IntoResponse {
    if let Err(status) = require_workspace_member(&state.pool, workspace_id, &auth.user_id).await {
        return status.into_response();
    }

    match fetch_workspace_chunk(&state.pool, workspace_id, chunk_id).await {
        Ok(Some(chunk)) => Json(chunk).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(err) => {
            error!(
                error = %err,
                user_id = %auth.user_id,
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

pub async fn list_documents(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(workspace_id): Path<Uuid>,
) -> impl IntoResponse {
    if let Err(status) = require_workspace_member(&state.pool, workspace_id, &auth.user_id).await {
        return status.into_response();
    }

    match fetch_workspace_documents(&state.pool, workspace_id).await {
        Ok(documents) => Json(documents).into_response(),
        Err(err) => {
            error!(
                error = %err,
                user_id = %auth.user_id,
                workspace_id = %workspace_id,
                "Failed to list documents"
            );
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

pub async fn delete_document(
    State(state): State<AppState>,
    auth: AuthUser,
    Path((workspace_id, document_id)): Path<(Uuid, Uuid)>,
) -> impl IntoResponse {
    if let Err(status) = require_workspace_admin(&state.pool, workspace_id, &auth.user_id).await {
        return status.into_response();
    }

    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(err) => {
            error!(
                error = %err,
                user_id = %auth.user_id,
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
                user_id = %auth.user_id,
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
                user_id = %auth.user_id,
                workspace_id = %workspace_id,
                document_id = %document_id,
                "Failed to delete document and graph provenance"
            );
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn fetch_workspace_documents(
    pool: &PgPool,
    workspace_id: Uuid,
) -> Result<Vec<DocumentResponse>, sqlx::Error> {
    sqlx::query_as(
        r#"
        SELECT id, filename, status, created_at
        FROM documents
        WHERE workspace_id = $1
        ORDER BY created_at DESC
        "#,
    )
    .bind(workspace_id)
    .fetch_all(pool)
    .await
}

pub async fn upload_document(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(workspace_id): Path<Uuid>,
    mut multipart: Multipart,
) -> impl IntoResponse {
    if let Err(status) = require_workspace_admin(&state.pool, workspace_id, &auth.user_id).await {
        return status.into_response();
    }

    let (filename, pdf_bytes) = match read_pdf_field(&mut multipart).await {
        Ok(v) => v,
        Err(status) => return status.into_response(),
    };

    let document_id: Uuid = match sqlx::query_scalar(
        r#"
        INSERT INTO documents (workspace_id, filename, status)
        VALUES ($1, $2, 'PROCESSING')
        RETURNING id
        "#,
    )
    .bind(workspace_id)
    .bind(&filename)
    .fetch_one(&state.pool)
    .await
    {
        Ok(id) => id,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let workspace_dir = state.upload_dir.join(workspace_id.to_string());
    if let Err(_) = tokio::fs::create_dir_all(&workspace_dir).await {
        let _ = mark_upload_failed(&state.pool, workspace_id, document_id).await;
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    let file_path = workspace_dir.join(format!("{document_id}.pdf"));
    if let Err(_) = tokio::fs::write(&file_path, &pdf_bytes).await {
        let _ = mark_upload_failed(&state.pool, workspace_id, document_id).await;
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    spawn_document_processing(
        state.pool.clone(),
        workspace_id,
        document_id,
        file_path,
        state.ingestion_limiter.clone(),
    );

    (
        StatusCode::ACCEPTED,
        Json(UploadDocumentResponse { document_id }),
    )
        .into_response()
}

async fn read_pdf_field(multipart: &mut Multipart) -> Result<(String, Vec<u8>), StatusCode> {
    let mut filename: Option<String> = None;
    let mut bytes: Option<Vec<u8>> = None;

    while let Ok(Some(field)) = multipart.next_field().await {
        if field.name() != Some("file") {
            continue;
        }

        filename = field
            .file_name()
            .map(sanitize_filename)
            .or_else(|| Some("upload.pdf".to_string()));

        let data = field
            .bytes()
            .await
            .map_err(|_| StatusCode::BAD_REQUEST)?
            .to_vec();

        if data.is_empty() {
            return Err(StatusCode::BAD_REQUEST);
        }

        if !is_pdf_upload(&filename.as_deref().unwrap_or("upload.pdf"), &data) {
            return Err(StatusCode::BAD_REQUEST);
        }

        bytes = Some(data);
        break;
    }

    match (filename, bytes) {
        (Some(name), Some(data)) => Ok((name, data)),
        _ => Err(StatusCode::BAD_REQUEST),
    }
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
