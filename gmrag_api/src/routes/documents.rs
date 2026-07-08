use axum::{
    Json,
    extract::{Multipart, Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use chrono::NaiveDateTime;
use serde::Serialize;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use tracing::error;
use uuid::Uuid;

use crate::auth::authz::{ApiError, Authz, Object, Relation};
use crate::auth::document_acl::{
    DocumentAccessMode, DocumentAclRow, collect_viewable_document_ids,
    ensure_document_workspace_relation, can_user_view_document, remove_document_workspace_relation,
};
use crate::ingestion::processor::spawn_document_processing;
use crate::state::AppState;
use crate::storage::build_original_document_object_key;

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

#[derive(sqlx::FromRow)]
struct DocumentListRow {
    id: Uuid,
    filename: String,
    status: String,
    processing_stage: String,
    created_at: NaiveDateTime,
    access_mode: String,
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

#[derive(sqlx::FromRow)]
struct ChunkWithDocumentAclRow {
    id: Uuid,
    original_text: String,
    document_id: Uuid,
    access_mode: String,
}

#[derive(sqlx::FromRow)]
struct DocumentDeleteTarget {
    object_key: String,
}

#[derive(sqlx::FromRow)]
struct RetryDocumentRow {
    status: String,
    object_key: String,
}

#[derive(sqlx::FromRow)]
struct PreviewDocumentTarget {
    status: String,
    access_mode: String,
}

pub async fn get_document_chunk(
    State(state): State<AppState>,
    authz: Authz,
    Path((workspace_id, chunk_id)): Path<(Uuid, Uuid)>,
) -> impl IntoResponse {
    if let Err(err) = authz
        .require_relation(Relation::Member, &Object::Workspace(workspace_id))
        .await
    {
        return err.into_response();
    }

    match fetch_workspace_chunk_with_acl(&state.pool, workspace_id, chunk_id).await {
        Ok(Some(chunk)) => {
            let access_mode = match parse_access_mode_or_500(
                chunk.document_id,
                &chunk.access_mode,
                &authz,
                workspace_id,
                Some(chunk_id),
                "Failed to parse chunk document access_mode",
            ) {
                Ok(mode) => mode,
                Err(response) => return response,
            };

            let can_view = match can_user_view_document(
                &state.authz_client,
                &authz.user_id,
                chunk.document_id,
                access_mode,
            )
            .await
            {
                Ok(allowed) => allowed,
                Err(err) => {
                    error!(
                        error = %err,
                        user_id = %authz.user_id,
                        workspace_id = %workspace_id,
                        chunk_id = %chunk_id,
                        document_id = %chunk.document_id,
                        "Failed to re-check document ACL for chunk"
                    );
                    return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                }
            };

            if !can_view {
                return StatusCode::NOT_FOUND.into_response();
            }

            Json(ChunkResponse {
                id: chunk.id,
                original_text: chunk.original_text,
            })
            .into_response()
        }
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

async fn fetch_workspace_chunk_with_acl(
    pool: &PgPool,
    workspace_id: Uuid,
    chunk_id: Uuid,
) -> Result<Option<ChunkWithDocumentAclRow>, sqlx::Error> {
    sqlx::query_as(
        r#"
        SELECT
            dc.id,
            dc.original_text,
            dc.document_id,
            d.access_mode
        FROM document_chunks dc
        INNER JOIN documents d
            ON d.id = dc.document_id
           AND d.workspace_id = dc.workspace_id
        WHERE dc.id = $1
          AND dc.workspace_id = $2
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
    if let Err(err) = authz
        .require_relation(Relation::Member, &Object::Workspace(workspace_id))
        .await
    {
        return err.into_response();
    }

    let preview_target: Result<Option<PreviewDocumentTarget>, sqlx::Error> = sqlx::query_as(
        r#"
        SELECT status, access_mode
        FROM documents
        WHERE id = $1 AND workspace_id = $2
        "#,
    )
    .bind(document_id)
    .bind(workspace_id)
    .fetch_optional(&state.pool)
    .await;

    let preview_target = match preview_target {
        Ok(Some(target)) => target,
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
    };

    let access_mode = match parse_access_mode_or_500(
        document_id,
        &preview_target.access_mode,
        &authz,
        workspace_id,
        None,
        "Failed to parse preview document access_mode",
    ) {
        Ok(mode) => mode,
        Err(response) => return response,
    };

    let can_view = match can_user_view_document(
        &state.authz_client,
        &authz.user_id,
        document_id,
        access_mode,
    )
    .await
    {
        Ok(allowed) => allowed,
        Err(err) => {
            error!(
                error = %err,
                user_id = %authz.user_id,
                workspace_id = %workspace_id,
                document_id = %document_id,
                "Failed to re-check document ACL before preview"
            );
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    if !can_view {
        return StatusCode::NOT_FOUND.into_response();
    }

    if preview_target.status != "COMPLETED" {
        return StatusCode::CONFLICT.into_response();
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

fn parse_access_mode_or_500(
    document_id: Uuid,
    raw_mode: &str,
    authz: &Authz,
    workspace_id: Uuid,
    chunk_id: Option<Uuid>,
    context_message: &str,
) -> Result<DocumentAccessMode, axum::response::Response> {
    match DocumentAccessMode::parse(raw_mode) {
        Some(mode) => Ok(mode),
        None => {
            error!(
                user_id = %authz.user_id,
                workspace_id = %workspace_id,
                document_id = %document_id,
                chunk_id = ?chunk_id,
                access_mode = %raw_mode,
                "{}",
                context_message,
            );
            Err(StatusCode::INTERNAL_SERVER_ERROR.into_response())
        }
    }
}

pub async fn list_documents(
    State(state): State<AppState>,
    authz: Authz,
    Path(workspace_id): Path<Uuid>,
) -> impl IntoResponse {
    if let Err(err) = authz
        .require_relation(Relation::Member, &Object::Workspace(workspace_id))
        .await
    {
        return err.into_response();
    }

    match fetch_workspace_documents(&state.pool, workspace_id).await {
        Ok(rows) => {
            let acl_rows = match rows
                .iter()
                .map(|row| {
                    let access_mode = DocumentAccessMode::parse(&row.access_mode).ok_or_else(|| {
                        format!(
                            "invalid access_mode '{}' for document {}",
                            row.access_mode, row.id
                        )
                    })?;
                    Ok(DocumentAclRow {
                        document_id: row.id,
                        access_mode,
                    })
                })
                .collect::<Result<Vec<DocumentAclRow>, String>>()
            {
                Ok(parsed) => parsed,
                Err(err) => {
                    error!(
                        error = %err,
                        user_id = %authz.user_id,
                        workspace_id = %workspace_id,
                        "Failed to parse document access_mode while listing"
                    );
                    return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                }
            };

            let visible_ids = match collect_viewable_document_ids(
                &state.authz_client,
                &authz.user_id,
                &acl_rows,
            )
            .await
            {
                Ok(ids) => ids,
                Err(err) => {
                    error!(
                        error = %err,
                        user_id = %authz.user_id,
                        workspace_id = %workspace_id,
                        "Failed to apply document ACL while listing"
                    );
                    return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                }
            };

            let documents: Vec<DocumentResponse> = rows
                .into_iter()
                .filter(|row| visible_ids.contains(&row.id))
                .map(|row| DocumentResponse {
                    id: row.id,
                    filename: row.filename,
                    status: row.status,
                    processing_stage: row.processing_stage,
                    created_at: row.created_at,
                })
                .collect();

            Json(documents).into_response()
        }
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
    if let Err(err) = authz
        .require_relation(Relation::Admin, &Object::Workspace(workspace_id))
        .await
    {
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

    let delete_target: Result<Option<DocumentDeleteTarget>, sqlx::Error> = sqlx::query_as(
        r#"
        SELECT object_key
        FROM documents
        WHERE id = $1 AND workspace_id = $2
        "#,
    )
    .bind(document_id)
    .bind(workspace_id)
    .fetch_optional(&mut *tx)
    .await;

    let delete_target = match delete_target {
        Ok(Some(target)) => target,
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
    };

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
            if let Err(err) = remove_document_workspace_relation(
                &state.authz_client,
                workspace_id,
                document_id,
            )
            .await
            {
                error!(
                    error = %err,
                    user_id = %authz.user_id,
                    workspace_id = %workspace_id,
                    document_id = %document_id,
                    "Failed to delete document workspace relation in OpenFGA"
                );
            }

            if let Err(err) = state.storage.delete_object(&delete_target.object_key).await {
                // TODO: Phase 3 can bo sung cleanup worker/outbox de xu ly object con sot sau delete.
                error!(
                    error = %err,
                    user_id = %authz.user_id,
                    workspace_id = %workspace_id,
                    document_id = %document_id,
                    object_key = %delete_target.object_key,
                    "Failed to delete document object after database cleanup"
                );
            }
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
    if let Err(err) = authz
        .require_relation(Relation::Admin, &Object::Workspace(workspace_id))
        .await
    {
        return err.into_response();
    }

    let document_row: Result<Option<RetryDocumentRow>, sqlx::Error> = sqlx::query_as(
        r#"
        SELECT status, object_key
        FROM documents
        WHERE id = $1 AND workspace_id = $2
        "#,
    )
    .bind(document_id)
    .bind(workspace_id)
    .fetch_optional(&state.pool)
    .await;

    let document_row = match document_row {
        Ok(Some(row)) => row,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
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
    };

    if document_row.status != "FAILED" {
        return (StatusCode::CONFLICT, "Document is not in a failed state").into_response();
    }

    match state.storage.object_exists(&document_row.object_key).await {
        Ok(true) => {}
        Ok(false) => {
            return ApiError {
                status: StatusCode::GONE,
                code: "DOCUMENT_OBJECT_MISSING",
                message: "Original document object is missing".to_string(),
            }
            .into_response();
        }
        Err(err) => {
            error!(
                error = %err,
                user_id = %authz.user_id,
                workspace_id = %workspace_id,
                document_id = %document_id,
                object_key = %document_row.object_key,
                "Failed to verify document object before retry"
            );
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
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

    spawn_document_processing(
        state.pool.clone(),
        state.storage.clone(),
        state.retrieval.clone(),
        workspace_id,
        document_id,
        state.ingestion_limiter.clone(),
    );

    StatusCode::ACCEPTED.into_response()
}

async fn fetch_workspace_documents(
    pool: &PgPool,
    workspace_id: Uuid,
) -> Result<Vec<DocumentListRow>, sqlx::Error> {
    sqlx::query_as(
        r#"
        SELECT id, filename, status, processing_stage, created_at, access_mode
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
    if let Err(err) = authz
        .require_relation(Relation::Admin, &Object::Workspace(workspace_id))
        .await
    {
        return err.into_response();
    }

    let tenant_id = match fetch_workspace_tenant_id(&state.pool, workspace_id).await {
        Ok(Some(tenant_id)) => tenant_id,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(err) => {
            error!(
                error = %err,
                user_id = %authz.user_id,
                workspace_id = %workspace_id,
                "Failed to resolve tenant_id for workspace upload"
            );
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let mut uploaded: Vec<UploadedDocumentItem> = Vec::new();

    while let Ok(Some(field)) = multipart.next_field().await {
        if field.name() != Some("file") {
            continue;
        }

        let filename = field
            .file_name()
            .map(sanitize_filename)
            .unwrap_or_else(|| "upload.pdf".to_string());

        let content_type = field.content_type().map(|value| value.to_string());

        let pdf_bytes = match field.bytes().await {
            Ok(bytes) => bytes.to_vec(),
            Err(_) => continue,
        };

        if pdf_bytes.is_empty() || !is_pdf_upload(&filename, &pdf_bytes) {
            continue;
        }

        let document_id = Uuid::new_v4();
        let object_key = build_original_document_object_key(tenant_id, workspace_id, document_id);
        let checksum_sha256 = checksum_sha256_hex(&pdf_bytes);
        let size_bytes = i64::try_from(pdf_bytes.len()).unwrap_or(i64::MAX);

        let storage_etag = match state
            .storage
            .put_original_document(&object_key, &pdf_bytes, content_type.as_deref())
            .await
        {
            Ok(result) => result.etag,
            Err(err) => {
                error!(
                    error = %err,
                    user_id = %authz.user_id,
                    workspace_id = %workspace_id,
                    filename = %filename,
                    document_id = %document_id,
                    object_key = %object_key,
                    "Failed to upload document object"
                );
                continue;
            }
        };

        let insert_result = sqlx::query(
            r#"
            INSERT INTO documents (
                id,
                workspace_id,
                owner_id,
                filename,
                status,
                processing_stage,
                object_key,
                bucket,
                content_type,
                size_bytes,
                checksum_sha256,
                storage_etag,
                uploaded_by
            )
            VALUES ($1, $2, $3, $4, 'PROCESSING', 'QUEUED', $5, $6, $7, $8, $9, $10, $11)
            "#,
        )
        .bind(document_id)
        .bind(workspace_id)
        .bind(&authz.user_id)
        .bind(&filename)
        .bind(&object_key)
        .bind(state.storage.bucket())
        .bind(content_type.as_deref())
        .bind(size_bytes)
        .bind(&checksum_sha256)
        .bind(storage_etag.as_deref())
        .bind(&authz.user_id)
        .execute(&state.pool)
        .await;

        if let Err(err) = insert_result {
            error!(
                error = %err,
                user_id = %authz.user_id,
                workspace_id = %workspace_id,
                filename = %filename,
                document_id = %document_id,
                object_key = %object_key,
                "Failed to insert document row during upload"
            );

            if let Err(storage_err) = state.storage.delete_object(&object_key).await {
                error!(
                    error = %storage_err,
                    user_id = %authz.user_id,
                    workspace_id = %workspace_id,
                    document_id = %document_id,
                    object_key = %object_key,
                    "Failed to cleanup object after database insert failure"
                );
            }
            continue;
        }

        if let Err(err) = ensure_document_workspace_relation(
            &state.authz_client,
            workspace_id,
            document_id,
        )
        .await
        {
            error!(
                error = %err,
                user_id = %authz.user_id,
                workspace_id = %workspace_id,
                document_id = %document_id,
                "Failed to sync document workspace relation to OpenFGA"
            );

            if let Err(db_err) = sqlx::query(
                r#"
                DELETE FROM documents
                WHERE id = $1 AND workspace_id = $2
                "#,
            )
            .bind(document_id)
            .bind(workspace_id)
            .execute(&state.pool)
            .await
            {
                error!(
                    error = %db_err,
                    user_id = %authz.user_id,
                    workspace_id = %workspace_id,
                    document_id = %document_id,
                    "Failed to cleanup document row after OpenFGA sync failure"
                );
            }

            if let Err(storage_err) = state.storage.delete_object(&object_key).await {
                error!(
                    error = %storage_err,
                    user_id = %authz.user_id,
                    workspace_id = %workspace_id,
                    document_id = %document_id,
                    object_key = %object_key,
                    "Failed to cleanup object after OpenFGA sync failure"
                );
            }
            continue;
        }

        spawn_document_processing(
            state.pool.clone(),
            state.storage.clone(),
            state.retrieval.clone(),
            workspace_id,
            document_id,
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

fn checksum_sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    format!("{:x}", digest)
}

async fn fetch_workspace_tenant_id(
    pool: &PgPool,
    workspace_id: Uuid,
) -> Result<Option<Uuid>, sqlx::Error> {
    sqlx::query_scalar(
        r#"
        SELECT tenant_id
        FROM workspaces
        WHERE id = $1
        "#,
    )
    .bind(workspace_id)
    .fetch_optional(pool)
    .await
}
