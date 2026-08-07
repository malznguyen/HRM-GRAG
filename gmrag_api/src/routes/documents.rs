use axum::{
    Json,
    extract::{Multipart, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use tracing::error;
use uuid::Uuid;

use crate::audit::{AuditEventRecord, AuditEventType, insert_audit_event, insert_audit_event_tx};
use crate::auth::authz::{ApiError, Authz, Object, Relation, delete_tuples_fga_first};
use crate::auth::document_acl::{
    DocumentAccessMode, DocumentAclError, DocumentPermissionsUpdateError, can_user_view_document,
    collect_viewable_restricted_document_ids, ensure_document_workspace_relation,
    grant_document_explicit_viewer, replace_document_permissions, revoke_document_explicit_viewer,
    set_document_access_mode,
};
use crate::auth::resource_cleanup::capture_document_delete_plan;
use crate::document_directory::{
    DocumentDirectoryItem, DocumentDirectoryPage, get_document as get_document_directory,
    get_document_permissions, list_documents as list_document_directory,
};
use crate::document_format::{DocumentFormat, document_max_upload_bytes};
use crate::ingestion::jobs::{IngestionWorkerConfig, enqueue_job_tx, retry_failed_document};
use crate::retrieval::outbox::enqueue_delete_by_document_tx;
use crate::state::AppState;
use crate::storage::build_original_document_object_key;
use crate::storage::outbox::enqueue_delete_object_tx;

#[derive(Deserialize)]
pub struct SetDocumentAccessModeRequest {
    pub access_mode: String,
}

#[derive(Deserialize)]
pub struct ListDocumentsQuery {
    limit: Option<i64>,
    offset: Option<i64>,
    q: Option<String>,
    status: Option<String>,
}

#[derive(Serialize)]
pub struct WorkspaceDocumentCallerCapabilities {
    pub can_manage_documents: bool,
}

#[derive(Serialize)]
pub struct WorkspaceDocumentsResponse {
    #[serde(flatten)]
    pub page: DocumentDirectoryPage,
    pub caller: WorkspaceDocumentCallerCapabilities,
}

#[derive(Serialize)]
pub struct DocumentStatusResponse {
    pub document_id: Uuid,
    pub filename: String,
    pub status: String,
    pub processing_stage: String,
    pub failure_code: Option<String>,
    pub failure_message: Option<String>,
    pub access_mode: String,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
    pub chunk_count: i64,
}

impl From<DocumentDirectoryItem> for DocumentStatusResponse {
    fn from(document: DocumentDirectoryItem) -> Self {
        Self {
            document_id: document.id,
            filename: document.filename,
            status: document.status,
            processing_stage: document.processing_stage,
            failure_code: document.failure_code,
            failure_message: document.failure_message,
            access_mode: document.access_mode,
            created_at: document.created_at,
            updated_at: document.updated_at,
            chunk_count: document.chunk_count,
        }
    }
}

#[derive(Deserialize)]
pub struct ReplaceDocumentPermissionsRequest {
    pub access_mode: String,
    #[serde(default)]
    pub authorized_user_ids: Vec<String>,
}

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
pub struct PreviewChunkRow {
    pub chunk_index: i32,
    pub original_text: String,
    pub id: Uuid,
}

#[derive(Serialize)]
pub struct DocumentPreviewResponse {
    pub content: String,
    pub chunks: Vec<PreviewChunkItem>,
    pub document: PreviewDocumentMeta,
}

/// Metadata đủ để trình xem hiển thị tiêu đề và chọn renderer mà không phải dò lại
/// danh sách tài liệu (danh sách bị chặn 100 bản ghi mỗi lần gọi).
#[derive(Serialize)]
pub struct PreviewDocumentMeta {
    pub id: Uuid,
    pub filename: String,
    pub content_type: Option<String>,
    pub created_at: NaiveDateTime,
    pub size_bytes: Option<i64>,
    pub access_mode: String,
    pub status: String,
}

#[derive(Serialize)]
pub struct PreviewChunkItem {
    pub chunk_index: i32,
    pub text: String,
    pub id: Uuid,
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
struct RetryDocumentRow {
    object_key: String,
}

#[derive(sqlx::FromRow)]
struct PreviewDocumentTarget {
    status: String,
    access_mode: String,
    filename: String,
    content_type: Option<String>,
    created_at: NaiveDateTime,
    size_bytes: Option<i64>,
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
        SELECT status, access_mode, filename, content_type, created_at, size_bytes
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

    let document_meta = PreviewDocumentMeta {
        id: document_id,
        filename: preview_target.filename,
        content_type: preview_target.content_type,
        created_at: preview_target.created_at,
        size_bytes: preview_target.size_bytes,
        access_mode: preview_target.access_mode,
        status: preview_target.status,
    };

    match fetch_document_preview(&state.pool, workspace_id, document_id, document_meta).await {
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
    Query(params): Query<ListDocumentsQuery>,
) -> impl IntoResponse {
    let workspace = Object::Workspace(workspace_id);
    if let Err(err) = authz.require_relation(Relation::Member, &workspace).await {
        return err.into_response();
    }

    let limit = params.limit.unwrap_or(20);
    let offset = params.offset.unwrap_or(0);
    if !(0..=100).contains(&limit) || offset < 0 {
        return ApiError::new(
            StatusCode::BAD_REQUEST,
            "INVALID_REQUEST",
            "limit must be between 0 and 100 and offset must be non-negative",
        )
        .into_response();
    }

    let query = params.q.as_deref().map(str::trim).filter(|q| !q.is_empty());
    let status = match params.status.as_deref() {
        None => None,
        Some("PROCESSING") => Some("PROCESSING"),
        Some("COMPLETED") => Some("COMPLETED"),
        Some("FAILED") => Some("FAILED"),
        Some(_) => {
            return ApiError::new(
                StatusCode::BAD_REQUEST,
                "INVALID_REQUEST",
                "status must be PROCESSING, COMPLETED, or FAILED",
            )
            .into_response();
        }
    };

    let (can_manage_documents, visible_restricted_ids) = tokio::join!(
        authz.check(Relation::Admin, &workspace),
        collect_viewable_restricted_document_ids(&state.authz_client, &authz.user_id),
    );
    let can_manage_documents = match can_manage_documents {
        Ok(allowed) => allowed,
        Err(err) => {
            error!(
                error = %err,
                user_id = %authz.user_id,
                workspace_id = %workspace_id,
                "Failed to resolve workspace document capabilities"
            );
            return ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "AUTHZ_ERROR",
                "Authorization service unavailable",
            )
            .into_response();
        }
    };
    let visible_restricted_ids = match visible_restricted_ids {
        Ok(ids) => ids.into_iter().collect::<Vec<_>>(),
        Err(err) => {
            error!(
                error = %err,
                user_id = %authz.user_id,
                workspace_id = %workspace_id,
                "Failed to apply document ACL while listing"
            );
            return match err {
                DocumentAclError::Authz(_) => ApiError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "AUTHZ_ERROR",
                    "Authorization service unavailable",
                )
                .into_response(),
                _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            };
        }
    };

    match list_document_directory(
        &state.pool,
        workspace_id,
        &visible_restricted_ids,
        limit,
        offset,
        query,
        status,
    )
    .await
    {
        Ok(page) => Json(WorkspaceDocumentsResponse {
            page,
            caller: WorkspaceDocumentCallerCapabilities {
                can_manage_documents,
            },
        })
        .into_response(),
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

pub async fn get_document_status(
    State(state): State<AppState>,
    authz: Authz,
    Path((workspace_id, document_id)): Path<(Uuid, Uuid)>,
) -> impl IntoResponse {
    if let Err(err) = authz
        .require_relation(Relation::Member, &Object::Workspace(workspace_id))
        .await
    {
        return if err.status.is_server_error() {
            err.into_response()
        } else {
            StatusCode::NOT_FOUND.into_response()
        };
    }

    let document = match get_document_directory(&state.pool, workspace_id, document_id).await {
        Ok(Some(document)) => document,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(err) => {
            error!(
                error = %err,
                user_id = %authz.user_id,
                workspace_id = %workspace_id,
                document_id = %document_id,
                "Failed to read document status"
            );
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let access_mode = match DocumentAccessMode::parse(&document.access_mode) {
        Some(access_mode) => access_mode,
        None => {
            error!(
                user_id = %authz.user_id,
                workspace_id = %workspace_id,
                document_id = %document_id,
                "Document status read found an invalid access mode"
            );
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };
    let can_view = match can_user_view_document(
        &state.authz_client,
        &authz.user_id,
        document_id,
        access_mode,
    )
    .await
    {
        Ok(can_view) => can_view,
        Err(err) => {
            error!(
                error = %err,
                user_id = %authz.user_id,
                workspace_id = %workspace_id,
                document_id = %document_id,
                "Failed to apply document ACL while reading status"
            );
            return match err {
                DocumentAclError::Authz(_) => ApiError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "AUTHZ_ERROR",
                    "Authorization service unavailable",
                )
                .into_response(),
                _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            };
        }
    };
    if !can_view {
        return StatusCode::NOT_FOUND.into_response();
    }

    Json(DocumentStatusResponse::from(document)).into_response()
}

pub async fn get_document_shares(
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

    match get_document_permissions(&state.pool, workspace_id, document_id).await {
        Ok(Some(view)) => Json(view).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(err) => {
            error!(
                error = %err,
                user_id = %authz.user_id,
                workspace_id = %workspace_id,
                document_id = %document_id,
                "Failed to list document shares"
            );
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

pub async fn put_document_permissions(
    State(state): State<AppState>,
    authz: Authz,
    Path((workspace_id, document_id)): Path<(Uuid, Uuid)>,
    Json(body): Json<ReplaceDocumentPermissionsRequest>,
) -> impl IntoResponse {
    if let Err(err) = authz
        .require_relation(Relation::Admin, &Object::Workspace(workspace_id))
        .await
    {
        return err.into_response();
    }

    let access_mode = match DocumentAccessMode::parse(&body.access_mode) {
        Some(mode) => mode,
        None => {
            return ApiError::new(
                StatusCode::BAD_REQUEST,
                "INVALID_REQUEST",
                "access_mode must be workspace_default or restricted",
            )
            .into_response();
        }
    };
    if !access_mode.is_restricted() && !body.authorized_user_ids.is_empty() {
        return ApiError::new(
            StatusCode::BAD_REQUEST,
            "INVALID_REQUEST",
            "authorized_user_ids must be empty for workspace_default",
        )
        .into_response();
    }

    match replace_document_permissions(
        &state.pool,
        &state.authz_client,
        document_id,
        workspace_id,
        access_mode,
        &body.authorized_user_ids,
        &authz.user_id,
    )
    .await
    {
        Ok(Some(())) => {}
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(DocumentPermissionsUpdateError::UserNotWorkspaceMember { user_id }) => {
            return ApiError::new(
                StatusCode::BAD_REQUEST,
                "USER_NOT_WORKSPACE_MEMBER",
                "Target user is not a member of this workspace",
            )
            .into_response_with_details(json!({ "user_id": user_id }));
        }
        Err(DocumentPermissionsUpdateError::Authz(err)) => {
            error!(
                error = %err,
                user_id = %authz.user_id,
                workspace_id = %workspace_id,
                document_id = %document_id,
                "Authorization operation failed while replacing document permissions"
            );
            return ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "AUTHZ_ERROR",
                "Authorization service unavailable",
            )
            .into_response();
        }
        Err(DocumentPermissionsUpdateError::Database(err)) => {
            error!(
                error = %err,
                user_id = %authz.user_id,
                workspace_id = %workspace_id,
                document_id = %document_id,
                "Database operation failed while replacing document permissions"
            );
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    }

    match get_document_permissions(&state.pool, workspace_id, document_id).await {
        Ok(Some(view)) => Json(view).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(err) => {
            error!(
                error = %err,
                user_id = %authz.user_id,
                workspace_id = %workspace_id,
                document_id = %document_id,
                "Failed to read document permissions after update"
            );
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

pub async fn patch_document_access_mode(
    State(state): State<AppState>,
    authz: Authz,
    Path((workspace_id, document_id)): Path<(Uuid, Uuid)>,
    Json(body): Json<SetDocumentAccessModeRequest>,
) -> impl IntoResponse {
    if let Err(err) = authz
        .require_relation(Relation::Admin, &Object::Workspace(workspace_id))
        .await
    {
        return err.into_response();
    }

    let access_mode = match DocumentAccessMode::parse(&body.access_mode) {
        Some(mode) => mode,
        None => {
            return ApiError {
                status: StatusCode::BAD_REQUEST,
                code: "INVALID_ACCESS_MODE",
                message: "access_mode must be workspace_default or restricted".to_string(),
            }
            .into_response();
        }
    };

    match set_document_access_mode(
        &state.pool,
        &state.authz_client,
        document_id,
        workspace_id,
        access_mode,
        &authz.user_id,
    )
    .await
    {
        Ok(Some(())) => StatusCode::NO_CONTENT.into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(err) => {
            error!(
                error = %err,
                user_id = %authz.user_id,
                workspace_id = %workspace_id,
                document_id = %document_id,
                "Failed to set document access_mode"
            );
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

pub async fn share_document(
    State(state): State<AppState>,
    authz: Authz,
    Path((workspace_id, document_id, user_id)): Path<(Uuid, Uuid, String)>,
) -> impl IntoResponse {
    if let Err(err) = authz
        .require_relation(Relation::Admin, &Object::Workspace(workspace_id))
        .await
    {
        return err.into_response();
    }

    match document_exists_in_workspace(&state.pool, workspace_id, document_id).await {
        Ok(true) => {}
        Ok(false) => return StatusCode::NOT_FOUND.into_response(),
        Err(err) => {
            error!(
                error = %err,
                user_id = %authz.user_id,
                workspace_id = %workspace_id,
                document_id = %document_id,
                "Failed to verify document before share"
            );
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    }

    // Share target phải pass SQL membership VÀ OpenFGA member relation.
    // SQL-only stale membership (FGA đã revoke) không được grant.
    match is_workspace_member(&state.pool, workspace_id, &user_id).await {
        Ok(true) => {}
        Ok(false) => {
            return ApiError {
                status: StatusCode::BAD_REQUEST,
                code: "USER_NOT_WORKSPACE_MEMBER",
                message: "Target user is not a member of this workspace".to_string(),
            }
            .into_response();
        }
        Err(err) => {
            error!(
                error = %err,
                user_id = %authz.user_id,
                workspace_id = %workspace_id,
                target_user_id = %user_id,
                "Failed to verify workspace membership before document share"
            );
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    }

    match state
        .authz_client
        .check_workspace_member(&user_id, workspace_id)
        .await
    {
        Ok(true) => {}
        Ok(false) => {
            return ApiError {
                status: StatusCode::BAD_REQUEST,
                code: "USER_NOT_WORKSPACE_MEMBER",
                message: "Target user is not a member of this workspace".to_string(),
            }
            .into_response();
        }
        Err(err) => {
            error!(
                error = %err,
                user_id = %authz.user_id,
                workspace_id = %workspace_id,
                target_user_id = %user_id,
                "OpenFGA membership check failed before document share"
            );
            return ApiError {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                code: "AUTHZ_ERROR",
                message: "Authorization service unavailable".to_string(),
            }
            .into_response();
        }
    }

    if let Err(err) =
        grant_document_explicit_viewer(&state.pool, &state.authz_client, document_id, &user_id)
            .await
    {
        error!(
            error = %err,
            user_id = %authz.user_id,
            workspace_id = %workspace_id,
            document_id = %document_id,
            target_user_id = %user_id,
            "Failed to grant document explicit viewer"
        );
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    if let Err(err) = insert_audit_event(
        &state.pool,
        AuditEventRecord::new(AuditEventType::DocumentShared)
            .with_actor_user_id(authz.user_id.clone())
            .with_workspace_id(workspace_id)
            .with_document_id(document_id)
            .with_target("document_share", user_id.clone())
            .with_metadata(json!({
                "shared_with_user_id": user_id,
            })),
    )
    .await
    {
        error!(
            error = %err,
            actor_user_id = %authz.user_id,
            workspace_id = %workspace_id,
            document_id = %document_id,
            "Failed to write audit event for document share"
        );
    }

    StatusCode::CREATED.into_response()
}

pub async fn revoke_document_share(
    State(state): State<AppState>,
    authz: Authz,
    Path((workspace_id, document_id, user_id)): Path<(Uuid, Uuid, String)>,
) -> impl IntoResponse {
    if let Err(err) = authz
        .require_relation(Relation::Admin, &Object::Workspace(workspace_id))
        .await
    {
        return err.into_response();
    }

    match document_exists_in_workspace(&state.pool, workspace_id, document_id).await {
        Ok(true) => {}
        Ok(false) => return StatusCode::NOT_FOUND.into_response(),
        Err(err) => {
            error!(
                error = %err,
                user_id = %authz.user_id,
                workspace_id = %workspace_id,
                document_id = %document_id,
                "Failed to verify document before revoke share"
            );
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    }

    if let Err(err) =
        revoke_document_explicit_viewer(&state.pool, &state.authz_client, document_id, &user_id)
            .await
    {
        error!(
            error = %err,
            user_id = %authz.user_id,
            workspace_id = %workspace_id,
            document_id = %document_id,
            target_user_id = %user_id,
            "Failed to revoke document explicit viewer"
        );
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    if let Err(err) = insert_audit_event(
        &state.pool,
        AuditEventRecord::new(AuditEventType::DocumentShareRevoked)
            .with_actor_user_id(authz.user_id.clone())
            .with_workspace_id(workspace_id)
            .with_document_id(document_id)
            .with_target("document_share", user_id.clone())
            .with_metadata(json!({
                "revoked_user_id": user_id,
            })),
    )
    .await
    {
        error!(
            error = %err,
            actor_user_id = %authz.user_id,
            workspace_id = %workspace_id,
            document_id = %document_id,
            "Failed to write audit event for document share revoke"
        );
    }

    StatusCode::NO_CONTENT.into_response()
}

async fn document_exists_in_workspace(
    pool: &PgPool,
    workspace_id: Uuid,
    document_id: Uuid,
) -> Result<bool, sqlx::Error> {
    let exists: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS(
            SELECT 1
            FROM documents
            WHERE id = $1 AND workspace_id = $2
        )
        "#,
    )
    .bind(document_id)
    .bind(workspace_id)
    .fetch_one(pool)
    .await?;

    Ok(exists)
}

async fn is_workspace_member(
    pool: &PgPool,
    workspace_id: Uuid,
    user_id: &str,
) -> Result<bool, sqlx::Error> {
    let exists: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS(
            SELECT 1
            FROM workspace_members
            WHERE workspace_id = $1 AND user_id = $2
        )
        "#,
    )
    .bind(workspace_id)
    .bind(user_id)
    .fetch_one(pool)
    .await?;

    Ok(exists)
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

    let delete_target = match capture_document_delete_plan(&mut tx, workspace_id, document_id).await
    {
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

    if let Err(err) = delete_tuples_fga_first(&state.authz_client, &delete_target.tuples).await {
        error!(error = %err, workspace_id = %workspace_id, document_id = %document_id, "Failed to revoke document tuples in OpenFGA before deletion");
        return ApiError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "AUTHZ_REVOKE_FAILED",
            message: "Failed to revoke document authorization before deletion".to_string(),
        }
        .into_response();
    }

    let result = async {
        insert_audit_event_tx(
            &mut tx,
            AuditEventRecord::new(AuditEventType::DocumentDeleted)
                .with_actor_user_id(authz.user_id.clone())
                .with_workspace_id(workspace_id)
                .with_document_id(document_id)
                .with_target("document", document_id.to_string())
                .with_metadata(json!({
                    "openfga_tuples_revoked": delete_target.tuples.len(),
                    "qdrant_cleanup": "outbox_enqueued",
                    "storage_cleanup": "outbox_enqueued",
                })),
        )
        .await?;

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

        // Outbox cùng transaction với SQL delete — commit xong đã có recovery row
        // dù process crash trước khi gọi Qdrant/S3 (LIFE-001 / LIFE-003).
        enqueue_delete_by_document_tx(&mut tx, workspace_id, document_id).await?;
        enqueue_delete_object_tx(
            &mut tx,
            &delete_target.object_key,
            &delete_target.bucket,
            workspace_id,
            document_id,
        )
        .await?;

        tx.commit().await
    }
    .await;

    match result {
        Ok(_) => {
            // Best-effort sau commit; outbox storage_outbox đã durable (LIFE-003).
            if let Err(err) = state.storage.delete_object(&delete_target.object_key).await {
                error!(
                    error = %err,
                    user_id = %authz.user_id,
                    workspace_id = %workspace_id,
                    document_id = %document_id,
                    object_key = %delete_target.object_key,
                    "Failed to delete document object after database cleanup"
                );
            }

            // SQL + outbox đã commit: Qdrant best-effort sau commit; lỗi/timeout không
            // fail HTTP (vẫn 204). Row outbox có thể còn PENDING cho worker (LIFE-002).
            if let Err(err) = state
                .retrieval
                .delete_points_by_document_for_request(workspace_id, document_id)
                .await
            {
                error!(
                    error = %err,
                    user_id = %authz.user_id,
                    workspace_id = %workspace_id,
                    document_id = %document_id,
                    request_timeout_secs = state.retrieval.delete_request_timeout_secs(),
                    "Failed to delete document points from Qdrant after database cleanup"
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
        SELECT object_key
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

    let requeued = retry_failed_document(
        &state.pool,
        document_id,
        workspace_id,
        IngestionWorkerConfig::from_env().max_attempts,
    )
    .await;
    let requeued = match requeued {
        Ok(requeued) => requeued,
        Err(err) => {
            error!(
                error = %err,
                user_id = %authz.user_id,
                workspace_id = %workspace_id,
                document_id = %document_id,
                "Failed to reset document status before retry"
            );
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };
    if !requeued {
        return ApiError {
            status: StatusCode::CONFLICT,
            code: "DOCUMENT_NOT_RETRYABLE",
            message: "Document is not in a retryable failed state".to_string(),
        }
        .into_response();
    }

    if let Err(err) = insert_audit_event(
        &state.pool,
        AuditEventRecord::new(AuditEventType::DocumentRetryStarted)
            .with_actor_user_id(authz.user_id.clone())
            .with_workspace_id(workspace_id)
            .with_document_id(document_id)
            .with_target("document", document_id.to_string()),
    )
    .await
    {
        error!(
            error = %err,
            actor_user_id = %authz.user_id,
            workspace_id = %workspace_id,
            document_id = %document_id,
            "Failed to write audit event for document retry"
        );
    }

    StatusCode::ACCEPTED.into_response()
}

async fn fetch_document_preview(
    pool: &PgPool,
    workspace_id: Uuid,
    document_id: Uuid,
    document: PreviewDocumentMeta,
) -> Result<DocumentPreviewResponse, sqlx::Error> {
    let rows: Vec<PreviewChunkRow> = sqlx::query_as(
        r#"
        SELECT chunk_index, original_text, id
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
            id: row.id,
        })
        .collect();

    let content = chunks
        .iter()
        .map(|chunk| chunk.text.as_str())
        .collect::<Vec<_>>()
        .join("\n\n");

    Ok(DocumentPreviewResponse {
        content,
        chunks,
        document,
    })
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

    // Drain multipart trước khi insert để access_mode không phụ thuộc thứ tự field.
    let mut access_mode = DocumentAccessMode::WorkspaceDefault;
    let mut pending_files: Vec<PendingUploadFile> = Vec::new();

    while let Ok(Some(field)) = multipart.next_field().await {
        match field.name() {
            Some("access_mode") => {
                let raw = match field.text().await {
                    Ok(text) => text.trim().to_string(),
                    Err(_) => {
                        return ApiError {
                            status: StatusCode::BAD_REQUEST,
                            code: "INVALID_ACCESS_MODE",
                            message: "access_mode must be workspace_default or restricted"
                                .to_string(),
                        }
                        .into_response();
                    }
                };
                match DocumentAccessMode::parse(&raw) {
                    Some(mode) => access_mode = mode,
                    None => {
                        return ApiError {
                            status: StatusCode::BAD_REQUEST,
                            code: "INVALID_ACCESS_MODE",
                            message: "access_mode must be workspace_default or restricted"
                                .to_string(),
                        }
                        .into_response();
                    }
                }
            }
            Some("file") => {
                let filename = field
                    .file_name()
                    .map(sanitize_filename)
                    .unwrap_or_else(|| "upload.pdf".to_string());
                let bytes = match field.bytes().await {
                    Ok(bytes) => bytes.to_vec(),
                    Err(_) => continue,
                };
                if bytes.len() > document_max_upload_bytes() {
                    continue;
                }
                let Some(format) = DocumentFormat::detect_upload(&filename, &bytes) else {
                    continue;
                };
                pending_files.push(PendingUploadFile {
                    filename,
                    format,
                    bytes,
                });
            }
            _ => continue,
        }
    }

    let mut uploaded: Vec<UploadedDocumentItem> = Vec::new();

    for file in pending_files {
        let filename = file.filename;
        let content_type = file.format.content_type();
        let bytes = file.bytes;

        let document_id = Uuid::new_v4();
        let object_key = build_original_document_object_key(tenant_id, workspace_id, document_id);
        let checksum_sha256 = checksum_sha256_hex(&bytes);
        let size_bytes = i64::try_from(bytes.len()).unwrap_or(i64::MAX);

        let storage_etag = match state
            .storage
            .put_original_document(&object_key, &bytes, Some(content_type))
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

        let mut transaction = match state.pool.begin().await {
            Ok(transaction) => transaction,
            Err(err) => {
                error!(error = %err, %workspace_id, %document_id, "Failed to begin upload database transaction");
                let _ = state.storage.delete_object(&object_key).await;
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
                access_mode,
                object_key,
                bucket,
                content_type,
                size_bytes,
                checksum_sha256,
                storage_etag,
                uploaded_by
            )
            VALUES ($1, $2, $3, $4, 'PROCESSING', 'QUEUED', $5, $6, $7, $8, $9, $10, $11, $12)
            "#,
        )
        .bind(document_id)
        .bind(workspace_id)
        .bind(&authz.user_id)
        .bind(&filename)
        .bind(access_mode.as_str())
        .bind(&object_key)
        .bind(state.storage.bucket())
        .bind(content_type)
        .bind(size_bytes)
        .bind(&checksum_sha256)
        .bind(storage_etag.as_deref())
        .bind(&authz.user_id)
        .execute(&mut *transaction)
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

        if let Err(err) = enqueue_job_tx(
            &mut transaction,
            document_id,
            workspace_id,
            IngestionWorkerConfig::from_env().max_attempts,
        )
        .await
        {
            error!(error = %err, %workspace_id, %document_id, "Failed to enqueue durable ingestion job during upload");
            let _ = transaction.rollback().await;
            let _ = state.storage.delete_object(&object_key).await;
            continue;
        }
        if let Err(err) = transaction.commit().await {
            error!(error = %err, %workspace_id, %document_id, "Failed to commit upload document and durable job");
            let _ = state.storage.delete_object(&object_key).await;
            continue;
        }

        if let Err(err) =
            ensure_document_workspace_relation(&state.authz_client, workspace_id, document_id).await
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

        if let Err(err) = insert_audit_event(
            &state.pool,
            AuditEventRecord::new(AuditEventType::DocumentUploaded)
                .with_actor_user_id(authz.user_id.clone())
                .with_tenant_id(tenant_id)
                .with_workspace_id(workspace_id)
                .with_document_id(document_id)
                .with_target("document", document_id.to_string())
                .with_metadata(json!({
                    "filename": filename.clone(),
                    "size_bytes": size_bytes,
                    "content_type": content_type,
                    "access_mode": access_mode.as_str(),
                })),
        )
        .await
        {
            error!(
                error = %err,
                actor_user_id = %authz.user_id,
                tenant_id = %tenant_id,
                workspace_id = %workspace_id,
                document_id = %document_id,
                "Failed to write audit event for document upload"
            );
        }

        uploaded.push(UploadedDocumentItem {
            document_id,
            filename,
        });
    }

    if uploaded.is_empty() {
        return ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "INVALID_REQUEST",
            message: "Request did not contain an acceptable file".to_string(),
        }
        .into_response();
    }

    (
        StatusCode::ACCEPTED,
        Json(UploadDocumentsResponse {
            documents: uploaded,
        }),
    )
        .into_response()
}

struct PendingUploadFile {
    filename: String,
    format: DocumentFormat,
    bytes: Vec<u8>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn document_status_response_contains_the_required_schema() {
        let document_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
        let created_at =
            NaiveDateTime::parse_from_str("2026-08-07 08:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let updated_at =
            NaiveDateTime::parse_from_str("2026-08-07 08:05:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let response = DocumentStatusResponse::from(DocumentDirectoryItem {
            id: document_id,
            filename: "policy.pdf".to_string(),
            status: "COMPLETED".to_string(),
            processing_stage: "DONE".to_string(),
            failure_code: None,
            failure_message: None,
            created_at,
            updated_at,
            chunk_count: 3,
            size_bytes: Some(1024),
            access_mode: "workspace_default".to_string(),
            uploaded_by: "employee-1".to_string(),
            uploaded_by_email: None,
            content_type: Some("application/pdf".to_string()),
        });

        assert_eq!(
            serde_json::to_value(response).unwrap(),
            json!({
                "document_id": document_id,
                "filename": "policy.pdf",
                "status": "COMPLETED",
                "processing_stage": "DONE",
                "failure_code": null,
                "failure_message": null,
                "access_mode": "workspace_default",
                "created_at": "2026-08-07T08:00:00",
                "updated_at": "2026-08-07T08:05:00",
                "chunk_count": 3
            })
        );
    }
}
