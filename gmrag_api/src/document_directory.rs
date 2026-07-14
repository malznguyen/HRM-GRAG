use chrono::NaiveDateTime;
use serde::Serialize;
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

#[derive(Serialize, FromRow)]
pub struct DocumentDirectoryItem {
    pub id: Uuid,
    pub filename: String,
    pub status: String,
    pub processing_stage: String,
    pub failure_code: Option<String>,
    pub failure_message: Option<String>,
    pub created_at: NaiveDateTime,
    pub size_bytes: Option<i64>,
    pub access_mode: String,
    pub uploaded_by: String,
    pub uploaded_by_email: Option<String>,
    pub content_type: Option<String>,
}

#[derive(Serialize)]
pub struct DocumentDirectoryPage {
    pub documents: Vec<DocumentDirectoryItem>,
    pub total: i64,
    pub limit: i64,
    pub offset: i64,
}

#[derive(Serialize, FromRow)]
pub struct DocumentShareItem {
    pub user_id: String,
    pub email: String,
    pub shared_at: Option<NaiveDateTime>,
}

#[derive(Serialize)]
pub struct DocumentPermissionsView {
    pub document_id: Uuid,
    pub access_mode: String,
    pub shares: Vec<DocumentShareItem>,
}

/// Đọc một trang tài liệu sau khi tập restricted-visible đã được OpenFGA xác nhận.
pub async fn list_documents(
    pool: &PgPool,
    workspace_id: Uuid,
    visible_restricted_ids: &[Uuid],
    limit: i64,
    offset: i64,
    query: Option<&str>,
    status: Option<&str>,
) -> Result<DocumentDirectoryPage, sqlx::Error> {
    let total = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::bigint
        FROM documents d
        WHERE d.workspace_id = $1
          AND (
                d.access_mode = 'workspace_default'
                OR d.id = ANY($2::uuid[])
          )
          AND ($3::text IS NULL OR d.filename ILIKE '%' || $3 || '%')
          AND ($4::text IS NULL OR d.status = $4)
        "#,
    )
    .bind(workspace_id)
    .bind(visible_restricted_ids)
    .bind(query)
    .bind(status)
    .fetch_one(pool)
    .await?;

    let documents = sqlx::query_as::<_, DocumentDirectoryItem>(
        r#"
        SELECT
            d.id,
            d.filename,
            d.status,
            d.processing_stage,
            d.failure_code,
            d.failure_message,
            d.created_at,
            d.size_bytes,
            d.access_mode,
            d.uploaded_by,
            uploader.email AS uploaded_by_email,
            d.content_type
        FROM documents d
        LEFT JOIN users uploader ON uploader.id = d.uploaded_by
        WHERE d.workspace_id = $1
          AND (
                d.access_mode = 'workspace_default'
                OR d.id = ANY($2::uuid[])
          )
          AND ($3::text IS NULL OR d.filename ILIKE '%' || $3 || '%')
          AND ($4::text IS NULL OR d.status = $4)
        ORDER BY d.created_at DESC, d.id DESC
        LIMIT $5 OFFSET $6
        "#,
    )
    .bind(workspace_id)
    .bind(visible_restricted_ids)
    .bind(query)
    .bind(status)
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await?;

    Ok(DocumentDirectoryPage {
        documents,
        total,
        limit,
        offset,
    })
}

/// Đọc access mode và share read model của một document trong đúng workspace.
pub async fn get_document_permissions(
    pool: &PgPool,
    workspace_id: Uuid,
    document_id: Uuid,
) -> Result<Option<DocumentPermissionsView>, sqlx::Error> {
    let access_mode: Option<String> = sqlx::query_scalar(
        r#"
        SELECT access_mode
        FROM documents
        WHERE id = $1 AND workspace_id = $2
        "#,
    )
    .bind(document_id)
    .bind(workspace_id)
    .fetch_optional(pool)
    .await?;

    let Some(access_mode) = access_mode else {
        return Ok(None);
    };

    let shares = sqlx::query_as::<_, DocumentShareItem>(
        r#"
        SELECT ds.user_id, u.email, ds.shared_at
        FROM document_shares ds
        INNER JOIN users u ON u.id = ds.user_id
        WHERE ds.document_id = $1
        ORDER BY u.email ASC, ds.user_id ASC
        "#,
    )
    .bind(document_id)
    .fetch_all(pool)
    .await?;

    Ok(Some(DocumentPermissionsView {
        document_id,
        access_mode,
        shares,
    }))
}
