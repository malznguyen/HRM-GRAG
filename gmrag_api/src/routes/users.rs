use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use serde::Serialize;
use serde_json::json;
use tracing::error;

use crate::auth::authz::{Authz, Object, Relation};
use crate::invite::upsert_user;
use crate::state::AppState;

#[derive(Serialize)]
pub struct UserResponse {
    pub id: String,
    pub email: String,
    pub is_super_admin: bool,
}

pub async fn get_current_user(State(state): State<AppState>, authz: Authz) -> impl IntoResponse {
    let db_user: Option<(String, String)> =
        sqlx::query_as("SELECT id, email FROM users WHERE id = $1")
            .bind(&authz.user_id)
            .fetch_optional(&state.pool)
            .await
            .unwrap_or(None);

    let Some((id, email)) = db_user else {
        return (StatusCode::NOT_FOUND, "User not found").into_response();
    };

    // Check Platform Admin via OpenFGA
    let is_super_admin = match authz.check(Relation::Admin, &Object::Platform).await {
        Ok(allowed) => allowed,
        Err(err) => {
            error!(error = %err, user_id = %authz.user_id, "Failed to check platform admin in OpenFGA");
            false
        }
    };

    let user_resp = UserResponse {
        id,
        email,
        is_super_admin,
    };

    Json(user_resp).into_response()
}

/// Đồng bộ profile từ claims OIDC đã xác thực, không nhận email từ client.
pub async fn sync_current_user(State(state): State<AppState>, authz: Authz) -> impl IntoResponse {
    let Some(email) = authz
        .email
        .as_deref()
        .map(str::trim)
        .filter(|email| !email.is_empty())
    else {
        return identity_sync_error(
            StatusCode::BAD_REQUEST,
            "IDENTITY_EMAIL_REQUIRED",
            "Verified email claim is required",
        );
    };
    if !authz.email_verified {
        return identity_sync_error(
            StatusCode::BAD_REQUEST,
            "IDENTITY_EMAIL_UNVERIFIED",
            "Verified email claim is required",
        );
    }

    if let Err(err) = upsert_user(&state.pool, &authz.user_id, &email).await {
        if is_identity_email_conflict(&err) {
            return identity_sync_error(
                StatusCode::CONFLICT,
                "IDENTITY_EMAIL_CONFLICT",
                "Email is already associated with another identity",
            );
        }
        error!(
            error = %err,
            user_id = %authz.user_id,
            "Failed to upsert user during sync"
        );
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    StatusCode::OK.into_response()
}

fn identity_sync_error(
    status: StatusCode,
    code: &'static str,
    message: &'static str,
) -> axum::response::Response {
    (
        status,
        Json(json!({ "error": { "code": code, "message": message } })),
    )
        .into_response()
}

fn is_identity_email_conflict(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|database_error| database_error.code())
        .is_some_and(|code| code == "23505")
}
