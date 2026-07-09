use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use serde::{Deserialize, Serialize};
use tracing::error;

use crate::auth::authz::{Authz, Object, Relation};
use crate::invite::{normalize_email, upsert_user};
use crate::state::AppState;

#[derive(Serialize)]
pub struct UserResponse {
    pub id: String,
    pub email: String,
    pub is_super_admin: bool,
}

#[derive(Deserialize)]
pub struct SyncUserRequest {
    pub email: String,
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

/// Upsert profile SQL theo JWT `sub`. Không còn reconcile placeholder invite.
pub async fn sync_current_user(
    State(state): State<AppState>,
    authz: Authz,
    Json(body): Json<SyncUserRequest>,
) -> impl IntoResponse {
    let email = normalize_email(&body.email);
    if email.is_empty() || !email.contains('@') {
        return (StatusCode::BAD_REQUEST, "Valid email is required").into_response();
    }

    // user_id lấy từ JWT đã verify; email body vẫn là debt legacy (xem CURRENT_API_CONTRACT)
    if let Err(err) = upsert_user(&state.pool, &authz.user_id, &email).await {
        error!(
            error = %err,
            user_id = %authz.user_id,
            email = %email,
            "Failed to upsert user during sync"
        );
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to sync user: {err}"),
        )
            .into_response();
    }

    StatusCode::OK.into_response()
}
