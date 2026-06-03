use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use serde::{Deserialize, Serialize};
use tracing::{error, warn};

use crate::auth::extractor::AuthUser;
use crate::invite::{normalize_email, reconcile_pending_invites};
use crate::state::AppState;

#[derive(Serialize, sqlx::FromRow)]
pub struct UserResponse {
    pub id: String,
    pub email: String,
    pub is_super_admin: bool,
}

#[derive(Deserialize)]
pub struct SyncUserRequest {
    pub email: String,
}

pub async fn get_current_user(State(state): State<AppState>, auth: AuthUser) -> impl IntoResponse {
    let user: Result<Option<UserResponse>, _> =
        sqlx::query_as("SELECT id, email, is_super_admin FROM users WHERE id = $1")
            .bind(&auth.user_id)
            .fetch_optional(&state.pool)
            .await;

    match user {
        Ok(Some(user)) => {
            if let Err(err) =
                reconcile_pending_invites(&state.pool, &auth.user_id, &user.email).await
            {
                warn!(
                    error = %err,
                    user_id = %auth.user_id,
                    "Failed to reconcile pending invites on current user fetch"
                );
            }
            Json(user).into_response()
        }
        Ok(None) => (StatusCode::NOT_FOUND, "User not found").into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

pub async fn sync_current_user(
    State(state): State<AppState>,
    auth: AuthUser,
    Json(body): Json<SyncUserRequest>,
) -> impl IntoResponse {
    let email = normalize_email(&body.email);
    if email.is_empty() || !email.contains('@') {
        return (StatusCode::BAD_REQUEST, "Valid email is required").into_response();
    }

    if let Err(err) = reconcile_pending_invites(&state.pool, &auth.user_id, &email).await {
        error!(
            error = %err,
            user_id = %auth.user_id,
            email = %email,
            "Failed to reconcile pending invites during sync"
        );
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to sync user: {err}"),
        )
            .into_response();
    }

    StatusCode::OK.into_response()
}
