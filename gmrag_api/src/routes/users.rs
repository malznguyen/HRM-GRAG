use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use serde::Serialize;

use crate::auth::extractor::AuthUser;
use crate::state::AppState;

#[derive(Serialize, sqlx::FromRow)]
pub struct UserResponse {
    pub id: String,
    pub email: String,
    pub is_super_admin: bool,
}

pub async fn get_current_user(State(state): State<AppState>, auth: AuthUser) -> impl IntoResponse {
    let user: Result<Option<UserResponse>, _> =
        sqlx::query_as("SELECT id, email, is_super_admin FROM users WHERE id = $1")
            .bind(&auth.user_id)
            .fetch_optional(&state.pool)
            .await;

    match user {
        Ok(Some(user)) => Json(user).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, "User not found").into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}
