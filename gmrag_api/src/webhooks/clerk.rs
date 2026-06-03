use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use serde::Deserialize;

use crate::state::AppState;
use svix::webhooks::Webhook;

#[derive(Deserialize)]
struct ClerkWebhookEvent {
    #[serde(rename = "type")]
    event_type: String,
    data: ClerkUserData,
}

#[derive(Deserialize)]
struct ClerkUserData {
    id: String,
    email_addresses: Vec<ClerkEmailAddress>,
}

#[derive(Deserialize)]
struct ClerkEmailAddress {
    email_address: String,
}

pub async fn handle_clerk_webhook(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let secret = match std::env::var("CLERK_WEBHOOK_SECRET") {
        Ok(s) if !s.is_empty() => s,
        _ => return StatusCode::INTERNAL_SERVER_ERROR,
    };

    let webhook = match Webhook::new(&secret) {
        Ok(w) => w,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
    };

    if webhook.verify(&body, &headers).is_err() {
        return StatusCode::BAD_REQUEST;
    }

    let event: ClerkWebhookEvent = match serde_json::from_slice(&body) {
        Ok(e) => e,
        Err(_) => return StatusCode::BAD_REQUEST,
    };

    if event.event_type != "user.created" {
        return StatusCode::NO_CONTENT;
    }

    let email = match event
        .data
        .email_addresses
        .first()
        .map(|e| e.email_address.as_str())
    {
        Some(email) if !email.is_empty() => email,
        _ => return StatusCode::BAD_REQUEST,
    };

    let result =
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING")
            .bind(&event.data.id)
            .bind(email)
            .execute(&state.pool)
            .await;

    match result {
        Ok(_) => StatusCode::NO_CONTENT,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}
