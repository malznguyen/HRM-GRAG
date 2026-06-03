use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use serde::Deserialize;
use tracing::error;

use crate::invite::{normalize_email, reconcile_pending_invites};
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
        Some(email) if !email.is_empty() => normalize_email(email),
        _ => return StatusCode::BAD_REQUEST,
    };

    match reconcile_pending_invites(&state.pool, &event.data.id, &email).await {
        Ok(()) => StatusCode::NO_CONTENT,
        Err(err) => {
            error!(
                error = %err,
                clerk_user_id = %event.data.id,
                email = %email,
                "Failed to reconcile user invite memberships"
            );
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}
