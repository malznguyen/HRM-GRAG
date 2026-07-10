use crate::auth::jwt::JwtError;
use crate::state::AppState;
use axum::{
    extract::FromRequestParts,
    http::{StatusCode, header::AUTHORIZATION, request::Parts},
};

pub struct AuthUser {
    pub user_id: String,
    pub email: Option<String>,
    pub email_verified: bool,
}

impl FromRequestParts<AppState> for AuthUser {
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let token = bearer_token(parts).ok_or((
            StatusCode::UNAUTHORIZED,
            "Missing or invalid Authorization header",
        ))?;

        let claims = state.jwt.validate(token).await.map_err(jwt_rejection)?;

        Ok(AuthUser {
            user_id: claims.sub,
            email: claims.email,
            email_verified: claims.email_verified,
        })
    }
}

fn bearer_token(parts: &Parts) -> Option<&str> {
    let value = parts.headers.get(AUTHORIZATION)?.to_str().ok()?;
    let prefix = "Bearer ";
    value.strip_prefix(prefix).filter(|t| !t.is_empty())
}

fn jwt_rejection(err: JwtError) -> (StatusCode, &'static str) {
    match err {
        JwtError::MissingConfig(_) | JwtError::InvalidConfig(_) => {
            (StatusCode::INTERNAL_SERVER_ERROR, "JWT not configured")
        }
        JwtError::FetchJwks | JwtError::InvalidJwks | JwtError::UnknownKeyId => {
            (StatusCode::UNAUTHORIZED, "Invalid token")
        }
        JwtError::InvalidToken => (StatusCode::UNAUTHORIZED, "Invalid token"),
    }
}
