use crate::api_error::ApiError;
use crate::auth::hrm::{HrmProvisionError, HrmScopeError, ensure_scope, provision_identity};
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
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let token = bearer_token(parts).ok_or_else(|| {
            ApiError::new(
                StatusCode::UNAUTHORIZED,
                "UNAUTHORIZED",
                "Missing or invalid Authorization header",
            )
        })?;

        let claims = state.jwt.validate(token).await.map_err(jwt_rejection)?;

        if let Some(config) = state.jwt.hrm_config() {
            ensure_scope(parts.uri.path(), config).map_err(hrm_scope_rejection)?;
            provision_identity(&state.pool, &state.authz_client, config, &claims)
                .await
                .map_err(hrm_provision_rejection)?;
        }

        parts.extensions.insert(claims.clone());

        Ok(AuthUser {
            user_id: claims.sub,
            email: claims.email,
            email_verified: claims.email_verified,
        })
    }
}

fn hrm_scope_rejection(error: HrmScopeError) -> ApiError {
    let message = match error {
        HrmScopeError::Tenant(tenant_id) => {
            format!("HRM token is scoped to a different tenant: {tenant_id}")
        }
        HrmScopeError::Workspace(workspace_id) => {
            format!("HRM token is scoped to a different workspace: {workspace_id}")
        }
    };
    ApiError::new(StatusCode::BAD_REQUEST, "HRM_SCOPE_MISMATCH", message)
}

fn hrm_provision_rejection(error: HrmProvisionError) -> ApiError {
    match error {
        HrmProvisionError::InvalidRole => ApiError::new(
            StatusCode::FORBIDDEN,
            "HRM_ROLE_REQUIRED",
            "A valid HRM role is required",
        ),
        HrmProvisionError::Database(error) => {
            tracing::error!(%error, "Failed to provision HRM user");
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                "Authentication provisioning failed",
            )
        }
        HrmProvisionError::Authz(error) => {
            tracing::error!(%error, "Failed to synchronize HRM authorization");
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "AUTHZ_ERROR",
                "Authorization service unavailable",
            )
        }
    }
}

fn bearer_token(parts: &Parts) -> Option<&str> {
    let value = parts.headers.get(AUTHORIZATION)?.to_str().ok()?;
    let prefix = "Bearer ";
    value.strip_prefix(prefix).filter(|t| !t.is_empty())
}

fn jwt_rejection(err: JwtError) -> ApiError {
    match err {
        JwtError::MissingConfig(_) | JwtError::InvalidConfig(_) => ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL_ERROR",
            "Authentication service unavailable",
        ),
        JwtError::FetchJwks | JwtError::InvalidJwks | JwtError::UnknownKeyId => {
            ApiError::new(StatusCode::UNAUTHORIZED, "INVALID_TOKEN", "Invalid token")
        }
        JwtError::InvalidToken => {
            ApiError::new(StatusCode::UNAUTHORIZED, "INVALID_TOKEN", "Invalid token")
        }
    }
}
