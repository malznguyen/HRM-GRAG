use axum::{
    extract::FromRequestParts,
    http::{StatusCode, request::Parts},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

pub use crate::api_error::ApiError;
use crate::auth::extractor::AuthUser;
use crate::state::AppState;

/// Đại diện cho các relation trong OpenFGA model
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Relation {
    Admin,
    Owner,
    Platform,
    Member,
    CanAssignRole,
    CanManageMember,
    Workspace,
    ExplicitViewer,
    BypassViewer,
    Tenant,
}

impl Relation {
    pub fn as_str(&self) -> &'static str {
        match self {
            Relation::Admin => "admin",
            Relation::Owner => "owner",
            Relation::Platform => "platform",
            Relation::Member => "member",
            Relation::CanAssignRole => "can_assign_role",
            Relation::CanManageMember => "can_manage_member",
            Relation::Workspace => "workspace",
            Relation::ExplicitViewer => "explicit_viewer",
            Relation::BypassViewer => "bypass_viewer",
            Relation::Tenant => "tenant",
        }
    }
}

/// Đại diện cho các loại đối tượng (objects) trong OpenFGA
#[derive(Debug, Clone)]
pub enum Object {
    Platform,
    Tenant(Uuid),
    Workspace(Uuid),
    Document(Uuid),
}

impl fmt::Display for Object {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Object::Platform => write!(f, "platform:system"),
            Object::Tenant(id) => write!(f, "tenant:{}", id),
            Object::Workspace(id) => write!(f, "workspace:{}", id),
            Object::Document(id) => write!(f, "document:{}", id),
        }
    }
}

/// Lỗi xảy ra khi tương tác với OpenFGA
#[derive(Debug)]
pub enum AuthzError {
    Http(reqwest::Error),
    OpenFga {
        status: reqwest::StatusCode,
        body: String,
    },
}

impl std::error::Error for AuthzError {}

impl fmt::Display for AuthzError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuthzError::Http(err) => write!(f, "HTTP request failed: {}", err),
            AuthzError::OpenFga { status, body } => write!(
                f,
                "OpenFGA error response: status {}, body {}",
                status, body
            ),
        }
    }
}

impl From<reqwest::Error> for AuthzError {
    fn from(err: reqwest::Error) -> Self {
        AuthzError::Http(err)
    }
}

/// Cache kết quả kiểm tra quyền trong phạm vi một request
#[derive(Clone, Default)]
pub struct RequestAuthzCache {
    cache: Arc<Mutex<HashMap<(String, String, String), bool>>>,
}

impl<S> FromRequestParts<S> for RequestAuthzCache
where
    S: Send + Sync,
{
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        if let Some(cache) = parts.extensions.get::<RequestAuthzCache>() {
            Ok(cache.clone())
        } else {
            let cache = RequestAuthzCache::default();
            parts.extensions.insert(cache.clone());
            Ok(cache)
        }
    }
}

/// Client kết nối tới OpenFGA
#[derive(Clone)]
pub struct AuthzClient {
    client: reqwest::Client,
    api_url: String,
    store_id: String,
    model_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TupleKey {
    pub user: String,
    pub relation: String,
    pub object: String,
}

#[derive(Debug, Serialize)]
struct CheckRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    authorization_model_id: Option<String>,
    tuple_key: TupleKey,
}

#[derive(Debug, Deserialize)]
struct CheckResponse {
    allowed: bool,
}

#[derive(Debug, Serialize)]
struct ListObjectsRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    authorization_model_id: Option<String>,
    #[serde(rename = "type")]
    object_type: String,
    relation: String,
    user: String,
}

#[derive(Debug, Deserialize)]
struct ListObjectsResponse {
    objects: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ReadTuplesResponse {
    #[serde(default)]
    tuples: Vec<ReadTuple>,
    continuation_token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ReadTuple {
    key: TupleKey,
}

#[derive(Debug, Serialize)]
struct TupleKeys {
    tuple_keys: Vec<TupleKey>,
}

#[derive(Debug, Serialize)]
struct WriteRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    writes: Option<TupleKeys>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deletes: Option<TupleKeys>,
}

impl AuthzClient {
    pub fn new(api_url: String, store_id: String, model_id: Option<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            api_url,
            store_id,
            model_id,
        }
    }

    pub fn from_env() -> Result<Self, &'static str> {
        let api_url = std::env::var("OPENFGA_API_URL")
            .unwrap_or_else(|_| "http://localhost:8080".to_string());
        let store_id =
            std::env::var("OPENFGA_STORE_ID").map_err(|_| "OPENFGA_STORE_ID must be set")?;
        let model_id = std::env::var("OPENFGA_MODEL_ID").ok();

        Ok(Self::new(api_url, store_id, model_id))
    }

    pub async fn check_fga(
        &self,
        user: &str,
        relation: Relation,
        object: &Object,
    ) -> Result<bool, AuthzError> {
        let url = format!("{}/stores/{}/check", self.api_url, self.store_id);
        let payload = CheckRequest {
            authorization_model_id: self.model_id.clone(),
            tuple_key: TupleKey {
                user: user.to_string(),
                relation: relation.as_str().to_string(),
                object: object.to_string(),
            },
        };

        let response = self.client.post(&url).json(&payload).send().await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(AuthzError::OpenFga { status, body });
        }

        let check_resp: CheckResponse = response.json().await?;
        Ok(check_resp.allowed)
    }

    pub async fn check_with_cache(
        &self,
        cache: &RequestAuthzCache,
        user: &str,
        relation: Relation,
        object: &Object,
    ) -> Result<bool, AuthzError> {
        let key = (
            user.to_string(),
            relation.as_str().to_string(),
            object.to_string(),
        );
        {
            let guard = cache.cache.lock().await;
            if let Some(&allowed) = guard.get(&key) {
                return Ok(allowed);
            }
        }

        let allowed = self.check_fga(user, relation, object).await?;

        let mut guard = cache.cache.lock().await;
        guard.insert(key, allowed);
        Ok(allowed)
    }

    pub async fn write_tuples(
        &self,
        writes: Vec<TupleKey>,
        deletes: Vec<TupleKey>,
    ) -> Result<(), AuthzError> {
        let url = format!("{}/stores/{}/write", self.api_url, self.store_id);

        let writes_payload = if writes.is_empty() {
            None
        } else {
            Some(TupleKeys { tuple_keys: writes })
        };

        let deletes_payload = if deletes.is_empty() {
            None
        } else {
            Some(TupleKeys {
                tuple_keys: deletes,
            })
        };

        let payload = WriteRequest {
            writes: writes_payload,
            deletes: deletes_payload,
        };

        let response = self.client.post(&url).json(&payload).send().await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(AuthzError::OpenFga { status, body });
        }

        Ok(())
    }

    pub async fn list_objects(
        &self,
        user: &str,
        relation: Relation,
        object_type: &str,
    ) -> Result<Vec<String>, AuthzError> {
        let url = format!("{}/stores/{}/list-objects", self.api_url, self.store_id);
        let payload = ListObjectsRequest {
            authorization_model_id: self.model_id.clone(),
            object_type: object_type.to_string(),
            relation: relation.as_str().to_string(),
            user: user.to_string(),
        };

        let response = self.client.post(&url).json(&payload).send().await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(AuthzError::OpenFga { status, body });
        }

        let list_resp: ListObjectsResponse = response.json().await?;
        Ok(list_resp.objects)
    }

    /// Đọc tuple để operator đối chiếu; không dùng cho authorization ở request path.
    /// OpenFGA Read API là POST; body rỗng nghĩa là quét mọi tuple trong store (phân trang).
    pub async fn list_all_tuples(&self) -> Result<Vec<TupleKey>, AuthzError> {
        let url = format!("{}/stores/{}/read", self.api_url, self.store_id);
        let mut tuples = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut body = serde_json::Map::new();
            body.insert("page_size".to_string(), serde_json::json!(100));
            if let Some(token) = &continuation_token {
                body.insert(
                    "continuation_token".to_string(),
                    serde_json::Value::String(token.clone()),
                );
            }

            let response = self.client.post(&url).json(&body).send().await?;
            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(AuthzError::OpenFga { status, body });
            }

            let page: ReadTuplesResponse = response.json().await?;
            tuples.extend(page.tuples.into_iter().map(|entry| entry.key));
            let Some(token) = page.continuation_token.filter(|value| !value.is_empty()) else {
                return Ok(tuples);
            };
            continuation_token = Some(token);
        }
    }

    pub async fn write_tuple(
        &self,
        user: &str,
        relation: Relation,
        object: &Object,
    ) -> Result<(), AuthzError> {
        self.write_tuples(
            vec![TupleKey {
                user: user.to_string(),
                relation: relation.as_str().to_string(),
                object: object.to_string(),
            }],
            vec![],
        )
        .await
    }

    pub async fn delete_tuple(
        &self,
        user: &str,
        relation: Relation,
        object: &Object,
    ) -> Result<(), AuthzError> {
        self.write_tuples(
            vec![],
            vec![TupleKey {
                user: user.to_string(),
                relation: relation.as_str().to_string(),
                object: object.to_string(),
            }],
        )
        .await
    }

    /// Kiểm tra relation `member` trên workspace (admin/tenant_owner inherit qua model).
    /// Fail-closed: lỗi dependency trả `Err`, không suy luận true.
    pub async fn check_workspace_member(
        &self,
        user_id: &str,
        workspace_id: Uuid,
    ) -> Result<bool, AuthzError> {
        self.check_fga(
            &format!("user:{user_id}"),
            Relation::Member,
            &Object::Workspace(workspace_id),
        )
        .await
    }

    /// Lọc candidate workspace theo relation `member` của caller.
    /// Fail-closed nếu bất kỳ check OpenFGA nào lỗi — không fallback SQL-only.
    pub async fn filter_workspaces_by_member(
        &self,
        user_id: &str,
        workspace_ids: &[Uuid],
    ) -> Result<Vec<Uuid>, AuthzError> {
        let mut allowed = Vec::with_capacity(workspace_ids.len());
        for workspace_id in workspace_ids {
            if self.check_workspace_member(user_id, *workspace_id).await? {
                allowed.push(*workspace_id);
            }
        }
        Ok(allowed)
    }

    /// Kiểm tra relation `admin` explicit/inherited trên workspace.
    pub async fn check_workspace_admin(
        &self,
        user_id: &str,
        workspace_id: Uuid,
    ) -> Result<bool, AuthzError> {
        self.check_fga(
            &format!("user:{user_id}"),
            Relation::Admin,
            &Object::Workspace(workspace_id),
        )
        .await
    }

    /// Kiểm tra relation `owner` trên tenant.
    pub async fn check_tenant_owner(
        &self,
        user_id: &str,
        tenant_id: Uuid,
    ) -> Result<bool, AuthzError> {
        self.check_fga(
            &format!("user:{user_id}"),
            Relation::Owner,
            &Object::Tenant(tenant_id),
        )
        .await
    }
}

/// Extractor của axum thực hiện kiểm tra quyền
pub struct Authz {
    pub client: AuthzClient,
    pub user_id: String,
    pub email: Option<String>,
    pub email_verified: bool,
    pub cache: RequestAuthzCache,
}

impl Authz {
    pub async fn check(&self, relation: Relation, object: &Object) -> Result<bool, AuthzError> {
        self.client
            .check_with_cache(
                &self.cache,
                &format!("user:{}", self.user_id),
                relation,
                object,
            )
            .await
    }

    pub async fn require_relation(
        &self,
        relation: Relation,
        object: &Object,
    ) -> Result<(), ApiError> {
        let allowed = self.check(relation, object).await.map_err(|_| {
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "AUTHZ_ERROR",
                "Authorization service unavailable",
            )
        })?;

        if allowed {
            Ok(())
        } else {
            let code = match relation {
                Relation::Admin => "WORKSPACE_ADMIN_REQUIRED",
                Relation::Owner => "TENANT_OWNER_REQUIRED",
                Relation::CanAssignRole => "ROLE_ASSIGNMENT_DENIED",
                Relation::CanManageMember => "MEMBER_MANAGEMENT_DENIED",
                _ => "FORBIDDEN",
            };
            let message = match relation {
                Relation::Admin => "Workspace admin access required".to_string(),
                Relation::Owner => "Tenant owner access required".to_string(),
                Relation::CanAssignRole => "Only tenant owners can assign roles".to_string(),
                Relation::CanManageMember => {
                    "Workspace admin or tenant owner access required to manage members".to_string()
                }
                _ => "Access denied".to_string(),
            };
            Err(ApiError::new(StatusCode::FORBIDDEN, code, message))
        }
    }
}

impl FromRequestParts<AppState> for Authz {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let auth_user = AuthUser::from_request_parts(parts, state).await?;

        let cache = if let Some(cache) = parts.extensions.get::<RequestAuthzCache>() {
            cache.clone()
        } else {
            let cache = RequestAuthzCache::default();
            parts.extensions.insert(cache.clone());
            cache
        };

        Ok(Self {
            client: state.authz_client.clone(),
            user_id: auth_user.user_id,
            email: auth_user.email,
            email_verified: auth_user.email_verified,
            cache,
        })
    }
}
