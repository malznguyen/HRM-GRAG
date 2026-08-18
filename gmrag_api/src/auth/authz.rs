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

/// Connect timeout mặc định tới OpenFGA (giây). Authz là gate trên hot path của
/// MỌI request nên phải fail fast: nếu OpenFGA sập/không reachable, chặn nhanh
/// thay vì để connect treo và cạn connection pool.
const DEFAULT_OPENFGA_CONNECT_TIMEOUT_SECS: u64 = 2;
/// Request timeout mặc định tới OpenFGA (giây). Một authz check chậm hơn mức này
/// đã là outage — thà fail-closed nhanh còn hơn để request treo vô hạn. Giữ ngắn
/// có chủ đích: 10s ở đây tự nó là một outage.
const DEFAULT_OPENFGA_REQUEST_TIMEOUT_SECS: u64 = 3;

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
    api_token: Option<String>,
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
        let connect_timeout_secs = super::auth_timeout_secs_from_env(
            "OPENFGA_CONNECT_TIMEOUT_SECS",
            DEFAULT_OPENFGA_CONNECT_TIMEOUT_SECS,
        );
        let request_timeout_secs = super::auth_timeout_secs_from_env(
            "OPENFGA_REQUEST_TIMEOUT_SECS",
            DEFAULT_OPENFGA_REQUEST_TIMEOUT_SECS,
        );
        // Đọc token ngay trong `new` thay vì thêm tham số: giữ nguyên chữ ký cho
        // mọi caller hiện có. Chuỗi rỗng coi như không cấu hình.
        let api_token = std::env::var("OPENFGA_API_TOKEN")
            .ok()
            .filter(|value| !value.trim().is_empty());

        Self {
            client: super::build_auth_http_client(connect_timeout_secs, request_timeout_secs),
            api_url,
            store_id,
            model_id,
            api_token,
        }
    }

    /// Gắn preshared token cho outbound OpenFGA khi `OPENFGA_API_TOKEN` được cấu hình.
    /// Không cấu hình thì request đi nguyên trạng — giữ tương thích với setup local cũ.
    fn with_auth_header(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(token) = &self.api_token {
            return request.bearer_auth(token);
        }
        request
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

        let response = self
            .with_auth_header(self.client.post(&url).json(&payload))
            .send()
            .await?;

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

        let response = self
            .with_auth_header(self.client.post(&url).json(&payload))
            .send()
            .await?;

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

        let response = self
            .with_auth_header(self.client.post(&url).json(&payload))
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(AuthzError::OpenFga { status, body });
        }

        let list_resp: ListObjectsResponse = response.json().await?;
        Ok(list_resp.objects)
    }

    /// Đọc tuple khớp `filter` (user + relation + object). `None` = quét cả store.
    ///
    /// OpenFGA Read API là POST. Dùng filter trên đường nóng; `list_all_tuples`
    /// chỉ dành cho operator report.
    pub async fn read_tuples(
        &self,
        filter: Option<&TupleKey>,
    ) -> Result<Vec<TupleKey>, AuthzError> {
        let url = format!("{}/stores/{}/read", self.api_url, self.store_id);
        let mut tuples = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut body = serde_json::Map::new();
            body.insert("page_size".to_string(), serde_json::json!(100));
            if let Some(tuple_key) = filter {
                body.insert(
                    "tuple_key".to_string(),
                    serde_json::json!({
                        "user": tuple_key.user,
                        "relation": tuple_key.relation,
                        "object": tuple_key.object,
                    }),
                );
            }
            if let Some(token) = &continuation_token {
                body.insert(
                    "continuation_token".to_string(),
                    serde_json::Value::String(token.clone()),
                );
            }

            let response = self
                .with_auth_header(self.client.post(&url).json(&body))
                .send()
                .await?;
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

    /// Đọc tuple để operator đối chiếu; không dùng cho authorization ở request path.
    /// OpenFGA Read API là POST; body rỗng nghĩa là quét mọi tuple trong store (phân trang).
    pub async fn list_all_tuples(&self) -> Result<Vec<TupleKey>, AuthzError> {
        self.read_tuples(None).await
    }

    /// `POST /read` đã lọc đúng một `tuple_key` — không quét store.
    pub async fn has_tuple(&self, tuple: &TupleKey) -> Result<bool, AuthzError> {
        let matches = self.read_tuples(Some(tuple)).await?;
        Ok(matches.iter().any(|found| found == tuple))
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

/// Xoá từng tuple theo thứ tự fail-closed; tuple đã thiếu được coi là idempotent success.
///
/// Không batch ở đây: nếu một revoke lỗi, caller phải giữ SQL nguyên vẹn để có thể retry.
pub async fn delete_tuples_fga_first(
    authz_client: &AuthzClient,
    tuples: &[TupleKey],
) -> Result<(), AuthzError> {
    for tuple in tuples {
        match authz_client
            .write_tuples(Vec::new(), vec![tuple.clone()])
            .await
        {
            Ok(()) => {}
            Err(error) if is_missing_tuple_delete_error(&error) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

/// OpenFGA có thể báo lỗi khi revoke một tuple đã bị dọn bởi lần chạy trước.
pub fn is_missing_tuple_delete_error(error: &AuthzError) -> bool {
    let AuthzError::OpenFga { body, .. } = error else {
        return false;
    };
    let body = body.to_ascii_lowercase();
    body.contains("does not exist") || body.contains("not found")
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::net::TcpListener;
    use std::time::{Duration, Instant};

    /// Listener chấp nhận kết nối nhưng không bao giờ trả response — mô phỏng OpenFGA
    /// treo giữa chừng (khác với connection-refused: đây là hang thực sự sau khi connect).
    fn spawn_blackhole_openfga() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind blackhole listener");
        let addr = listener.local_addr().expect("blackhole addr");
        std::thread::spawn(move || {
            // Giữ mọi socket mở, đọc rồi lờ đi; không bao giờ ghi response.
            let mut held = Vec::new();
            for stream in listener.incoming() {
                match stream {
                    Ok(mut socket) => {
                        let mut scratch = [0u8; 1024];
                        let _ = socket.read(&mut scratch);
                        held.push(socket);
                    }
                    Err(_) => break,
                }
            }
        });
        format!("http://{addr}")
    }

    /// Client trỏ tới blackhole với request timeout 1s để test chạy nhanh và xác định.
    fn blackhole_client() -> AuthzClient {
        AuthzClient {
            client: crate::auth::build_auth_http_client(2, 1),
            api_url: spawn_blackhole_openfga(),
            store_id: "test-store".to_string(),
            model_id: None,
            api_token: None,
        }
    }

    /// Client tối thiểu chỉ để test `with_auth_header`; không gửi request thật.
    fn client_with_token(api_token: Option<&str>) -> AuthzClient {
        AuthzClient {
            client: reqwest::Client::new(),
            api_url: "http://127.0.0.1".to_string(),
            store_id: "test-store".to_string(),
            model_id: None,
            api_token: api_token.map(str::to_string),
        }
    }

    /// Không cấu hình token thì request phải đi nguyên trạng — đây là thứ giữ cho
    /// setup local cũ (OpenFGA authn=none) tiếp tục chạy sau thay đổi này.
    #[test]
    fn with_auth_header_is_noop_when_token_absent() {
        let authz = client_with_token(None);
        let request = authz
            .with_auth_header(authz.client.post("http://127.0.0.1/"))
            .build()
            .expect("build request");

        assert!(
            request
                .headers()
                .get(reqwest::header::AUTHORIZATION)
                .is_none(),
            "không cấu hình OPENFGA_API_TOKEN thì không được gắn Authorization header"
        );
    }

    /// Có token thì mọi outbound OpenFGA phải mang bearer — thiếu ở bất kỳ site nào
    /// sẽ thành 401 chỉ trên đúng một thao tác, rất khó truy.
    #[test]
    fn with_auth_header_sets_bearer_when_token_present() {
        let authz = client_with_token(Some("test-token"));
        let request = authz
            .with_auth_header(authz.client.post("http://127.0.0.1/"))
            .build()
            .expect("build request");

        let header = request
            .headers()
            .get(reqwest::header::AUTHORIZATION)
            .expect("phải có Authorization header khi token được cấu hình");
        assert_eq!(header, "Bearer test-token");
    }

    #[tokio::test]
    async fn check_fga_times_out_against_unresponsive_openfga() {
        let client = blackhole_client();

        let start = Instant::now();
        let result = client
            .check_fga("user:u1", Relation::Member, &Object::Workspace(Uuid::nil()))
            .await;
        let elapsed = start.elapsed();

        // Không được resolve thành một quyết định (true/false); phải là lỗi.
        assert!(
            result.is_err(),
            "unresponsive OpenFGA must surface an error, not a decision"
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "authz call must time out fast instead of blocking, took {elapsed:?}"
        );
        match result {
            Err(AuthzError::Http(err)) => {
                assert!(err.is_timeout(), "expected a timeout error, got: {err}");
            }
            other => panic!("expected an Http timeout error, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn authz_timeout_denies_instead_of_allowing() {
        let authz = Authz {
            client: blackhole_client(),
            user_id: "u1".to_string(),
            email: None,
            email_verified: false,
            cache: RequestAuthzCache::default(),
        };

        // `check` không bao giờ được trả Ok(true) khi dependency treo.
        let decision = authz
            .check(Relation::Member, &Object::Workspace(Uuid::nil()))
            .await;
        assert!(
            decision.is_err(),
            "timeout must surface as Err, never Ok(true)"
        );

        // `require_relation` phải fail-closed: 500 AUTHZ_ERROR, không cho qua.
        let required = authz
            .require_relation(Relation::Admin, &Object::Workspace(Uuid::nil()))
            .await;
        let error = required.expect_err("authz timeout must deny the request");
        assert_eq!(error.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(error.code, "AUTHZ_ERROR");
    }
}
