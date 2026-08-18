use sqlx::PgPool;
use std::sync::Arc;

use crate::admission::ChatAdmission;
use crate::auth::authz::AuthzClient;
use crate::auth::jwt::JwtValidator;
use crate::auth::keycloak::KeycloakClient;
use crate::retrieval::RetrievalClient;
use crate::storage::StorageClient;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub jwt: Arc<JwtValidator>,
    pub storage: StorageClient,
    pub retrieval: RetrievalClient,
    /// Giới hạn số chat xử lý đồng thời. Xem [`crate::admission`].
    pub chat_admission: ChatAdmission,
    pub authz_client: AuthzClient,
    pub keycloak_client: KeycloakClient,
}
