use reqwest::Client;
use serde::Deserialize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::error;

/// Client tương tác với Keycloak Admin REST API
#[derive(Clone)]
pub struct KeycloakClient {
    client: Client,
    admin_url: String,
    realm: String,
    client_id: String,
    client_secret: String,
    token_cache: Arc<RwLock<Option<(String, Instant)>>>,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: u64,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct KeycloakUser {
    pub id: String,
    pub email: Option<String>,
    pub email_verified: Option<bool>,
}

impl KeycloakClient {
    pub fn from_env() -> Result<Self, &'static str> {
        let admin_url = std::env::var("KEYCLOAK_ADMIN_URL")
            .unwrap_or_else(|_| "http://localhost:8080".to_string());
        let realm = std::env::var("KEYCLOAK_REALM").unwrap_or_else(|_| "gmrag".to_string());
        let client_id = std::env::var("KEYCLOAK_CLIENT_ID")
            .unwrap_or_else(|_| "gmrag-admin-client".to_string());
        let client_secret =
            std::env::var("KEYCLOAK_CLIENT_SECRET").unwrap_or_else(|_| "dummy_secret".to_string());

        Ok(Self {
            client: Client::new(),
            admin_url: admin_url.trim_end_matches('/').to_string(),
            realm,
            client_id,
            client_secret,
            token_cache: Arc::new(RwLock::new(None)),
        })
    }

    /// Lấy Access Token từ Keycloak Client Credentials flow
    async fn get_token(&self) -> Result<String, reqwest::Error> {
        {
            let cache = self.token_cache.read().await;
            if let Some((token, expiry)) = cache.as_ref() {
                if Instant::now() < *expiry {
                    return Ok(token.clone());
                }
            }
        }

        let mut cache = self.token_cache.write().await;
        if let Some((token, expiry)) = cache.as_ref() {
            if Instant::now() < *expiry {
                return Ok(token.clone());
            }
        }

        let token_url = format!(
            "{}/realms/{}/protocol/openid-connect/token",
            self.admin_url, self.realm
        );
        let params = [
            ("grant_type", "client_credentials"),
            ("client_id", &self.client_id),
            ("client_secret", &self.client_secret),
        ];

        let resp: TokenResponse = self
            .client
            .post(&token_url)
            .form(&params)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        let expiry = Instant::now() + Duration::from_secs(resp.expires_in.saturating_sub(10));
        *cache = Some((resp.access_token.clone(), expiry));

        Ok(resp.access_token)
    }

    /// Tìm kiếm và lấy user theo email đã verify từ Keycloak
    pub async fn get_verified_user_by_email(
        &self,
        email: &str,
    ) -> Result<Option<KeycloakUser>, reqwest::Error> {
        if test_bypass_enabled("TEST_BYPASS_KEYCLOAK") {
            // Map email cố định cho integration test (tenant owner + member add)
            let bypass_user = match email {
                "verified-owner@test.com" => Some(("verified-keycloak-owner-uuid", email)),
                "member@test.com" => Some(("test-workspace-member-id", email)),
                "new_member@test.com" => Some(("test-new-member-id", email)),
                "admin@test.com" => Some(("test-workspace-admin-id", email)),
                "unverified-owner@test.com" => None,
                _ => None,
            };

            return Ok(bypass_user.map(|(id, email)| KeycloakUser {
                id: id.to_string(),
                email: Some(email.to_string()),
                email_verified: Some(true),
            }));
        }

        let token = self.get_token().await?;
        let url = format!("{}/admin/realms/{}/users", self.admin_url, self.realm);

        let users: Vec<KeycloakUser> = self
            .client
            .get(&url)
            .bearer_auth(token)
            .query(&[("email", email), ("exact", "true")])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        // Tìm user đầu tiên có email_verified = true
        let verified_user = users
            .into_iter()
            .find(|u| u.email_verified.unwrap_or(false));
        Ok(verified_user)
    }
}

fn test_bypass_enabled(flag_name: &str) -> bool {
    if std::env::var_os(flag_name).is_none() {
        return false;
    }

    if std::env::var("APP_ENV")
        .ok()
        .is_some_and(|value| value.eq_ignore_ascii_case("production"))
    {
        error!(
            flag = flag_name,
            "Test bypass flag is blocked in production APP_ENV"
        );
        return false;
    }

    true
}
