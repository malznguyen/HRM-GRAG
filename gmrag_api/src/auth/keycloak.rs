use reqwest::Client;
use serde::Deserialize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

use crate::auth::test_bypass_enabled;
use crate::invite::normalize_email;

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
    pub enabled: Option<bool>,
}

impl KeycloakClient {
    pub fn from_env() -> Result<Self, String> {
        if test_bypass_enabled("TEST_BYPASS_KEYCLOAK") {
            return Ok(Self {
                client: Client::new(),
                admin_url: "http://test-bypass-keycloak".to_string(),
                realm: "test".to_string(),
                client_id: "test".to_string(),
                client_secret: "test".to_string(),
                token_cache: Arc::new(RwLock::new(None)),
            });
        }

        let admin_url = required_admin_url()?;
        let realm = required_env("KEYCLOAK_REALM")?;
        let client_id = required_env("KEYCLOAK_CLIENT_ID")?;
        let client_secret = required_env("KEYCLOAK_CLIENT_SECRET")?;
        if client_secret == "dummy_secret" {
            return Err("KEYCLOAK_CLIENT_SECRET must not use dummy_secret".to_string());
        }

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
            let bypass_user: Option<(String, String)> = match email {
                "verified-owner@test.com" => Some((
                    "verified-keycloak-owner-uuid".to_string(),
                    email.to_string(),
                )),
                "member@test.com" => {
                    Some(("test-workspace-member-id".to_string(), email.to_string()))
                }
                "new_member@test.com" => {
                    Some(("test-new-member-id".to_string(), email.to_string()))
                }
                "admin@test.com" => {
                    Some(("test-workspace-admin-id".to_string(), email.to_string()))
                }
                "unverified-owner@test.com" => None,
                // Phase 0B: local-part@phase0b.test → user id phase0b-{local-part}
                other if other.ends_with("@phase0b.test") => {
                    let local = other.trim_end_matches("@phase0b.test");
                    if local.is_empty() {
                        None
                    } else {
                        Some((format!("phase0b-{local}"), other.to_string()))
                    }
                }
                _ => None,
            };

            return Ok(bypass_user.map(|(id, email)| KeycloakUser {
                id,
                email: Some(email),
                email_verified: Some(true),
                enabled: Some(true),
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

        let expected_email = normalize_email(email);
        let verified_user = users.into_iter().find(|user| {
            user.email_verified.unwrap_or(false)
                && user
                    .email
                    .as_deref()
                    .is_some_and(|returned_email| normalize_email(returned_email) == expected_email)
        });
        Ok(verified_user)
    }

    /// Lấy user theo Keycloak `sub` và chỉ chấp nhận tài khoản có email đã verify.
    pub async fn get_verified_user_by_id(
        &self,
        user_id: &str,
    ) -> Result<Option<KeycloakUser>, reqwest::Error> {
        Ok(self
            .get_user_by_id(user_id)
            .await?
            .filter(|user| user.email_verified.unwrap_or(false) && user.enabled.unwrap_or(true)))
    }

    /// Lấy user thô theo exact id để operator kiểm tra trạng thái identity.
    pub async fn get_user_by_id(
        &self,
        user_id: &str,
    ) -> Result<Option<KeycloakUser>, reqwest::Error> {
        if test_bypass_enabled("TEST_BYPASS_KEYCLOAK") {
            return Ok(Some(KeycloakUser {
                id: user_id.to_string(),
                email: None,
                email_verified: Some(true),
                enabled: Some(true),
            }));
        }

        let token = self.get_token().await?;
        let url = format!(
            "{}/admin/realms/{}/users/{}",
            self.admin_url, self.realm, user_id
        );
        let response = self.client.get(&url).bearer_auth(token).send().await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        response
            .error_for_status()?
            .json::<KeycloakUser>()
            .await
            .map(Some)
    }

    /// Trả về mọi account Keycloak có email exact để phát hiện identity trùng.
    pub async fn get_users_by_email_exact(
        &self,
        email: &str,
    ) -> Result<Vec<KeycloakUser>, reqwest::Error> {
        if test_bypass_enabled("TEST_BYPASS_KEYCLOAK") {
            return Ok(Vec::new());
        }

        let token = self.get_token().await?;
        let url = format!("{}/admin/realms/{}/users", self.admin_url, self.realm);
        self.client
            .get(&url)
            .bearer_auth(token)
            .query(&[("email", email), ("exact", "true")])
            .send()
            .await?
            .error_for_status()?
            .json::<Vec<KeycloakUser>>()
            .await
    }
}

fn required_env(name: &str) -> Result<String, String> {
    let value = std::env::var(name).map_err(|_| format!("{name} must be set"))?;
    let value = value.trim();
    (!value.is_empty())
        .then(|| value.to_string())
        .ok_or_else(|| format!("{name} must be set"))
}

fn required_admin_url() -> Result<String, String> {
    let value = required_env("KEYCLOAK_ADMIN_URL")?;
    let url = reqwest::Url::parse(&value)
        .map_err(|_| "KEYCLOAK_ADMIN_URL must be a valid HTTP URL".to_string())?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err("KEYCLOAK_ADMIN_URL must be a valid HTTP URL".to_string());
    }
    Ok(value.trim_end_matches('/').to_string())
}
