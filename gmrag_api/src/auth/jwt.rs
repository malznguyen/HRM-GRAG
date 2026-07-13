use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, warn};

use crate::auth::test_bypass_enabled;

/// Connect timeout mặc định khi fetch JWKS (giây). JWKS fetch nằm trên request path
/// của mọi token chưa cache key; IdP treo không được kéo theo token validation treo.
const DEFAULT_JWKS_CONNECT_TIMEOUT_SECS: u64 = 3;
/// Request timeout mặc định khi fetch JWKS (giây). Ít khi chạy (key được cache) nhưng
/// vẫn phải bounded để không treo request đầu tiên sau khi key xoay vòng.
const DEFAULT_JWKS_REQUEST_TIMEOUT_SECS: u64 = 5;

/// Dựng HTTP client cho JWKS fetch với connect + request timeout (đọc từ env, có default).
fn build_jwks_client() -> Client {
    let connect_timeout_secs = crate::auth::auth_timeout_secs_from_env(
        "JWT_JWKS_CONNECT_TIMEOUT_SECS",
        DEFAULT_JWKS_CONNECT_TIMEOUT_SECS,
    );
    let request_timeout_secs = crate::auth::auth_timeout_secs_from_env(
        "JWT_JWKS_REQUEST_TIMEOUT_SECS",
        DEFAULT_JWKS_REQUEST_TIMEOUT_SECS,
    );
    crate::auth::build_auth_http_client(connect_timeout_secs, request_timeout_secs)
}

#[derive(Debug, PartialEq, Eq)]
pub enum JwtError {
    MissingConfig(&'static str),
    InvalidConfig(&'static str),
    FetchJwks,
    InvalidJwks,
    InvalidToken,
    UnknownKeyId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JwtClaims {
    pub sub: String,
    pub email: Option<String>,
    pub email_verified: bool,
}

#[derive(Deserialize)]
struct JwksResponse {
    keys: Vec<Jwk>,
}

#[derive(Deserialize)]
struct Jwk {
    kid: String,
    kty: String,
    #[serde(rename = "use")]
    use_: Option<String>,
    n: Option<String>,
    e: Option<String>,
}

#[derive(Deserialize)]
struct OidcClaims {
    sub: String,
    email: Option<String>,
    email_verified: Option<bool>,
}

pub struct JwtValidator {
    issuer: Option<String>,
    audience: Option<String>,
    jwks_url: Option<String>,
    client: Client,
    keys: Arc<RwLock<HashMap<String, DecodingKey>>>,
}

impl JwtValidator {
    pub fn from_env() -> Result<Arc<Self>, JwtError> {
        if test_bypass_enabled("TEST_BYPASS_JWT") {
            return Ok(Arc::new(Self {
                issuer: None,
                audience: None,
                jwks_url: None,
                client: build_jwks_client(),
                keys: Arc::new(RwLock::new(HashMap::new())),
            }));
        }

        let issuer = required_env("JWT_ISSUER")?;
        if issuer == "http://test-bypass-jwt" {
            return Err(JwtError::InvalidConfig("JWT_ISSUER"));
        }
        let audience = required_env("JWT_AUDIENCE")?;
        let jwks_url = required_http_url("JWT_JWKS_URL")?;

        Ok(Arc::new(Self {
            issuer: Some(issuer),
            audience: Some(audience),
            jwks_url: Some(jwks_url),
            client: build_jwks_client(),
            keys: Arc::new(RwLock::new(HashMap::new())),
        }))
    }

    pub async fn validate(&self, token: &str) -> Result<JwtClaims, JwtError> {
        if test_bypass_enabled("TEST_BYPASS_JWT") {
            let sub = token.trim();
            if sub.is_empty() {
                return Err(JwtError::InvalidToken);
            }
            return Ok(JwtClaims {
                sub: sub.to_string(),
                email: None,
                email_verified: false,
            });
        }

        let header = decode_header(token).map_err(|err| {
            error!(%err, "JWT header decode failed");
            JwtError::InvalidToken
        })?;
        let kid = header.kid.ok_or_else(|| {
            error!("JWT header missing kid");
            JwtError::UnknownKeyId
        })?;
        let key = self.decoding_key_for(&kid).await?;

        let issuer = self
            .issuer
            .as_deref()
            .ok_or(JwtError::MissingConfig("JWT_ISSUER"))?;
        let audience = self
            .audience
            .as_deref()
            .ok_or(JwtError::MissingConfig("JWT_AUDIENCE"))?;
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&[issuer]);
        validation.set_audience(&[audience]);
        validation.validate_exp = true;

        let token_data = decode::<OidcClaims>(token, &key, &validation).map_err(|err| {
            error!(%err, issuer = %issuer, kid = %kid, "JWT decode/validation failed");
            JwtError::InvalidToken
        })?;
        let sub = token_data.claims.sub.trim();
        if sub.is_empty() {
            return Err(JwtError::InvalidToken);
        }

        Ok(JwtClaims {
            sub: sub.to_string(),
            email: token_data.claims.email,
            email_verified: token_data.claims.email_verified.unwrap_or(false),
        })
    }

    async fn decoding_key_for(&self, kid: &str) -> Result<DecodingKey, JwtError> {
        {
            let cache = self.keys.read().await;
            if let Some(key) = cache.get(kid) {
                return Ok(key.clone());
            }
        }

        self.refresh_jwks().await?;
        let cache = self.keys.read().await;
        cache.get(kid).cloned().ok_or_else(|| {
            warn!(%kid, "JWT kid not found in JWKS cache after refresh");
            JwtError::UnknownKeyId
        })
    }

    async fn refresh_jwks(&self) -> Result<(), JwtError> {
        let jwks_url = self
            .jwks_url
            .as_deref()
            .ok_or(JwtError::MissingConfig("JWT_JWKS_URL"))?;
        let response: JwksResponse = self
            .client
            .get(jwks_url)
            .send()
            .await
            .map_err(|err| {
                error!(%err, url = %jwks_url, "JWKS fetch request failed");
                JwtError::FetchJwks
            })?
            .error_for_status()
            .map_err(|err| {
                error!(%err, url = %jwks_url, "JWKS fetch returned error status");
                JwtError::FetchJwks
            })?
            .json()
            .await
            .map_err(|err| {
                error!(%err, url = %jwks_url, "JWKS response JSON parse failed");
                JwtError::FetchJwks
            })?;

        let mut next = HashMap::new();
        for jwk in response.keys {
            if jwk.kty != "RSA" || jwk.use_.as_deref() == Some("enc") {
                continue;
            }
            let (Some(n), Some(e)) = (jwk.n, jwk.e) else {
                continue;
            };
            let key = DecodingKey::from_rsa_components(&n, &e).map_err(|err| {
                error!(%err, kid = %jwk.kid, "Failed to build RSA decoding key from JWKS entry");
                JwtError::InvalidJwks
            })?;
            next.insert(jwk.kid, key);
        }

        if next.is_empty() {
            error!(url = %jwks_url, "JWKS response contained no usable RSA signing keys");
            return Err(JwtError::InvalidJwks);
        }
        *self.keys.write().await = next;
        Ok(())
    }
}

fn required_env(name: &'static str) -> Result<String, JwtError> {
    let value = std::env::var(name).map_err(|_| JwtError::MissingConfig(name))?;
    let value = value.trim();
    (!value.is_empty())
        .then(|| value.trim_end_matches('/').to_string())
        .ok_or(JwtError::MissingConfig(name))
}

fn required_http_url(name: &'static str) -> Result<String, JwtError> {
    let value = required_env(name)?;
    let url = reqwest::Url::parse(&value).map_err(|_| JwtError::InvalidConfig(name))?;
    matches!(url.scheme(), "http" | "https")
        .then_some(value)
        .ok_or(JwtError::InvalidConfig(name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_missing_or_unsafe_runtime_jwt_configuration() {
        assert_eq!(
            required_http_url("JWT_JWKS_URL_MISSING"),
            Err(JwtError::MissingConfig("JWT_JWKS_URL_MISSING"))
        );
        assert_eq!(reqwest::Url::parse("not-a-url").is_err(), true);
    }
}
