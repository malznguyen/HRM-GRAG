use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, warn};

use crate::auth::test_bypass_enabled;

#[derive(Debug)]
pub enum JwtError {
    MissingConfig,
    FetchJwks,
    InvalidJwks,
    InvalidToken,
    UnknownKeyId,
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
struct ClerkClaims {
    sub: String,
}

pub struct JwtValidator {
    issuer: String,
    jwks_url: String,
    client: Client,
    keys: Arc<RwLock<HashMap<String, DecodingKey>>>,
}

impl JwtValidator {
    pub fn from_env() -> Result<Arc<Self>, JwtError> {
        let issuer =
            std::env::var("CLERK_ISSUER").unwrap_or_else(|_| "http://test-bypass-jwt".to_string());
        let issuer = issuer.trim_end_matches('/').to_string();
        let jwks_url = format!("{issuer}/.well-known/jwks.json");

        Ok(Arc::new(Self {
            issuer,
            jwks_url,
            client: Client::new(),
            keys: Arc::new(RwLock::new(HashMap::new())),
        }))
    }

    pub async fn validate(&self, token: &str) -> Result<String, JwtError> {
        if test_bypass_enabled("TEST_BYPASS_JWT") {
            return Ok(token.to_string());
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

        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&[&self.issuer]);
        validation.validate_exp = true;
        // Clerk session tokens use `azp`, not `aud`. jsonwebtoken rejects any token
        // that carries `aud` when validate_aud is true but no audience is configured.
        validation.validate_aud = false;

        let token_data = decode::<ClerkClaims>(token, &key, &validation).map_err(|err| {
            error!(%err, issuer = %self.issuer, kid = %kid, "JWT decode/validation failed");
            JwtError::InvalidToken
        })?;

        Ok(token_data.claims.sub)
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
        let response: JwksResponse = self
            .client
            .get(&self.jwks_url)
            .send()
            .await
            .map_err(|err| {
                error!(%err, url = %self.jwks_url, "JWKS fetch request failed");
                JwtError::FetchJwks
            })?
            .error_for_status()
            .map_err(|err| {
                error!(%err, url = %self.jwks_url, "JWKS fetch returned error status");
                JwtError::FetchJwks
            })?
            .json()
            .await
            .map_err(|err| {
                error!(%err, url = %self.jwks_url, "JWKS response JSON parse failed");
                JwtError::FetchJwks
            })?;

        let mut next = HashMap::new();
        for jwk in response.keys {
            if jwk.kty != "RSA" {
                continue;
            }
            if jwk.use_.as_deref() == Some("enc") {
                continue;
            }
            let (Some(n), Some(e)) = (jwk.n, jwk.e) else {
                continue;
            };
            let key = rsa_decoding_key(&n, &e).map_err(|err| {
                error!(%err, kid = %jwk.kid, "Failed to build RSA decoding key from JWKS entry");
                JwtError::InvalidJwks
            })?;
            next.insert(jwk.kid, key);
        }

        if next.is_empty() {
            error!(url = %self.jwks_url, "JWKS response contained no usable RSA signing keys");
            return Err(JwtError::InvalidJwks);
        }

        let mut cache = self.keys.write().await;
        *cache = next;
        Ok(())
    }
}

fn rsa_decoding_key(n: &str, e: &str) -> Result<DecodingKey, jsonwebtoken::errors::Error> {
    // JWK `n`/`e` are base64url-encoded; jsonwebtoken expects the same encoding.
    DecodingKey::from_rsa_components(n, e)
}
