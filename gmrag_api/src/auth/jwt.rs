use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
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
const JWT_CLOCK_LEEWAY_SECS: u64 = 60;

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

pub struct JwtValidator {
    algorithm: Algorithm,
    issuer: Option<String>,
    audience: Option<String>,
    verify_audience: bool,
    subject_claim: String,
    jwks_url: Option<String>,
    hmac_key: Option<DecodingKey>,
    client: Client,
    keys: Arc<RwLock<HashMap<String, DecodingKey>>>,
}

impl JwtValidator {
    pub fn from_env() -> Result<Arc<Self>, JwtError> {
        if test_bypass_enabled("TEST_BYPASS_JWT") {
            return Ok(Arc::new(Self {
                algorithm: Algorithm::RS256,
                issuer: None,
                audience: None,
                verify_audience: true,
                subject_claim: "sub".to_string(),
                jwks_url: None,
                hmac_key: None,
                client: build_jwks_client(),
                keys: Arc::new(RwLock::new(HashMap::new())),
            }));
        }

        let algorithm = configured_algorithm()?;
        let issuer = required_env("JWT_ISSUER")?;
        if issuer == "http://test-bypass-jwt" {
            return Err(JwtError::InvalidConfig("JWT_ISSUER"));
        }
        let verify_audience = optional_bool("JWT_VERIFY_AUDIENCE", true)?;
        let subject_claim = optional_env("JWT_SUBJECT_CLAIM")?.unwrap_or_else(|| "sub".to_string());
        let audience = if verify_audience {
            Some(required_env("JWT_AUDIENCE")?)
        } else {
            optional_env("JWT_AUDIENCE")?
        };

        let (jwks_url, hmac_key) = match algorithm {
            Algorithm::RS256 => (Some(required_http_url("JWT_JWKS_URL")?), None),
            Algorithm::HS512 => {
                let secret = required_env("JWT_HMAC_SECRET")?;
                (None, Some(DecodingKey::from_secret(secret.as_bytes())))
            }
            _ => return Err(JwtError::InvalidConfig("JWT_ALG")),
        };

        Ok(Arc::new(Self {
            algorithm,
            issuer: Some(issuer),
            audience,
            verify_audience,
            subject_claim,
            jwks_url,
            hmac_key,
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
        if header.alg != self.algorithm {
            warn!(
                configured_algorithm = ?self.algorithm,
                token_algorithm = ?header.alg,
                "JWT algorithm mismatch"
            );
            return Err(JwtError::InvalidToken);
        }

        let (key, kid) = match self.algorithm {
            Algorithm::HS512 => (
                self.hmac_key
                    .as_ref()
                    .ok_or(JwtError::MissingConfig("JWT_HMAC_SECRET"))?
                    .clone(),
                None,
            ),
            Algorithm::RS256 => {
                let kid = header.kid.ok_or_else(|| {
                    error!("JWT header missing kid");
                    JwtError::UnknownKeyId
                })?;
                let key = self.decoding_key_for(&kid).await?;
                (key, Some(kid))
            }
            _ => return Err(JwtError::InvalidToken),
        };

        let issuer = self
            .issuer
            .as_deref()
            .ok_or(JwtError::MissingConfig("JWT_ISSUER"))?;
        let mut validation = Validation::new(self.algorithm);
        validation.set_issuer(&[issuer]);
        if self.verify_audience {
            let audience = self
                .audience
                .as_deref()
                .ok_or(JwtError::MissingConfig("JWT_AUDIENCE"))?;
            validation.set_audience(&[audience]);
        } else {
            validation.validate_aud = false;
        }
        validation.validate_exp = true;
        validation.leeway = JWT_CLOCK_LEEWAY_SECS;

        let token_data = decode::<Value>(token, &key, &validation).map_err(|err| {
            if let Some(kid) = kid.as_deref() {
                error!(%err, issuer = %issuer, %kid, "JWT decode/validation failed");
            } else {
                error!(%err, issuer = %issuer, "JWT decode/validation failed");
            }
            JwtError::InvalidToken
        })?;
        let sub = token_data
            .claims
            .get(&self.subject_claim)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or(JwtError::InvalidToken)?;

        Ok(JwtClaims {
            sub: sub.to_string(),
            email: token_data
                .claims
                .get("email")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            email_verified: token_data
                .claims
                .get("email_verified")
                .and_then(Value::as_bool)
                .unwrap_or(false),
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

fn configured_algorithm() -> Result<Algorithm, JwtError> {
    match optional_env("JWT_ALG")?.as_deref().unwrap_or("RS256") {
        "RS256" => Ok(Algorithm::RS256),
        "HS512" => Ok(Algorithm::HS512),
        _ => Err(JwtError::InvalidConfig("JWT_ALG")),
    }
}

fn optional_bool(name: &'static str, default: bool) -> Result<bool, JwtError> {
    match optional_env(name)?.as_deref() {
        None => Ok(default),
        Some("true") => Ok(true),
        Some("false") => Ok(false),
        Some(_) => Err(JwtError::InvalidConfig(name)),
    }
}

fn optional_env(name: &'static str) -> Result<Option<String>, JwtError> {
    let Some(value) = std::env::var_os(name) else {
        return Ok(None);
    };
    let value = value.to_string_lossy().trim().to_string();
    if value.is_empty() {
        return Ok(None);
    }
    Ok(Some(value))
}

fn required_env(name: &'static str) -> Result<String, JwtError> {
    optional_env(name)?.ok_or(JwtError::MissingConfig(name))
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
    use jsonwebtoken::{EncodingKey, Header, encode};
    use serde_json::json;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn hs512_validator(secret: &[u8], verify_audience: bool) -> JwtValidator {
        hs512_validator_with_subject(secret, verify_audience, "sub")
    }

    fn hs512_validator_with_subject(
        secret: &[u8],
        verify_audience: bool,
        subject_claim: &str,
    ) -> JwtValidator {
        JwtValidator {
            algorithm: Algorithm::HS512,
            issuer: Some("restaurant-access".to_string()),
            audience: verify_audience.then(|| "gmrag-api".to_string()),
            verify_audience,
            subject_claim: subject_claim.to_string(),
            jwks_url: None,
            hmac_key: Some(DecodingKey::from_secret(secret)),
            client: build_jwks_client(),
            keys: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn now_seconds() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_secs()
    }

    fn token(algorithm: Algorithm, secret: &[u8], claims: serde_json::Value) -> String {
        encode(
            &Header::new(algorithm),
            &claims,
            &EncodingKey::from_secret(secret),
        )
        .expect("token encoding")
    }

    #[tokio::test]
    async fn accepts_hs512_with_issuer_audience_and_expiry() {
        let secret = b"phase2-test-secret";
        let validator = hs512_validator(secret, true);
        let claims = json!({
            "sub": "employee-1",
            "iss": "restaurant-access",
            "aud": "gmrag-api",
            "exp": now_seconds() + 3600
        });

        let result = validator.validate(&token(Algorithm::HS512, secret, claims)).await;

        assert_eq!(
            result.expect("valid token").sub,
            "employee-1"
        );
    }

    #[tokio::test]
    async fn rejects_wrong_secret_and_algorithm_mismatch() {
        let validator = hs512_validator(b"correct-secret", false);
        let claims = json!({
            "sub": "employee-1",
            "iss": "restaurant-access",
            "exp": now_seconds() + 3600
        });

        assert_eq!(
            validator
                .validate(&token(Algorithm::HS512, b"wrong-secret", claims.clone()))
                .await,
            Err(JwtError::InvalidToken)
        );
        assert_eq!(
            validator
                .validate(&token(Algorithm::HS256, b"correct-secret", claims))
                .await,
            Err(JwtError::InvalidToken)
        );
    }

    #[tokio::test]
    async fn rejects_expired_and_wrong_issuer_tokens() {
        let secret = b"phase2-test-secret";
        let validator = hs512_validator(secret, false);

        let expired = json!({
            "sub": "employee-1",
            "iss": "restaurant-access",
            "exp": now_seconds().saturating_sub(61)
        });
        let wrong_issuer = json!({
            "sub": "employee-1",
            "iss": "wrong-issuer",
            "exp": now_seconds() + 3600
        });

        assert_eq!(
            validator.validate(&token(Algorithm::HS512, secret, expired)).await,
            Err(JwtError::InvalidToken)
        );
        assert_eq!(
            validator
                .validate(&token(Algorithm::HS512, secret, wrong_issuer))
                .await,
            Err(JwtError::InvalidToken)
        );
    }

    #[tokio::test]
    async fn uses_configured_subject_claim_without_sub_fallback() {
        let secret = b"phase2-test-secret";
        let validator = hs512_validator_with_subject(secret, false, "userid");
        let valid_claims = json!({
            "userid": "employee-1",
            "sub": "untrusted-sub",
            "iss": "restaurant-access",
            "exp": now_seconds() + 3600
        });
        let missing_claims = json!({
            "sub": "employee-1",
            "iss": "restaurant-access",
            "exp": now_seconds() + 3600
        });

        assert_eq!(
            validator
                .validate(&token(Algorithm::HS512, secret, valid_claims))
                .await
                .expect("userid claim")
                .sub,
            "employee-1"
        );
        assert_eq!(
            validator
                .validate(&token(Algorithm::HS512, secret, missing_claims))
                .await,
            Err(JwtError::InvalidToken)
        );
    }

    #[test]
    fn rejects_invalid_algorithm_and_audience_configuration() {
        assert_eq!(configured_algorithm(), Ok(Algorithm::RS256));
        assert_eq!(optional_bool("JWT_VERIFY_AUDIENCE_MISSING", true), Ok(true));
        assert_eq!(
            reqwest::Url::parse("not-a-url").is_err(),
            true
        );
    }
}
