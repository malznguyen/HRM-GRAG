pub mod authz;
pub mod document_acl;
pub mod extractor;
pub mod jwt;
pub mod keycloak;
pub mod outbox;
pub mod outbox_health;
pub mod resource_cleanup;
pub mod workspace_role;

/// Trần an toàn chung cho mọi HTTP timeout của auth layer (giây) — chặn cấu hình
/// cực lớn tự biến thành treo pool (một authz check chậm cỡ này đã là outage).
const MAX_AUTH_HTTP_TIMEOUT_SECS: u64 = 30;

/// Đọc timeout (giây) từ env theo convention `*_TIMEOUT_SECS` (giống
/// `QDRANT_DELETE_*_TIMEOUT_SECS`): parse, fallback default, clamp về [1, trần].
pub(crate) fn auth_timeout_secs_from_env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
        .clamp(1, MAX_AUTH_HTTP_TIMEOUT_SECS)
}

/// Dựng reqwest client với connect + request timeout tường minh cho outbound của auth layer.
///
/// Panic ngay lúc boot nếu không dựng được client (chỉ xảy ra khi init TLS backend lỗi):
/// thà fail loud lúc khởi động còn hơn âm thầm chạy với client không timeout —
/// đúng tinh thần fail-closed, vì client không timeout là DoS tự gây, không log, không crash.
pub(crate) fn build_auth_http_client(connect_secs: u64, request_secs: u64) -> reqwest::Client {
    reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(connect_secs))
        .timeout(std::time::Duration::from_secs(request_secs))
        .build()
        .expect("failed to build auth-layer HTTP client with timeouts")
}

const TEST_BYPASS_FLAGS: [&str; 2] = ["TEST_BYPASS_JWT", "TEST_BYPASS_KEYCLOAK"];
const UNSAFE_SECRET_DEFAULTS: [(&str, &str); 5] = [
    ("KEYCLOAK_CLIENT_SECRET", "dummy_secret"),
    (
        "KEYCLOAK_CLIENT_SECRET",
        "replace_with_keycloak_admin_client_secret",
    ),
    ("S3_ACCESS_KEY_ID", "minioadmin"),
    ("S3_SECRET_ACCESS_KEY", "minioadmin"),
    // So sánh toàn chuỗi để không chặn nhầm API key thật tình cờ cùng prefix `sk-`.
    ("DEEPSEEK_API_KEY", "sk-your-deepseek-api-key"),
];

/// Chỉ cho phép bypass xác thực khi cờ được bật trong môi trường test tường minh.
pub fn test_bypass_enabled(flag_name: &str) -> bool {
    test_bypass_is_enabled(
        std::env::var_os(flag_name).is_some(),
        std::env::var("APP_ENV").ok().as_deref(),
    )
}

/// Từ chối khởi động nếu bypass xác thực được bật ngoài môi trường test.
pub fn validate_test_bypass_configuration() -> Result<(), TestBypassConfigurationError> {
    for flag_name in TEST_BYPASS_FLAGS {
        if let Some(error) = test_bypass_configuration_error(
            flag_name,
            std::env::var_os(flag_name).is_some(),
            std::env::var("APP_ENV").ok().as_deref(),
        ) {
            return Err(error);
        }
    }

    Ok(())
}

/// Từ chối placeholder credential khi không chạy test tường minh.
pub fn validate_secret_configuration() -> Result<(), SecretConfigurationError> {
    validate_secret_configuration_with(
        std::env::var("APP_ENV").ok().as_deref(),
        std::env::var("ALLOW_INSECURE_DEFAULTS").ok().as_deref(),
        |key| std::env::var(key).ok(),
    )
}

fn validate_secret_configuration_with<F>(
    app_env: Option<&str>,
    allow_insecure_defaults: Option<&str>,
    mut env_getter: F,
) -> Result<(), SecretConfigurationError>
where
    F: FnMut(&str) -> Option<String>,
{
    if app_env == Some("test") || allow_insecure_defaults == Some("1") {
        return Ok(());
    }

    for (key, unsafe_value) in UNSAFE_SECRET_DEFAULTS {
        if env_getter(key)
            .as_deref()
            .is_some_and(|value| value.trim() == unsafe_value)
        {
            return Err(SecretConfigurationError { key });
        }
    }

    // Đây là kiểm tra substring đơn giản cho password placeholder, không phải URL parser đầy đủ.
    if env_getter("DATABASE_URL").is_some_and(|database_url| database_url.contains("change_me")) {
        return Err(SecretConfigurationError {
            key: "DATABASE_URL",
        });
    }

    Ok(())
}

/// Lỗi credential placeholder không được phép ngoài test.
#[derive(Debug, PartialEq, Eq)]
pub struct SecretConfigurationError {
    key: &'static str,
}

impl std::fmt::Display for SecretConfigurationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{0} uses an unsafe placeholder value; set a real credential, APP_ENV=test, or ALLOW_INSECURE_DEFAULTS=1 for a local demo only",
            self.key
        )
    }
}

impl std::error::Error for SecretConfigurationError {}

/// Lỗi cấu hình bypass xác thực không an toàn.
#[derive(Debug, PartialEq, Eq)]
pub struct TestBypassConfigurationError {
    flag_name: &'static str,
}

impl std::fmt::Display for TestBypassConfigurationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{0} requires APP_ENV=test; test authentication bypasses are disabled outside the test environment",
            self.flag_name
        )
    }
}

impl std::error::Error for TestBypassConfigurationError {}

fn test_bypass_is_enabled(flag_is_set: bool, app_env: Option<&str>) -> bool {
    flag_is_set && app_env == Some("test")
}

fn test_bypass_configuration_error(
    flag_name: &'static str,
    flag_is_set: bool,
    app_env: Option<&str>,
) -> Option<TestBypassConfigurationError> {
    (flag_is_set && !test_bypass_is_enabled(flag_is_set, app_env))
        .then_some(TestBypassConfigurationError { flag_name })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bypass_requires_a_flag_and_the_exact_test_environment() {
        assert!(!test_bypass_is_enabled(false, Some("test")));

        for app_env in [
            None,
            Some(""),
            Some("development"),
            Some("staging"),
            Some("production"),
            Some("Test"),
        ] {
            assert!(!test_bypass_is_enabled(true, app_env));
        }

        assert!(test_bypass_is_enabled(true, Some("test")));
    }

    #[test]
    fn test_bypass_predicate_applies_to_jwt_and_keycloak_flags() {
        for flag_name in TEST_BYPASS_FLAGS {
            assert!(test_bypass_configuration_error(flag_name, false, None).is_none());
            assert!(test_bypass_configuration_error(flag_name, true, Some("test")).is_none());

            for app_env in [
                None,
                Some(""),
                Some("development"),
                Some("staging"),
                Some("production"),
            ] {
                assert_eq!(
                    test_bypass_configuration_error(flag_name, true, app_env),
                    Some(TestBypassConfigurationError { flag_name })
                );
            }
        }
    }

    #[test]
    fn unsafe_secret_defaults_fail_outside_test_only() {
        for (key, value) in UNSAFE_SECRET_DEFAULTS {
            let error = validate_secret_configuration_with(Some("production"), None, |candidate| {
                (candidate == key).then(|| value.to_string())
            });
            assert_eq!(error, Err(SecretConfigurationError { key }));

            let test_result = validate_secret_configuration_with(Some("test"), None, |candidate| {
                (candidate == key).then(|| value.to_string())
            });
            assert_eq!(test_result, Ok(()));

            let local_result =
                validate_secret_configuration_with(Some("development"), Some("1"), |candidate| {
                    (candidate == key).then(|| value.to_string())
                });
            assert_eq!(local_result, Ok(()));
        }
    }

    #[test]
    fn database_url_placeholder_fails_outside_test_without_opt_in() {
        let database_url = "postgres://gmrag_user:change_me@127.0.0.1:5432/gmrag";
        let rejected = validate_secret_configuration_with(Some("development"), None, |key| {
            (key == "DATABASE_URL").then(|| database_url.to_string())
        });
        assert_eq!(
            rejected,
            Err(SecretConfigurationError {
                key: "DATABASE_URL"
            })
        );
    }
}
