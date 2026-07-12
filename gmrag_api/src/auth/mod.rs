pub mod authz;
pub mod document_acl;
pub mod extractor;
pub mod jwt;
pub mod keycloak;
pub mod outbox;
pub mod outbox_health;
pub mod workspace_role;

const TEST_BYPASS_FLAGS: [&str; 2] = ["TEST_BYPASS_JWT", "TEST_BYPASS_KEYCLOAK"];

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
}
