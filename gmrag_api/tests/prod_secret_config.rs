use std::process::Command;

fn run_api(app_env: &str, allow: Option<&str>, overrides: &[(&str, &str)]) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_gmrag_api"));
    command
        .env("APP_ENV", app_env)
        .env("S3_ACCESS_KEY_ID", "safe-test-access-key")
        .env("S3_SECRET_ACCESS_KEY", "safe-test-secret")
        .env(
            "DATABASE_URL",
            "postgres://gmrag_user:safe-test-password@127.0.0.1:0/gmrag",
        )
        // Không để dotenv nạp cờ local demo từ `.env` của workspace vào child process.
        .env("ALLOW_INSECURE_DEFAULTS", "0");
    if let Some(value) = allow {
        command.env("ALLOW_INSECURE_DEFAULTS", value);
    }
    for (key, value) in overrides {
        command.env(key, value);
    }
    command.output().expect("API child process should start")
}

fn stderr(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn secret_guard_requires_explicit_local_opt_in_outside_test() {
    let production = run_api("production", None, &[("S3_ACCESS_KEY_ID", "minioadmin")]);
    assert!(stderr(&production).contains("S3_ACCESS_KEY_ID uses an unsafe placeholder value"));
    let test = run_api("test", None, &[("S3_ACCESS_KEY_ID", "minioadmin")]);
    assert!(!stderr(&test).contains("Invalid secret configuration"));
    let opted_in = run_api(
        "development",
        Some("1"),
        &[("S3_ACCESS_KEY_ID", "minioadmin")],
    );
    assert!(!stderr(&opted_in).contains("Invalid secret configuration"));
    let development = run_api("development", None, &[("S3_ACCESS_KEY_ID", "minioadmin")]);
    assert!(stderr(&development).contains("S3_ACCESS_KEY_ID uses an unsafe placeholder value"));
}

#[test]
fn keycloak_and_database_placeholders_require_explicit_local_opt_in() {
    let keycloak = run_api(
        "production",
        None,
        &[(
            "KEYCLOAK_CLIENT_SECRET",
            "replace_with_keycloak_admin_client_secret",
        )],
    );
    assert!(stderr(&keycloak).contains("KEYCLOAK_CLIENT_SECRET uses an unsafe placeholder value"));
    let keycloak_opted_in = run_api(
        "development",
        Some("1"),
        &[(
            "KEYCLOAK_CLIENT_SECRET",
            "replace_with_keycloak_admin_client_secret",
        )],
    );
    assert!(!stderr(&keycloak_opted_in).contains("Invalid secret configuration"));
    let database = run_api(
        "production",
        None,
        &[(
            "DATABASE_URL",
            "postgres://gmrag_user:change_me@127.0.0.1:0/gmrag",
        )],
    );
    assert!(stderr(&database).contains("DATABASE_URL uses an unsafe placeholder value"));
    let database_opted_in = run_api(
        "development",
        Some("1"),
        &[(
            "DATABASE_URL",
            "postgres://gmrag_user:change_me@127.0.0.1:0/gmrag",
        )],
    );
    assert!(!stderr(&database_opted_in).contains("Invalid secret configuration"));
}
