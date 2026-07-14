use std::sync::OnceLock;

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct ExternalTestEnvironment {
    pub database_url: String,
    pub qdrant_url: String,
    pub qdrant_collection: String,
    pub s3_bucket: String,
    pub openfga_store_id: String,
}

static EXTERNAL_TEST_ENVIRONMENT: OnceLock<ExternalTestEnvironment> = OnceLock::new();

pub fn require_external_test_environment() -> &'static ExternalTestEnvironment {
    EXTERNAL_TEST_ENVIRONMENT.get_or_init(load_external_test_environment)
}

#[allow(dead_code)]
pub fn database_url() -> Result<String, std::env::VarError> {
    Ok(require_external_test_environment().database_url.clone())
}

fn load_external_test_environment() -> ExternalTestEnvironment {
    let app_env = required_env("APP_ENV");
    assert_eq!(app_env, "test", "APP_ENV must be exactly test");

    let run_id = required_env("GMRAG_TEST_RUN_ID");
    assert!(
        run_id.starts_with("gmrag_test_")
            && run_id
                .chars()
                .all(|character| character.is_ascii_lowercase()
                    || character.is_ascii_digit()
                    || character == '_'),
        "GMRAG_TEST_RUN_ID must use the reserved gmrag_test_ prefix"
    );

    let database_url = required_env("DATABASE_URL");
    let test_database_url = required_env("TEST_DATABASE_URL");
    let dev_database_url = required_env("DEV_DATABASE_URL");
    assert_eq!(
        database_url, test_database_url,
        "DATABASE_URL must equal TEST_DATABASE_URL"
    );
    assert_ne!(
        test_database_url, dev_database_url,
        "TEST_DATABASE_URL must differ from DEV_DATABASE_URL"
    );
    assert_eq!(
        database_name(&test_database_url),
        run_id,
        "test database name must equal GMRAG_TEST_RUN_ID"
    );
    assert_ne!(
        database_name(&test_database_url),
        database_name(&dev_database_url),
        "test and dev database names must differ"
    );

    let qdrant_url = required_env("QDRANT_URL");
    let test_qdrant_url = required_env("TEST_QDRANT_URL");
    let dev_qdrant_url = required_env("DEV_QDRANT_URL");
    let qdrant_collection = required_env("QDRANT_COLLECTION");
    let test_qdrant_collection = required_env("TEST_QDRANT_COLLECTION");
    let dev_qdrant_collection = required_env("DEV_QDRANT_COLLECTION");
    assert_eq!(
        qdrant_url, test_qdrant_url,
        "QDRANT_URL must equal TEST_QDRANT_URL"
    );
    assert_ne!(
        test_qdrant_url, dev_qdrant_url,
        "TEST_QDRANT_URL must differ from DEV_QDRANT_URL"
    );
    assert_eq!(
        qdrant_collection, test_qdrant_collection,
        "QDRANT_COLLECTION must equal TEST_QDRANT_COLLECTION"
    );
    assert_eq!(
        test_qdrant_collection, run_id,
        "test Qdrant collection must equal GMRAG_TEST_RUN_ID"
    );
    assert_ne!(
        test_qdrant_collection, dev_qdrant_collection,
        "test and dev Qdrant collections must differ"
    );

    let s3_bucket = required_env("S3_BUCKET");
    let test_s3_bucket = required_env("TEST_S3_BUCKET");
    let dev_s3_bucket = required_env("DEV_S3_BUCKET");
    let expected_bucket = run_id
        .replacen("gmrag_test_", "gmrag-test-", 1)
        .replace('_', "-");
    assert_eq!(
        s3_bucket, test_s3_bucket,
        "S3_BUCKET must equal TEST_S3_BUCKET"
    );
    assert_eq!(
        test_s3_bucket, expected_bucket,
        "test MinIO bucket must match GMRAG_TEST_RUN_ID"
    );
    assert_ne!(
        test_s3_bucket, dev_s3_bucket,
        "test and dev MinIO buckets must differ"
    );

    let openfga_store_id = required_env("OPENFGA_STORE_ID");
    let test_openfga_store_id = required_env("TEST_OPENFGA_STORE_ID");
    let dev_openfga_store_id = required_env("DEV_OPENFGA_STORE_ID");
    let test_openfga_store_name = required_env("TEST_OPENFGA_STORE_NAME");
    assert_eq!(
        openfga_store_id, test_openfga_store_id,
        "OPENFGA_STORE_ID must equal TEST_OPENFGA_STORE_ID"
    );
    assert_eq!(
        test_openfga_store_name, expected_bucket,
        "test OpenFGA store name must match GMRAG_TEST_RUN_ID"
    );
    assert_ne!(
        test_openfga_store_id, dev_openfga_store_id,
        "test and dev OpenFGA stores must differ"
    );

    ExternalTestEnvironment {
        database_url,
        qdrant_url,
        qdrant_collection,
        s3_bucket,
        openfga_store_id,
    }
}

fn required_env(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("{name} must be set by the isolated integration-test runner"))
}

fn database_name(database_url: &str) -> String {
    database_url
        .split('?')
        .next()
        .and_then(|url| url.rsplit('/').next())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| panic!("database URL must include a database name"))
        .to_string()
}
