pub mod cleanup;

use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use std::fmt;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageConfig {
    pub endpoint_url: Option<String>,
    pub region: String,
    pub bucket: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub force_path_style: bool,
    pub presign_expiry_secs: u64,
}

#[derive(Debug)]
pub enum StorageConfigError {
    MissingEnv { key: &'static str },
    InvalidBool { key: &'static str, value: String },
    InvalidNumber { key: &'static str, value: String },
}

impl fmt::Display for StorageConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageConfigError::MissingEnv { key } => {
                write!(f, "Missing required env var: {key}")
            }
            StorageConfigError::InvalidBool { key, value } => {
                write!(f, "Invalid boolean env var {key}: {value}")
            }
            StorageConfigError::InvalidNumber { key, value } => {
                write!(f, "Invalid numeric env var {key}: {value}")
            }
        }
    }
}

impl std::error::Error for StorageConfigError {}

impl StorageConfig {
    pub fn from_env() -> Result<Self, StorageConfigError> {
        Self::from_provider(|key| std::env::var(key).ok())
    }

    fn from_provider<F>(mut env_getter: F) -> Result<Self, StorageConfigError>
    where
        F: FnMut(&str) -> Option<String>,
    {
        let endpoint_url = env_getter("S3_ENDPOINT_URL")
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());

        let region = required_env(&mut env_getter, "S3_REGION")?;
        let bucket = required_env(&mut env_getter, "S3_BUCKET")?;
        let access_key_id = required_env(&mut env_getter, "S3_ACCESS_KEY_ID")?;
        let secret_access_key = required_env(&mut env_getter, "S3_SECRET_ACCESS_KEY")?;

        let force_path_style = parse_bool(
            &required_env(&mut env_getter, "S3_FORCE_PATH_STYLE")?,
            "S3_FORCE_PATH_STYLE",
        )?;

        let presign_expiry_secs = parse_u64(
            &required_env(&mut env_getter, "S3_PRESIGN_EXPIRY_SECS")?,
            "S3_PRESIGN_EXPIRY_SECS",
        )?;

        Ok(Self {
            endpoint_url,
            region,
            bucket,
            access_key_id,
            secret_access_key,
            force_path_style,
            presign_expiry_secs,
        })
    }
}

fn required_env<F>(env_getter: &mut F, key: &'static str) -> Result<String, StorageConfigError>
where
    F: FnMut(&str) -> Option<String>,
{
    let value = env_getter(key)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or(StorageConfigError::MissingEnv { key })?;

    Ok(value)
}

fn parse_bool(value: &str, key: &'static str) -> Result<bool, StorageConfigError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(StorageConfigError::InvalidBool {
            key,
            value: value.to_string(),
        }),
    }
}

fn parse_u64(value: &str, key: &'static str) -> Result<u64, StorageConfigError> {
    value
        .trim()
        .parse::<u64>()
        .map_err(|_| StorageConfigError::InvalidNumber {
            key,
            value: value.to_string(),
        })
}

#[derive(Debug)]
pub enum StorageError {
    ObjectNotFound { object_key: String },
    OperationFailed { message: String },
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageError::ObjectNotFound { object_key } => {
                write!(f, "Object not found: {object_key}")
            }
            StorageError::OperationFailed { message } => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for StorageError {}

#[derive(Debug, Clone)]
pub struct PutObjectResult {
    pub etag: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageObjectInfo {
    pub key: String,
    pub size_bytes: Option<i64>,
}

#[derive(Clone)]
pub struct StorageClient {
    client: Client,
    bucket: String,
}

impl StorageClient {
    pub async fn from_config(config: StorageConfig) -> Self {
        let credentials = aws_sdk_s3::config::Credentials::new(
            config.access_key_id,
            config.secret_access_key,
            None,
            None,
            "gmrag-storage-config",
        );

        let shared_config = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(config.region))
            .credentials_provider(credentials)
            .load()
            .await;

        let mut builder = aws_sdk_s3::config::Builder::from(&shared_config)
            .force_path_style(config.force_path_style);

        if let Some(endpoint_url) = config.endpoint_url {
            builder = builder.endpoint_url(endpoint_url);
        }

        let client = Client::from_conf(builder.build());

        Self {
            client,
            bucket: config.bucket,
        }
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub async fn put_original_document(
        &self,
        object_key: &str,
        bytes: &[u8],
        content_type: Option<&str>,
    ) -> Result<PutObjectResult, StorageError> {
        let mut request = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(object_key)
            .body(ByteStream::from(bytes.to_vec()));

        if let Some(content_type) = content_type {
            request = request.content_type(content_type);
        }

        let response = request
            .send()
            .await
            .map_err(|err| StorageError::OperationFailed {
                message: format!(
                    "S3 put_object failed for bucket={} object_key={}: {}",
                    self.bucket, object_key, err
                ),
            })?;

        Ok(PutObjectResult {
            etag: response.e_tag,
        })
    }

    pub async fn get_original_document(&self, object_key: &str) -> Result<Vec<u8>, StorageError> {
        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(object_key)
            .send()
            .await
            .map_err(|err| map_s3_error(err, object_key, "get_object"))?;

        let body = response
            .body
            .collect()
            .await
            .map_err(|err| StorageError::OperationFailed {
                message: format!(
                    "S3 get_object body read failed for bucket={} object_key={}: {}",
                    self.bucket, object_key, err
                ),
            })?;

        Ok(body.into_bytes().to_vec())
    }

    pub async fn delete_object(&self, object_key: &str) -> Result<(), StorageError> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(object_key)
            .send()
            .await
            .map_err(|err| map_s3_error(err, object_key, "delete_object"))?;

        Ok(())
    }

    pub async fn object_exists(&self, object_key: &str) -> Result<bool, StorageError> {
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(object_key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(err) if is_not_found_error(&err) => Ok(false),
            Err(err) => Err(StorageError::OperationFailed {
                message: format!(
                    "S3 head_object failed for bucket={} object_key={}: {}",
                    self.bucket, object_key, err
                ),
            }),
        }
    }

    pub async fn list_objects(
        &self,
        prefix: Option<&str>,
    ) -> Result<Vec<StorageObjectInfo>, StorageError> {
        let mut continuation_token: Option<String> = None;
        let mut objects: Vec<StorageObjectInfo> = Vec::new();

        loop {
            let mut request = self.client.list_objects_v2().bucket(&self.bucket);
            if let Some(prefix) = prefix {
                request = request.prefix(prefix);
            }
            if let Some(token) = continuation_token.as_deref() {
                request = request.continuation_token(token);
            }

            let response = request
                .send()
                .await
                .map_err(|err| StorageError::OperationFailed {
                    message: format!(
                        "S3 list_objects_v2 failed for bucket={} prefix={}: {}",
                        self.bucket,
                        prefix.unwrap_or_default(),
                        err
                    ),
                })?;

            for object in response.contents() {
                let Some(key) = object.key() else {
                    continue;
                };

                objects.push(StorageObjectInfo {
                    key: key.to_string(),
                    size_bytes: object.size(),
                });
            }

            if !response.is_truncated().unwrap_or(false) {
                break;
            }

            continuation_token = response.next_continuation_token().map(ToString::to_string);

            if continuation_token.is_none() {
                break;
            }
        }

        Ok(objects)
    }

    pub async fn delete_objects(&self, object_keys: &[String]) -> Result<usize, StorageError> {
        if object_keys.is_empty() {
            return Ok(0);
        }

        let mut deleted_count = 0usize;

        for chunk in object_keys.chunks(1000) {
            let object_identifiers = chunk
                .iter()
                .map(|object_key| {
                    ObjectIdentifier::builder()
                        .key(object_key)
                        .build()
                        .map_err(|err| StorageError::OperationFailed {
                            message: format!("Failed to build object identifier: {err}"),
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;

            let delete_payload = Delete::builder()
                .set_objects(Some(object_identifiers))
                .build()
                .map_err(|err| StorageError::OperationFailed {
                    message: format!("Failed to build delete payload: {err}"),
                })?;

            let response = self
                .client
                .delete_objects()
                .bucket(&self.bucket)
                .delete(delete_payload)
                .send()
                .await
                .map_err(|err| StorageError::OperationFailed {
                    message: format!(
                        "S3 delete_objects failed for bucket={}: {}",
                        self.bucket, err
                    ),
                })?;

            for err in response.errors() {
                if err
                    .code()
                    .is_some_and(|code| matches!(code, "NoSuchKey" | "NotFound" | "404"))
                {
                    continue;
                }

                return Err(StorageError::OperationFailed {
                    message: format!(
                        "S3 delete_objects returned error code={} message={}",
                        err.code().unwrap_or("unknown"),
                        err.message().unwrap_or("unknown")
                    ),
                });
            }

            deleted_count += response.deleted().len();
        }

        Ok(deleted_count)
    }
}

pub fn build_original_document_object_key(
    tenant_id: Uuid,
    workspace_id: Uuid,
    document_id: Uuid,
) -> String {
    format!("tenants/{tenant_id}/workspaces/{workspace_id}/documents/{document_id}/original.pdf")
}

fn map_s3_error<E, R>(
    err: aws_sdk_s3::error::SdkError<E, R>,
    object_key: &str,
    operation: &str,
) -> StorageError
where
    E: ProvideErrorMetadata,
{
    if is_not_found_error(&err) {
        return StorageError::ObjectNotFound {
            object_key: object_key.to_string(),
        };
    }

    StorageError::OperationFailed {
        message: format!("S3 {operation} failed for object_key={object_key}: {err}"),
    }
}

fn is_not_found_error<E, R>(err: &aws_sdk_s3::error::SdkError<E, R>) -> bool
where
    E: ProvideErrorMetadata,
{
    err.as_service_error()
        .and_then(|service_error| service_error.code())
        .is_some_and(|code| matches!(code, "NoSuchKey" | "NotFound" | "404"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_key_builder_uses_expected_format() {
        let tenant_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
        let workspace_id = Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();
        let document_id = Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap();

        let key = build_original_document_object_key(tenant_id, workspace_id, document_id);

        assert_eq!(
            key,
            "tenants/11111111-1111-1111-1111-111111111111/workspaces/22222222-2222-2222-2222-222222222222/documents/33333333-3333-3333-3333-333333333333/original.pdf"
        );
    }

    #[test]
    fn object_key_builder_contains_required_identifiers_only() {
        let tenant_id = Uuid::new_v4();
        let workspace_id = Uuid::new_v4();
        let document_id = Uuid::new_v4();

        let key = build_original_document_object_key(tenant_id, workspace_id, document_id);

        assert!(key.contains(&tenant_id.to_string()));
        assert!(key.contains(&workspace_id.to_string()));
        assert!(key.contains(&document_id.to_string()));
        assert!(!key.contains("invoice-final.pdf"));
    }

    #[test]
    fn object_key_builder_prevents_path_traversal_segments() {
        let key =
            build_original_document_object_key(Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());

        assert!(!key.contains(".."));
        assert!(!key.contains('\\'));
        assert!(!key.contains("//"));
    }

    #[test]
    fn storage_config_parses_provider_values() {
        let vars = std::collections::HashMap::from([
            ("S3_ENDPOINT_URL", "http://localhost:9000"),
            ("S3_REGION", "us-east-1"),
            ("S3_BUCKET", "gmrag-documents"),
            ("S3_ACCESS_KEY_ID", "minioadmin"),
            ("S3_SECRET_ACCESS_KEY", "minioadmin"),
            ("S3_FORCE_PATH_STYLE", "true"),
            ("S3_PRESIGN_EXPIRY_SECS", "900"),
        ]);

        let parsed =
            StorageConfig::from_provider(|key| vars.get(key).map(|value| value.to_string()))
                .expect("storage config should parse");

        assert_eq!(
            parsed.endpoint_url.as_deref(),
            Some("http://localhost:9000")
        );
        assert_eq!(parsed.region, "us-east-1");
        assert_eq!(parsed.bucket, "gmrag-documents");
        assert!(parsed.force_path_style);
        assert_eq!(parsed.presign_expiry_secs, 900);
    }
}
