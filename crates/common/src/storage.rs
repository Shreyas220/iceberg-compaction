/*
 * Storage abstraction using opendal.
 *
 * Provides a uniform interface for accessing S3, GCS, Azure, and local storage.
 * Workers use this to read data files and write compacted output.
 */

use crate::{CompactionError, Result};
use opendal::{Operator, Scheme};
use std::collections::HashMap;

/// Storage backend type (mirrors proto::StorageType for common crate usage).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageBackend {
    S3,
    Gcs,
    Azure,
    Local,
}

/// Configuration for building a storage operator.
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// The storage backend type
    pub backend: StorageBackend,
    /// Root path (bucket/container name or local path)
    pub root: String,
    /// Endpoint override (for S3-compatible services)
    pub endpoint: Option<String>,
    /// Region (for S3/Azure)
    pub region: Option<String>,
    /// Configuration options (credentials, etc.)
    pub options: HashMap<String, String>,
}

impl StorageConfig {
    /// Creates a local filesystem storage config.
    pub fn local(root: impl Into<String>) -> Self {
        Self {
            backend: StorageBackend::Local,
            root: root.into(),
            endpoint: None,
            region: None,
            options: HashMap::new(),
        }
    }

    /// Creates an S3 storage config.
    pub fn s3(bucket: impl Into<String>) -> Self {
        Self {
            backend: StorageBackend::S3,
            root: bucket.into(),
            endpoint: None,
            region: None,
            options: HashMap::new(),
        }
    }

    /// Creates a GCS storage config.
    pub fn gcs(bucket: impl Into<String>) -> Self {
        Self {
            backend: StorageBackend::Gcs,
            root: bucket.into(),
            endpoint: None,
            region: None,
            options: HashMap::new(),
        }
    }

    /// Creates an Azure Blob storage config.
    pub fn azure(container: impl Into<String>) -> Self {
        Self {
            backend: StorageBackend::Azure,
            root: container.into(),
            endpoint: None,
            region: None,
            options: HashMap::new(),
        }
    }

    /// Sets the endpoint.
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Sets the region.
    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Adds a configuration option.
    pub fn with_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.insert(key.into(), value.into());
        self
    }

    /// Sets AWS credentials.
    pub fn with_aws_credentials(
        self,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        session_token: Option<String>,
    ) -> Self {
        let mut s = self
            .with_option("access_key_id", access_key_id)
            .with_option("secret_access_key", secret_access_key);
        if let Some(token) = session_token {
            s = s.with_option("session_token", token);
        }
        s
    }

    /// Sets GCP service account.
    pub fn with_gcp_service_account(self, service_account_json: impl Into<String>) -> Self {
        self.with_option("credential", service_account_json)
    }

    /// Sets Azure storage key.
    pub fn with_azure_storage_key(
        self,
        account_name: impl Into<String>,
        account_key: impl Into<String>,
    ) -> Self {
        self.with_option("account_name", account_name)
            .with_option("account_key", account_key)
    }
}

/// Builds an opendal Operator from storage configuration.
///
/// This is the central factory function for creating storage access.
/// Workers call this to get an operator for reading data files and writing output.
pub fn build_operator(config: &StorageConfig) -> Result<Operator> {
    match config.backend {
        StorageBackend::S3 => build_s3_operator(config),
        StorageBackend::Gcs => build_gcs_operator(config),
        StorageBackend::Azure => build_azure_operator(config),
        StorageBackend::Local => build_local_operator(config),
    }
}

fn build_s3_operator(config: &StorageConfig) -> Result<Operator> {
    let mut builder = opendal::services::S3::default();

    builder = builder.bucket(&config.root);

    if let Some(endpoint) = &config.endpoint {
        builder = builder.endpoint(endpoint);
    }

    if let Some(region) = &config.region {
        builder = builder.region(region);
    }

    // Apply credentials if present
    if let Some(access_key) = config.options.get("access_key_id") {
        builder = builder.access_key_id(access_key);
    }
    if let Some(secret_key) = config.options.get("secret_access_key") {
        builder = builder.secret_access_key(secret_key);
    }
    if let Some(session_token) = config.options.get("session_token") {
        builder = builder.session_token(session_token);
    }

    // Apply additional options
    if let Some(role_arn) = config.options.get("role_arn") {
        builder = builder.role_arn(role_arn);
    }

    Operator::new(builder)
        .map(|op| op.finish())
        .map_err(|e| CompactionError::Storage(format!("Failed to build S3 operator: {}", e)))
}

fn build_gcs_operator(config: &StorageConfig) -> Result<Operator> {
    let mut builder = opendal::services::Gcs::default();

    builder = builder.bucket(&config.root);

    if let Some(endpoint) = &config.endpoint {
        builder = builder.endpoint(endpoint);
    }

    // GCP credentials
    if let Some(credential) = config.options.get("credential") {
        builder = builder.credential(credential);
    }

    Operator::new(builder)
        .map(|op| op.finish())
        .map_err(|e| CompactionError::Storage(format!("Failed to build GCS operator: {}", e)))
}

fn build_azure_operator(config: &StorageConfig) -> Result<Operator> {
    let mut builder = opendal::services::Azblob::default();

    builder = builder.container(&config.root);

    if let Some(endpoint) = &config.endpoint {
        builder = builder.endpoint(endpoint);
    }

    // Azure credentials
    if let Some(account_name) = config.options.get("account_name") {
        builder = builder.account_name(account_name);
    }
    if let Some(account_key) = config.options.get("account_key") {
        builder = builder.account_key(account_key);
    }
    if let Some(sas_token) = config.options.get("sas_token") {
        builder = builder.sas_token(sas_token);
    }

    Operator::new(builder)
        .map(|op| op.finish())
        .map_err(|e| CompactionError::Storage(format!("Failed to build Azure operator: {}", e)))
}

fn build_local_operator(config: &StorageConfig) -> Result<Operator> {
    let mut builder = opendal::services::Fs::default();

    builder = builder.root(&config.root);

    Operator::new(builder)
        .map(|op| op.finish())
        .map_err(|e| CompactionError::Storage(format!("Failed to build local operator: {}", e)))
}

/// Helper to get the scheme string for a storage backend.
pub fn backend_to_scheme(backend: &StorageBackend) -> Scheme {
    match backend {
        StorageBackend::S3 => Scheme::S3,
        StorageBackend::Gcs => Scheme::Gcs,
        StorageBackend::Azure => Scheme::Azblob,
        StorageBackend::Local => Scheme::Fs,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_local_config() {
        let config = StorageConfig::local("/tmp/test");
        assert_eq!(config.backend, StorageBackend::Local);
        assert_eq!(config.root, "/tmp/test");
    }

    #[test]
    fn test_s3_config_with_credentials() {
        let config = StorageConfig::s3("my-bucket")
            .with_region("us-west-2")
            .with_aws_credentials("AKID", "SECRET", Some("TOKEN".to_string()));

        assert_eq!(config.backend, StorageBackend::S3);
        assert_eq!(config.root, "my-bucket");
        assert_eq!(config.region, Some("us-west-2".to_string()));
        assert_eq!(config.options.get("access_key_id"), Some(&"AKID".to_string()));
        assert_eq!(config.options.get("session_token"), Some(&"TOKEN".to_string()));
    }

    #[test]
    fn test_build_local_operator() {
        // This should succeed on any system
        let config = StorageConfig::local("/tmp");
        let result = build_operator(&config);
        assert!(result.is_ok());
    }
}
