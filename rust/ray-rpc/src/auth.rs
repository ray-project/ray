// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Authentication middleware for gRPC.
//!
//! Replaces `src/ray/rpc/authentication/` (10 files).
//! Uses tower Layer/Service for intercepting requests.

use std::path::Path;

use tonic::{Request, Status};

/// Well-known Kubernetes paths for service account tokens.
pub mod k8s {
    /// Default path to the Kubernetes service account token.
    pub const SERVICE_ACCOUNT_TOKEN_PATH: &str =
        "/var/run/secrets/kubernetes.io/serviceaccount/token";

    /// Default path to the Kubernetes CA cert.
    pub const CA_CERT_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt";

    /// Default path to the Kubernetes namespace file.
    pub const NAMESPACE_PATH: &str =
        "/var/run/secrets/kubernetes.io/serviceaccount/namespace";

    /// Read the K8s service account token from the default path.
    pub fn read_service_account_token() -> Option<String> {
        read_token_from_path(SERVICE_ACCOUNT_TOKEN_PATH)
    }

    /// Read a token from a file path, trimming whitespace.
    pub fn read_token_from_path(path: &str) -> Option<String> {
        std::fs::read_to_string(path)
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    }

    /// Check if running inside a Kubernetes pod by looking for the token file.
    pub fn is_running_in_k8s() -> bool {
        std::path::Path::new(SERVICE_ACCOUNT_TOKEN_PATH).exists()
    }

    /// Read the current Kubernetes namespace.
    pub fn read_namespace() -> Option<String> {
        read_token_from_path(NAMESPACE_PATH)
    }
}

/// Authentication mode, matching C++ `AuthenticationMode` enum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthenticationMode {
    Disabled,
    ClusterAuth,
    K8sTokenAuth,
}

impl AuthenticationMode {
    pub fn from_config(mode_str: &str, k8s_enabled: bool) -> Self {
        if k8s_enabled {
            return Self::K8sTokenAuth;
        }
        match mode_str {
            "disabled" => Self::Disabled,
            "cluster" => Self::ClusterAuth,
            _ => Self::Disabled,
        }
    }

    /// Auto-detect the best authentication mode.
    pub fn auto_detect(cluster_token: Option<&str>) -> Self {
        if k8s::is_running_in_k8s() {
            Self::K8sTokenAuth
        } else if cluster_token.is_some() {
            Self::ClusterAuth
        } else {
            Self::Disabled
        }
    }
}

/// A tonic interceptor that adds/validates auth tokens.
#[derive(Clone)]
pub struct AuthInterceptor {
    mode: AuthenticationMode,
    token: Option<String>,
}

impl AuthInterceptor {
    pub fn new(mode: AuthenticationMode, token: Option<String>) -> Self {
        Self { mode, token }
    }

    /// Create an interceptor with auto-detected K8s token.
    pub fn with_k8s_token() -> Self {
        let token = k8s::read_service_account_token();
        Self {
            mode: AuthenticationMode::K8sTokenAuth,
            token,
        }
    }

    /// Create an interceptor from a token file path.
    pub fn from_token_file(mode: AuthenticationMode, path: &Path) -> Self {
        let token = std::fs::read_to_string(path)
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        Self { mode, token }
    }

    /// Get the current authentication mode.
    pub fn mode(&self) -> &AuthenticationMode {
        &self.mode
    }

    /// Check if authentication is enabled.
    pub fn is_enabled(&self) -> bool {
        self.mode != AuthenticationMode::Disabled
    }

    /// Intercept outgoing requests (client-side) to add auth token.
    pub fn intercept_request(&self, mut request: Request<()>) -> Result<Request<()>, Status> {
        match &self.mode {
            AuthenticationMode::Disabled => Ok(request),
            AuthenticationMode::ClusterAuth | AuthenticationMode::K8sTokenAuth => {
                if let Some(token) = &self.token {
                    let bearer = format!("Bearer {token}");
                    request.metadata_mut().insert(
                        "authorization",
                        bearer
                            .parse()
                            .map_err(|_| Status::internal("Failed to set authorization header"))?,
                    );
                }
                Ok(request)
            }
        }
    }

    /// Validate an incoming request (server-side).
    pub fn validate_request<T>(&self, request: &Request<T>) -> Result<(), Status> {
        match &self.mode {
            AuthenticationMode::Disabled => Ok(()),
            AuthenticationMode::ClusterAuth | AuthenticationMode::K8sTokenAuth => {
                let expected_token = self
                    .token
                    .as_deref()
                    .ok_or_else(|| Status::internal("No auth token configured"))?;

                let auth_header = request
                    .metadata()
                    .get("authorization")
                    .ok_or_else(|| Status::unauthenticated("Missing authorization header"))?
                    .to_str()
                    .map_err(|_| Status::unauthenticated("Invalid authorization header"))?;

                let token = auth_header
                    .strip_prefix("Bearer ")
                    .ok_or_else(|| Status::unauthenticated("Invalid bearer token format"))?;

                if token == expected_token {
                    Ok(())
                } else {
                    Err(Status::unauthenticated("Invalid auth token"))
                }
            }
        }
    }
}

impl tonic::service::Interceptor for AuthInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        self.validate_request(&request)?;
        Ok(request)
    }
}

/// Result of a token review (K8s TokenReview API).
#[derive(Debug, Clone)]
pub struct TokenReviewResult {
    /// Whether the token is authenticated.
    pub authenticated: bool,
    /// The username associated with the token, if authenticated.
    pub username: Option<String>,
    /// Groups the user belongs to.
    pub groups: Vec<String>,
    /// Error message if authentication failed.
    pub error: Option<String>,
}

/// Trait for validating tokens against an external authority (e.g., K8s API).
///
/// The default `AuthInterceptor` does simple string comparison.
/// For production K8s deployments, implement this trait to call the
/// K8s TokenReview API for proper token validation.
pub trait TokenReviewer: Send + Sync {
    /// Validate a bearer token and return the review result.
    fn review_token(&self, token: &str) -> TokenReviewResult;
}

/// A simple local token reviewer that compares against a known token.
///
/// Used for ClusterAuth mode where the token is a shared secret.
pub struct LocalTokenReviewer {
    expected_token: String,
    username: String,
}

impl LocalTokenReviewer {
    pub fn new(expected_token: impl Into<String>, username: impl Into<String>) -> Self {
        Self {
            expected_token: expected_token.into(),
            username: username.into(),
        }
    }
}

impl TokenReviewer for LocalTokenReviewer {
    fn review_token(&self, token: &str) -> TokenReviewResult {
        if token == self.expected_token {
            TokenReviewResult {
                authenticated: true,
                username: Some(self.username.clone()),
                groups: vec!["ray-users".to_string()],
                error: None,
            }
        } else {
            TokenReviewResult {
                authenticated: false,
                username: None,
                groups: vec![],
                error: Some("Invalid token".to_string()),
            }
        }
    }
}

/// TLS configuration for gRPC channels.
#[derive(Debug, Clone, Default)]
pub struct TlsConfig {
    /// Path to CA certificate for server verification.
    pub ca_cert_path: Option<String>,
    /// Path to client certificate (for mutual TLS).
    pub client_cert_path: Option<String>,
    /// Path to client key (for mutual TLS).
    pub client_key_path: Option<String>,
    /// Whether to skip server certificate verification (INSECURE).
    pub skip_verify: bool,
}

impl TlsConfig {
    /// Create a TLS config for K8s in-cluster communication.
    pub fn k8s_in_cluster() -> Self {
        Self {
            ca_cert_path: Some(k8s::CA_CERT_PATH.to_string()),
            client_cert_path: None,
            client_key_path: None,
            skip_verify: false,
        }
    }

    /// Check if TLS is configured (any cert paths set).
    pub fn is_enabled(&self) -> bool {
        self.ca_cert_path.is_some()
            || self.client_cert_path.is_some()
    }

    /// Check if mutual TLS is configured.
    pub fn is_mtls(&self) -> bool {
        self.client_cert_path.is_some() && self.client_key_path.is_some()
    }

    /// Read the CA certificate bytes, if configured.
    pub fn read_ca_cert(&self) -> Option<Vec<u8>> {
        self.ca_cert_path
            .as_ref()
            .and_then(|p| std::fs::read(p).ok())
    }

    /// Read the client certificate bytes, if configured.
    pub fn read_client_cert(&self) -> Option<Vec<u8>> {
        self.client_cert_path
            .as_ref()
            .and_then(|p| std::fs::read(p).ok())
    }

    /// Read the client key bytes, if configured.
    pub fn read_client_key(&self) -> Option<Vec<u8>> {
        self.client_key_path
            .as_ref()
            .and_then(|p| std::fs::read(p).ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_disabled_auth() {
        let interceptor = AuthInterceptor::new(AuthenticationMode::Disabled, None);
        let request = Request::new(());
        assert!(interceptor.intercept_request(request).is_ok());
    }

    #[test]
    fn test_auth_mode_from_config() {
        assert_eq!(
            AuthenticationMode::from_config("disabled", false),
            AuthenticationMode::Disabled
        );
        assert_eq!(
            AuthenticationMode::from_config("cluster", false),
            AuthenticationMode::ClusterAuth
        );
        assert_eq!(
            AuthenticationMode::from_config("disabled", true),
            AuthenticationMode::K8sTokenAuth
        );
    }

    #[test]
    fn test_auth_mode_unknown_string() {
        assert_eq!(
            AuthenticationMode::from_config("unknown", false),
            AuthenticationMode::Disabled
        );
    }

    #[test]
    fn test_cluster_auth_intercept_adds_bearer() {
        let interceptor =
            AuthInterceptor::new(AuthenticationMode::ClusterAuth, Some("secret123".into()));
        let request = Request::new(());
        let result = interceptor.intercept_request(request).unwrap();
        let auth = result.metadata().get("authorization").unwrap().to_str().unwrap();
        assert_eq!(auth, "Bearer secret123");
    }

    #[test]
    fn test_cluster_auth_no_token_still_succeeds() {
        // If no token is set, the interceptor doesn't add a header (but doesn't fail)
        let interceptor = AuthInterceptor::new(AuthenticationMode::ClusterAuth, None);
        let request = Request::new(());
        let result = interceptor.intercept_request(request).unwrap();
        assert!(result.metadata().get("authorization").is_none());
    }

    #[test]
    fn test_validate_disabled_allows_all() {
        let interceptor = AuthInterceptor::new(AuthenticationMode::Disabled, None);
        let request = Request::new(());
        assert!(interceptor.validate_request(&request).is_ok());
    }

    #[test]
    fn test_validate_cluster_auth_valid_token() {
        let interceptor =
            AuthInterceptor::new(AuthenticationMode::ClusterAuth, Some("mytoken".into()));
        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("authorization", "Bearer mytoken".parse().unwrap());
        assert!(interceptor.validate_request(&request).is_ok());
    }

    #[test]
    fn test_validate_cluster_auth_invalid_token() {
        let interceptor =
            AuthInterceptor::new(AuthenticationMode::ClusterAuth, Some("mytoken".into()));
        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("authorization", "Bearer wrongtoken".parse().unwrap());
        let err = interceptor.validate_request(&request).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn test_validate_missing_header() {
        let interceptor =
            AuthInterceptor::new(AuthenticationMode::ClusterAuth, Some("token".into()));
        let request = Request::new(());
        let err = interceptor.validate_request(&request).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn test_validate_wrong_format() {
        let interceptor =
            AuthInterceptor::new(AuthenticationMode::ClusterAuth, Some("token".into()));
        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("authorization", "Basic dXNlcjpwYXNz".parse().unwrap());
        let err = interceptor.validate_request(&request).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn test_validate_no_configured_token() {
        let interceptor = AuthInterceptor::new(AuthenticationMode::ClusterAuth, None);
        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("authorization", "Bearer test".parse().unwrap());
        let err = interceptor.validate_request(&request).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);
    }

    #[test]
    fn test_is_enabled() {
        assert!(!AuthInterceptor::new(AuthenticationMode::Disabled, None).is_enabled());
        assert!(AuthInterceptor::new(AuthenticationMode::ClusterAuth, None).is_enabled());
        assert!(AuthInterceptor::new(AuthenticationMode::K8sTokenAuth, None).is_enabled());
    }

    #[test]
    fn test_from_token_file() {
        let dir = tempfile::tempdir().unwrap();
        let token_path = dir.path().join("token");
        std::fs::write(&token_path, "  file-token-123  \n").unwrap();

        let interceptor =
            AuthInterceptor::from_token_file(AuthenticationMode::ClusterAuth, &token_path);
        let request = Request::new(());
        let result = interceptor.intercept_request(request).unwrap();
        let auth = result.metadata().get("authorization").unwrap().to_str().unwrap();
        assert_eq!(auth, "Bearer file-token-123");
    }

    #[test]
    fn test_from_token_file_missing() {
        let interceptor = AuthInterceptor::from_token_file(
            AuthenticationMode::ClusterAuth,
            Path::new("/nonexistent/token"),
        );
        assert!(interceptor.token.is_none());
    }

    #[test]
    fn test_k8s_read_token_from_path() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("token");
        std::fs::write(&path, "k8s-token-abc\n").unwrap();

        let token = k8s::read_token_from_path(path.to_str().unwrap());
        assert_eq!(token, Some("k8s-token-abc".to_string()));
    }

    #[test]
    fn test_k8s_read_token_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty");
        std::fs::write(&path, "  \n").unwrap();

        let token = k8s::read_token_from_path(path.to_str().unwrap());
        assert!(token.is_none());
    }

    #[test]
    fn test_k8s_read_token_missing_file() {
        let token = k8s::read_token_from_path("/nonexistent/path");
        assert!(token.is_none());
    }

    #[test]
    fn test_auto_detect_disabled() {
        // Not in K8s, no cluster token → disabled
        let mode = AuthenticationMode::auto_detect(None);
        // This test might return K8sTokenAuth if running in k8s, but normally:
        if !k8s::is_running_in_k8s() {
            assert_eq!(mode, AuthenticationMode::Disabled);
        }
    }

    #[test]
    fn test_auto_detect_cluster_token() {
        if !k8s::is_running_in_k8s() {
            let mode = AuthenticationMode::auto_detect(Some("token"));
            assert_eq!(mode, AuthenticationMode::ClusterAuth);
        }
    }

    #[test]
    fn test_roundtrip_intercept_validate() {
        let token = "roundtrip-secret";
        let client_interceptor =
            AuthInterceptor::new(AuthenticationMode::ClusterAuth, Some(token.into()));
        let server_interceptor =
            AuthInterceptor::new(AuthenticationMode::ClusterAuth, Some(token.into()));

        // Client adds the token
        let request = Request::new(());
        let outgoing = client_interceptor.intercept_request(request).unwrap();

        // Transfer metadata to a new request (simulating network)
        let mut incoming = Request::new("payload");
        if let Some(auth) = outgoing.metadata().get("authorization") {
            incoming
                .metadata_mut()
                .insert("authorization", auth.clone());
        }

        // Server validates
        assert!(server_interceptor.validate_request(&incoming).is_ok());
    }

    #[test]
    fn test_local_token_reviewer_valid() {
        let reviewer = LocalTokenReviewer::new("secret", "ray-user");
        let result = reviewer.review_token("secret");
        assert!(result.authenticated);
        assert_eq!(result.username, Some("ray-user".to_string()));
        assert!(result.groups.contains(&"ray-users".to_string()));
        assert!(result.error.is_none());
    }

    #[test]
    fn test_local_token_reviewer_invalid() {
        let reviewer = LocalTokenReviewer::new("secret", "ray-user");
        let result = reviewer.review_token("wrong");
        assert!(!result.authenticated);
        assert!(result.username.is_none());
        assert!(result.groups.is_empty());
        assert!(result.error.is_some());
    }

    #[test]
    fn test_tls_config_default() {
        let config = TlsConfig::default();
        assert!(!config.is_enabled());
        assert!(!config.is_mtls());
        assert!(!config.skip_verify);
    }

    #[test]
    fn test_tls_config_k8s() {
        let config = TlsConfig::k8s_in_cluster();
        assert!(config.is_enabled());
        assert!(!config.is_mtls());
        assert_eq!(config.ca_cert_path.as_deref(), Some(k8s::CA_CERT_PATH));
    }

    #[test]
    fn test_tls_config_mtls() {
        let config = TlsConfig {
            ca_cert_path: Some("/ca.pem".into()),
            client_cert_path: Some("/client.pem".into()),
            client_key_path: Some("/client.key".into()),
            skip_verify: false,
        };
        assert!(config.is_enabled());
        assert!(config.is_mtls());
    }

    #[test]
    fn test_tls_config_read_missing() {
        let config = TlsConfig {
            ca_cert_path: Some("/nonexistent/ca.pem".into()),
            ..Default::default()
        };
        assert!(config.read_ca_cert().is_none());
        assert!(config.read_client_cert().is_none());
        assert!(config.read_client_key().is_none());
    }

    #[test]
    fn test_interceptor_trait_disabled() {
        let mut auth = AuthInterceptor::new(AuthenticationMode::Disabled, None);
        let request = Request::new(());
        // Disabled auth should pass through.
        use tonic::service::Interceptor;
        let result = auth.call(request);
        assert!(result.is_ok());
    }

    #[test]
    fn test_interceptor_trait_rejects_unauthenticated() {
        let mut auth =
            AuthInterceptor::new(AuthenticationMode::ClusterAuth, Some("secret".into()));
        let request = Request::new(());
        // No auth header — should be rejected.
        use tonic::service::Interceptor;
        let result = auth.call(request);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn test_interceptor_trait_accepts_valid_token() {
        let mut auth =
            AuthInterceptor::new(AuthenticationMode::ClusterAuth, Some("secret".into()));
        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("authorization", "Bearer secret".parse().unwrap());
        use tonic::service::Interceptor;
        let result = auth.call(request);
        assert!(result.is_ok());
    }

    #[test]
    fn test_tls_config_read_files() {
        let dir = tempfile::tempdir().unwrap();
        let ca_path = dir.path().join("ca.pem");
        let cert_path = dir.path().join("client.pem");
        let key_path = dir.path().join("client.key");
        std::fs::write(&ca_path, b"CA_CERT_DATA").unwrap();
        std::fs::write(&cert_path, b"CLIENT_CERT_DATA").unwrap();
        std::fs::write(&key_path, b"CLIENT_KEY_DATA").unwrap();

        let config = TlsConfig {
            ca_cert_path: Some(ca_path.to_str().unwrap().into()),
            client_cert_path: Some(cert_path.to_str().unwrap().into()),
            client_key_path: Some(key_path.to_str().unwrap().into()),
            skip_verify: false,
        };
        assert_eq!(config.read_ca_cert().unwrap(), b"CA_CERT_DATA");
        assert_eq!(config.read_client_cert().unwrap(), b"CLIENT_CERT_DATA");
        assert_eq!(config.read_client_key().unwrap(), b"CLIENT_KEY_DATA");
    }
}
