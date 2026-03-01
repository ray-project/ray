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
}
