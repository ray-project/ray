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

use tonic::{Request, Status};

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
}
