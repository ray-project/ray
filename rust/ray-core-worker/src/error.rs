// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Core worker error types.

use ray_common::status::RayError;

/// Errors specific to the core worker.
#[derive(Debug, thiserror::Error)]
pub enum CoreWorkerError {
    #[error("object not found: {0}")]
    ObjectNotFound(String),

    #[error("object already exists: {0}")]
    ObjectAlreadyExists(String),

    #[error("actor not found: {0}")]
    ActorNotFound(String),

    #[error("task submission failed: {0}")]
    TaskSubmissionFailed(String),

    #[error("core worker not initialized")]
    NotInitialized,

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("operation timed out: {0}")]
    TimedOut(String),

    #[error("ray status error: {0}")]
    RayStatus(#[from] RayError),

    #[error("internal error: {0}")]
    Internal(String),

    #[error("{0}")]
    Other(String),
}

/// Result type alias for core worker operations.
pub type CoreWorkerResult<T> = Result<T, CoreWorkerError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let e = CoreWorkerError::ObjectNotFound("abc".into());
        assert_eq!(format!("{}", e), "object not found: abc");
    }

    #[test]
    fn test_all_error_variants() {
        let variants: Vec<CoreWorkerError> = vec![
            CoreWorkerError::ObjectNotFound("oid".into()),
            CoreWorkerError::ObjectAlreadyExists("oid".into()),
            CoreWorkerError::ActorNotFound("aid".into()),
            CoreWorkerError::TaskSubmissionFailed("reason".into()),
            CoreWorkerError::NotInitialized,
            CoreWorkerError::InvalidArgument("arg".into()),
            CoreWorkerError::TimedOut("5s".into()),
            CoreWorkerError::Internal("oops".into()),
            CoreWorkerError::Other("misc".into()),
        ];
        for v in &variants {
            // All variants should have a non-empty Display.
            assert!(!format!("{}", v).is_empty());
        }
    }

    #[test]
    fn test_error_is_debug() {
        let e = CoreWorkerError::NotInitialized;
        let dbg = format!("{:?}", e);
        assert!(dbg.contains("NotInitialized"));
    }

    #[test]
    fn test_from_ray_error() {
        let ray_err = RayError::object_not_found("xyz");
        let cw_err: CoreWorkerError = ray_err.into();
        assert!(format!("{}", cw_err).contains("xyz"));
    }
}
