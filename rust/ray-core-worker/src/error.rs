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
