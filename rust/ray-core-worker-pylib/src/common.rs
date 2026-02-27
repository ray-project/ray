// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Common enums and error conversion for the Python bindings.

use ray_core_worker::CoreWorkerError;

/// Python-facing language enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PyLanguage {
    Python,
    Java,
    Cpp,
}

impl PyLanguage {
    pub fn to_core(self) -> ray_core_worker::Language {
        match self {
            PyLanguage::Python => ray_core_worker::Language::Python,
            PyLanguage::Java => ray_core_worker::Language::Java,
            PyLanguage::Cpp => ray_core_worker::Language::Cpp,
        }
    }

    pub fn from_i32(v: i32) -> Option<Self> {
        match v {
            0 => Some(PyLanguage::Python),
            1 => Some(PyLanguage::Java),
            2 => Some(PyLanguage::Cpp),
            _ => None,
        }
    }
}

/// Python-facing worker type enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PyWorkerType {
    Worker,
    Driver,
    SpillWorker,
    RestoreWorker,
}

impl PyWorkerType {
    pub fn to_core(self) -> ray_core_worker::WorkerType {
        match self {
            PyWorkerType::Worker => ray_core_worker::WorkerType::Worker,
            PyWorkerType::Driver => ray_core_worker::WorkerType::Driver,
            PyWorkerType::SpillWorker => ray_core_worker::WorkerType::SpillWorker,
            PyWorkerType::RestoreWorker => ray_core_worker::WorkerType::RestoreWorker,
        }
    }

    pub fn from_i32(v: i32) -> Option<Self> {
        match v {
            0 => Some(PyWorkerType::Worker),
            1 => Some(PyWorkerType::Driver),
            2 => Some(PyWorkerType::SpillWorker),
            3 => Some(PyWorkerType::RestoreWorker),
            _ => None,
        }
    }
}

/// Convert a `CoreWorkerError` to a Python-compatible error string and category.
///
/// Returns `(exception_type, message)` where exception_type is one of:
/// "KeyError", "TimeoutError", "ValueError", "RuntimeError".
pub fn classify_error(err: &CoreWorkerError) -> (&'static str, String) {
    match err {
        CoreWorkerError::ObjectNotFound(msg) => ("KeyError", msg.clone()),
        CoreWorkerError::ActorNotFound(msg) => ("KeyError", msg.clone()),
        CoreWorkerError::ObjectAlreadyExists(msg) => ("ValueError", msg.clone()),
        CoreWorkerError::TimedOut(msg) => ("TimeoutError", msg.clone()),
        CoreWorkerError::InvalidArgument(msg) => ("ValueError", msg.clone()),
        CoreWorkerError::NotInitialized => ("RuntimeError", err.to_string()),
        CoreWorkerError::TaskSubmissionFailed(msg) => ("RuntimeError", msg.clone()),
        CoreWorkerError::RayStatus(e) => ("RuntimeError", e.to_string()),
        CoreWorkerError::Internal(msg) => ("RuntimeError", msg.clone()),
        CoreWorkerError::Other(msg) => ("RuntimeError", msg.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_py_language_roundtrip() {
        assert_eq!(PyLanguage::from_i32(0), Some(PyLanguage::Python));
        assert_eq!(PyLanguage::from_i32(1), Some(PyLanguage::Java));
        assert_eq!(PyLanguage::from_i32(2), Some(PyLanguage::Cpp));
        assert_eq!(PyLanguage::from_i32(99), None);
    }

    #[test]
    fn test_py_worker_type_roundtrip() {
        assert_eq!(PyWorkerType::from_i32(0), Some(PyWorkerType::Worker));
        assert_eq!(PyWorkerType::from_i32(1), Some(PyWorkerType::Driver));
        assert_eq!(PyWorkerType::from_i32(3), Some(PyWorkerType::RestoreWorker));
        assert_eq!(PyWorkerType::from_i32(-1), None);
    }

    #[test]
    fn test_classify_error() {
        let (ty, _) = classify_error(&CoreWorkerError::ObjectNotFound("x".into()));
        assert_eq!(ty, "KeyError");

        let (ty, _) = classify_error(&CoreWorkerError::TimedOut("t".into()));
        assert_eq!(ty, "TimeoutError");

        let (ty, _) = classify_error(&CoreWorkerError::InvalidArgument("a".into()));
        assert_eq!(ty, "ValueError");

        let (ty, _) = classify_error(&CoreWorkerError::NotInitialized);
        assert_eq!(ty, "RuntimeError");
    }
}
