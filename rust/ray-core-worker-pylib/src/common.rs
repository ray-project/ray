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
    fn test_py_language_to_core() {
        use ray_core_worker::Language;
        assert_eq!(PyLanguage::Python.to_core(), Language::Python);
        assert_eq!(PyLanguage::Java.to_core(), Language::Java);
        assert_eq!(PyLanguage::Cpp.to_core(), Language::Cpp);
    }

    #[test]
    fn test_py_worker_type_roundtrip() {
        assert_eq!(PyWorkerType::from_i32(0), Some(PyWorkerType::Worker));
        assert_eq!(PyWorkerType::from_i32(1), Some(PyWorkerType::Driver));
        assert_eq!(PyWorkerType::from_i32(2), Some(PyWorkerType::SpillWorker));
        assert_eq!(PyWorkerType::from_i32(3), Some(PyWorkerType::RestoreWorker));
        assert_eq!(PyWorkerType::from_i32(-1), None);
        assert_eq!(PyWorkerType::from_i32(4), None);
    }

    #[test]
    fn test_py_worker_type_to_core() {
        use ray_core_worker::WorkerType;
        assert_eq!(PyWorkerType::Worker.to_core(), WorkerType::Worker);
        assert_eq!(PyWorkerType::Driver.to_core(), WorkerType::Driver);
        assert_eq!(PyWorkerType::SpillWorker.to_core(), WorkerType::SpillWorker);
        assert_eq!(
            PyWorkerType::RestoreWorker.to_core(),
            WorkerType::RestoreWorker
        );
    }

    #[test]
    fn test_classify_error_all_variants() {
        // KeyError variants
        let (ty, msg) = classify_error(&CoreWorkerError::ObjectNotFound("obj1".into()));
        assert_eq!(ty, "KeyError");
        assert_eq!(msg, "obj1");

        let (ty, msg) = classify_error(&CoreWorkerError::ActorNotFound("act1".into()));
        assert_eq!(ty, "KeyError");
        assert_eq!(msg, "act1");

        // ValueError variants
        let (ty, msg) = classify_error(&CoreWorkerError::ObjectAlreadyExists("dup".into()));
        assert_eq!(ty, "ValueError");
        assert_eq!(msg, "dup");

        let (ty, msg) = classify_error(&CoreWorkerError::InvalidArgument("bad".into()));
        assert_eq!(ty, "ValueError");
        assert_eq!(msg, "bad");

        // TimeoutError
        let (ty, msg) = classify_error(&CoreWorkerError::TimedOut("slow".into()));
        assert_eq!(ty, "TimeoutError");
        assert_eq!(msg, "slow");

        // RuntimeError variants
        let (ty, _) = classify_error(&CoreWorkerError::NotInitialized);
        assert_eq!(ty, "RuntimeError");

        let (ty, msg) = classify_error(&CoreWorkerError::TaskSubmissionFailed("fail".into()));
        assert_eq!(ty, "RuntimeError");
        assert_eq!(msg, "fail");

        let (ty, msg) = classify_error(&CoreWorkerError::Internal("crash".into()));
        assert_eq!(ty, "RuntimeError");
        assert_eq!(msg, "crash");

        let (ty, msg) = classify_error(&CoreWorkerError::Other("misc".into()));
        assert_eq!(ty, "RuntimeError");
        assert_eq!(msg, "misc");

        // RayStatus
        let ray_err = ray_common::status::RayError::not_found("gone");
        let (ty, _) = classify_error(&CoreWorkerError::RayStatus(ray_err));
        assert_eq!(ty, "RuntimeError");
    }
}
