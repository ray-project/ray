// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Ray status/error types.
//!
//! Replaces `src/ray/common/status.h/cc`. In Rust, `Status` becomes `Result<T, RayError>`.

use std::fmt;

/// Status codes matching the C++ `StatusCode` enum values exactly.
/// The discriminant values must match for cross-language compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i8)]
pub enum StatusCode {
    OK = 0,
    OutOfMemory = 1,
    KeyError = 2,
    TypeError = 3,
    Invalid = 4,
    IOError = 5,
    UnknownError = 9,
    NotImplemented = 10,
    RedisError = 11,
    TimedOut = 12,
    Interrupted = 13,
    IntentionalSystemExit = 14,
    UnexpectedSystemExit = 15,
    CreationTaskError = 16,
    NotFound = 17,
    Disconnected = 18,
    SchedulingCancelled = 19,
    AlreadyExists = 20,
    ObjectExists = 21,
    ObjectNotFound = 22,
    ObjectAlreadySealed = 23,
    ObjectStoreFull = 24,
    TransientObjectStoreFull = 25,
    OutOfDisk = 28,
    ObjectUnknownOwner = 29,
    RpcError = 30,
    OutOfResource = 31,
    ObjectRefEndOfStream = 32,
    Unauthenticated = 33,
    InvalidArgument = 34,
    ChannelError = 35,
    ChannelTimeoutError = 36,
    PermissionDenied = 37,
}

impl StatusCode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::OK => "OK",
            Self::OutOfMemory => "OutOfMemory",
            Self::KeyError => "KeyError",
            Self::TypeError => "TypeError",
            Self::Invalid => "Invalid",
            Self::IOError => "IOError",
            Self::UnknownError => "UnknownError",
            Self::NotImplemented => "NotImplemented",
            Self::RedisError => "RedisError",
            Self::TimedOut => "TimedOut",
            Self::Interrupted => "Interrupted",
            Self::IntentionalSystemExit => "IntentionalSystemExit",
            Self::UnexpectedSystemExit => "UnexpectedSystemExit",
            Self::CreationTaskError => "CreationTaskError",
            Self::NotFound => "NotFound",
            Self::Disconnected => "Disconnected",
            Self::SchedulingCancelled => "SchedulingCancelled",
            Self::AlreadyExists => "AlreadyExists",
            Self::ObjectExists => "ObjectExists",
            Self::ObjectNotFound => "ObjectNotFound",
            Self::ObjectAlreadySealed => "ObjectAlreadySealed",
            Self::ObjectStoreFull => "ObjectStoreFull",
            Self::TransientObjectStoreFull => "TransientObjectStoreFull",
            Self::OutOfDisk => "OutOfDisk",
            Self::ObjectUnknownOwner => "ObjectUnknownOwner",
            Self::RpcError => "RpcError",
            Self::OutOfResource => "OutOfResource",
            Self::ObjectRefEndOfStream => "ObjectRefEndOfStream",
            Self::Unauthenticated => "Unauthenticated",
            Self::InvalidArgument => "InvalidArgument",
            Self::ChannelError => "ChannelError",
            Self::ChannelTimeoutError => "ChannelTimeoutError",
            Self::PermissionDenied => "PermissionDenied",
        }
    }

    /// Parse a status code from its string name.
    pub fn from_str_name(s: &str) -> Option<Self> {
        match s {
            "OK" => Some(Self::OK),
            "OutOfMemory" => Some(Self::OutOfMemory),
            "KeyError" => Some(Self::KeyError),
            "TypeError" => Some(Self::TypeError),
            "Invalid" => Some(Self::Invalid),
            "IOError" => Some(Self::IOError),
            "UnknownError" => Some(Self::UnknownError),
            "NotImplemented" => Some(Self::NotImplemented),
            "RedisError" => Some(Self::RedisError),
            "TimedOut" => Some(Self::TimedOut),
            "Interrupted" => Some(Self::Interrupted),
            "IntentionalSystemExit" => Some(Self::IntentionalSystemExit),
            "UnexpectedSystemExit" => Some(Self::UnexpectedSystemExit),
            "CreationTaskError" => Some(Self::CreationTaskError),
            "NotFound" => Some(Self::NotFound),
            "Disconnected" => Some(Self::Disconnected),
            "SchedulingCancelled" => Some(Self::SchedulingCancelled),
            "AlreadyExists" => Some(Self::AlreadyExists),
            "ObjectExists" => Some(Self::ObjectExists),
            "ObjectNotFound" => Some(Self::ObjectNotFound),
            "ObjectAlreadySealed" => Some(Self::ObjectAlreadySealed),
            "ObjectStoreFull" => Some(Self::ObjectStoreFull),
            "TransientObjectStoreFull" => Some(Self::TransientObjectStoreFull),
            "OutOfDisk" => Some(Self::OutOfDisk),
            "ObjectUnknownOwner" => Some(Self::ObjectUnknownOwner),
            "RpcError" => Some(Self::RpcError),
            "OutOfResource" => Some(Self::OutOfResource),
            "ObjectRefEndOfStream" => Some(Self::ObjectRefEndOfStream),
            "Unauthenticated" => Some(Self::Unauthenticated),
            "InvalidArgument" => Some(Self::InvalidArgument),
            "ChannelError" => Some(Self::ChannelError),
            "ChannelTimeoutError" => Some(Self::ChannelTimeoutError),
            "PermissionDenied" => Some(Self::PermissionDenied),
            _ => None,
        }
    }
}

impl fmt::Display for StatusCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// The primary error type for Ray operations.
///
/// In C++, `Status` is used with `Status::OK()` and various error factories.
/// In Rust, the idiomatic equivalent is `Result<T, RayError>`.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{code}: {message}")]
pub struct RayError {
    pub code: StatusCode,
    pub message: String,
    /// For RpcError, the gRPC status code.
    pub rpc_code: Option<i32>,
}

impl RayError {
    pub fn new(code: StatusCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            rpc_code: None,
        }
    }

    pub fn with_rpc_code(code: StatusCode, message: impl Into<String>, rpc_code: i32) -> Self {
        Self {
            code,
            message: message.into(),
            rpc_code: Some(rpc_code),
        }
    }

    // Convenience constructors matching C++ static methods
    pub fn out_of_memory(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::OutOfMemory, msg)
    }
    pub fn key_error(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::KeyError, msg)
    }
    pub fn type_error(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::TypeError, msg)
    }
    pub fn invalid(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::Invalid, msg)
    }
    pub fn io_error(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::IOError, msg)
    }
    pub fn not_found(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::NotFound, msg)
    }
    pub fn not_implemented(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::NotImplemented, msg)
    }
    pub fn timed_out(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::TimedOut, msg)
    }
    pub fn disconnected(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::Disconnected, msg)
    }
    pub fn redis_error(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::RedisError, msg)
    }
    pub fn rpc_error(msg: impl Into<String>, rpc_code: i32) -> Self {
        Self::with_rpc_code(StatusCode::RpcError, msg, rpc_code)
    }
    pub fn object_store_full(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::ObjectStoreFull, msg)
    }
    pub fn object_not_found(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::ObjectNotFound, msg)
    }
    pub fn already_exists(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::AlreadyExists, msg)
    }
    pub fn unauthenticated(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::Unauthenticated, msg)
    }
    pub fn permission_denied(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::PermissionDenied, msg)
    }
    pub fn interrupted(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::Interrupted, msg)
    }
    pub fn invalid_argument(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::InvalidArgument, msg)
    }
    pub fn channel_error(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::ChannelError, msg)
    }

    // Status code predicates
    pub fn is_out_of_memory(&self) -> bool {
        self.code == StatusCode::OutOfMemory
    }
    pub fn is_key_error(&self) -> bool {
        self.code == StatusCode::KeyError
    }
    pub fn is_not_found(&self) -> bool {
        self.code == StatusCode::NotFound
    }
    pub fn is_timed_out(&self) -> bool {
        self.code == StatusCode::TimedOut
    }
    pub fn is_disconnected(&self) -> bool {
        self.code == StatusCode::Disconnected
    }
    pub fn is_rpc_error(&self) -> bool {
        self.code == StatusCode::RpcError
    }
    pub fn is_object_store_full(&self) -> bool {
        self.code == StatusCode::ObjectStoreFull
    }
}

/// Convenience type alias: `Result<T, RayError>`.
/// This is the Rust equivalent of C++'s `StatusOr<T>`.
pub type RayResult<T> = Result<T, RayError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_status_code_roundtrip() {
        let code = StatusCode::OutOfMemory;
        assert_eq!(code.as_str(), "OutOfMemory");
        assert_eq!(StatusCode::from_str_name("OutOfMemory"), Some(code));
    }

    #[test]
    fn test_ray_error_display() {
        let err = RayError::io_error("disk full");
        assert_eq!(err.to_string(), "IOError: disk full");
    }

    #[test]
    fn test_ray_result() {
        let ok: RayResult<i32> = Ok(42);
        assert!(ok.is_ok());

        let err: RayResult<i32> = Err(RayError::not_found("item"));
        assert!(err.is_err());
        assert!(err.unwrap_err().is_not_found());
    }

    #[test]
    fn test_rpc_error_code() {
        let err = RayError::rpc_error("connection refused", 14);
        assert!(err.is_rpc_error());
        assert_eq!(err.rpc_code, Some(14));
    }

    #[test]
    fn test_all_error_factory_methods() {
        let cases: Vec<(RayError, StatusCode, &str)> = vec![
            (RayError::out_of_memory("oom"), StatusCode::OutOfMemory, "oom"),
            (RayError::key_error("key"), StatusCode::KeyError, "key"),
            (RayError::type_error("type"), StatusCode::TypeError, "type"),
            (RayError::invalid("inv"), StatusCode::Invalid, "inv"),
            (RayError::io_error("io"), StatusCode::IOError, "io"),
            (RayError::not_found("nf"), StatusCode::NotFound, "nf"),
            (RayError::not_implemented("ni"), StatusCode::NotImplemented, "ni"),
            (RayError::timed_out("to"), StatusCode::TimedOut, "to"),
            (RayError::disconnected("dc"), StatusCode::Disconnected, "dc"),
            (RayError::redis_error("redis"), StatusCode::RedisError, "redis"),
            (RayError::object_store_full("full"), StatusCode::ObjectStoreFull, "full"),
            (RayError::object_not_found("onf"), StatusCode::ObjectNotFound, "onf"),
            (RayError::already_exists("ae"), StatusCode::AlreadyExists, "ae"),
            (RayError::unauthenticated("ua"), StatusCode::Unauthenticated, "ua"),
            (RayError::permission_denied("pd"), StatusCode::PermissionDenied, "pd"),
            (RayError::interrupted("int"), StatusCode::Interrupted, "int"),
            (RayError::invalid_argument("ia"), StatusCode::InvalidArgument, "ia"),
            (RayError::channel_error("ch"), StatusCode::ChannelError, "ch"),
        ];
        for (err, expected_code, expected_msg) in cases {
            assert_eq!(err.code, expected_code);
            assert!(err.message.contains(expected_msg));
            assert!(err.rpc_code.is_none());
        }
    }

    #[test]
    fn test_all_predicates() {
        assert!(RayError::out_of_memory("x").is_out_of_memory());
        assert!(!RayError::out_of_memory("x").is_key_error());

        assert!(RayError::key_error("x").is_key_error());
        assert!(!RayError::key_error("x").is_not_found());

        assert!(RayError::not_found("x").is_not_found());
        assert!(RayError::timed_out("x").is_timed_out());
        assert!(RayError::disconnected("x").is_disconnected());
        assert!(RayError::rpc_error("x", 0).is_rpc_error());
        assert!(RayError::object_store_full("x").is_object_store_full());
    }

    #[test]
    fn test_status_code_as_str_all_variants() {
        let all_codes = vec![
            (StatusCode::OK, "OK"),
            (StatusCode::OutOfMemory, "OutOfMemory"),
            (StatusCode::KeyError, "KeyError"),
            (StatusCode::TypeError, "TypeError"),
            (StatusCode::Invalid, "Invalid"),
            (StatusCode::IOError, "IOError"),
            (StatusCode::UnknownError, "UnknownError"),
            (StatusCode::NotImplemented, "NotImplemented"),
            (StatusCode::RedisError, "RedisError"),
            (StatusCode::TimedOut, "TimedOut"),
            (StatusCode::Interrupted, "Interrupted"),
            (StatusCode::IntentionalSystemExit, "IntentionalSystemExit"),
            (StatusCode::UnexpectedSystemExit, "UnexpectedSystemExit"),
            (StatusCode::CreationTaskError, "CreationTaskError"),
            (StatusCode::NotFound, "NotFound"),
            (StatusCode::Disconnected, "Disconnected"),
            (StatusCode::SchedulingCancelled, "SchedulingCancelled"),
            (StatusCode::AlreadyExists, "AlreadyExists"),
            (StatusCode::ObjectExists, "ObjectExists"),
            (StatusCode::ObjectNotFound, "ObjectNotFound"),
            (StatusCode::ObjectAlreadySealed, "ObjectAlreadySealed"),
            (StatusCode::ObjectStoreFull, "ObjectStoreFull"),
            (StatusCode::TransientObjectStoreFull, "TransientObjectStoreFull"),
            (StatusCode::OutOfDisk, "OutOfDisk"),
            (StatusCode::ObjectUnknownOwner, "ObjectUnknownOwner"),
            (StatusCode::RpcError, "RpcError"),
            (StatusCode::OutOfResource, "OutOfResource"),
            (StatusCode::ObjectRefEndOfStream, "ObjectRefEndOfStream"),
            (StatusCode::Unauthenticated, "Unauthenticated"),
            (StatusCode::InvalidArgument, "InvalidArgument"),
            (StatusCode::ChannelError, "ChannelError"),
            (StatusCode::ChannelTimeoutError, "ChannelTimeoutError"),
            (StatusCode::PermissionDenied, "PermissionDenied"),
        ];
        for (code, expected_str) in &all_codes {
            assert_eq!(code.as_str(), *expected_str);
        }
    }

    #[test]
    fn test_status_code_from_str_roundtrip_all() {
        let all_names = vec![
            "OK", "OutOfMemory", "KeyError", "TypeError", "Invalid", "IOError",
            "UnknownError", "NotImplemented", "RedisError", "TimedOut", "Interrupted",
            "IntentionalSystemExit", "UnexpectedSystemExit", "CreationTaskError",
            "NotFound", "Disconnected", "SchedulingCancelled", "AlreadyExists",
            "ObjectExists", "ObjectNotFound", "ObjectAlreadySealed", "ObjectStoreFull",
            "TransientObjectStoreFull", "OutOfDisk", "ObjectUnknownOwner", "RpcError",
            "OutOfResource", "ObjectRefEndOfStream", "Unauthenticated", "InvalidArgument",
            "ChannelError", "ChannelTimeoutError", "PermissionDenied",
        ];
        for name in all_names {
            let code = StatusCode::from_str_name(name).unwrap_or_else(|| panic!("failed to parse {name}"));
            assert_eq!(code.as_str(), name);
        }
        assert_eq!(StatusCode::from_str_name("Bogus"), None);
    }

    #[test]
    fn test_status_code_display() {
        assert_eq!(format!("{}", StatusCode::TimedOut), "TimedOut");
        assert_eq!(format!("{}", StatusCode::OK), "OK");
    }

    #[test]
    fn test_ray_error_with_rpc_code() {
        let err = RayError::with_rpc_code(StatusCode::RpcError, "timeout", 4);
        assert_eq!(err.code, StatusCode::RpcError);
        assert_eq!(err.rpc_code, Some(4));
        assert_eq!(err.message, "timeout");
    }

    #[test]
    fn test_ray_error_clone() {
        let err = RayError::io_error("cloneable");
        let cloned = err.clone();
        assert_eq!(err.code, cloned.code);
        assert_eq!(err.message, cloned.message);
    }
}
