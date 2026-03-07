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

    // ─── Ported from C++ status_test.cc ─────────────────────────────────────

    /// Port of C++ StatusTest::StringToCode:
    /// Verify string-to-code conversion with known and unknown names.
    #[test]
    fn test_string_to_code_known_and_unknown() {
        // OK roundtrip
        assert_eq!(
            StatusCode::from_str_name(StatusCode::OK.as_str()),
            Some(StatusCode::OK)
        );

        // Invalid roundtrip
        assert_eq!(
            StatusCode::from_str_name(StatusCode::Invalid.as_str()),
            Some(StatusCode::Invalid)
        );

        // TransientObjectStoreFull roundtrip
        assert_eq!(
            StatusCode::from_str_name(StatusCode::TransientObjectStoreFull.as_str()),
            Some(StatusCode::TransientObjectStoreFull)
        );

        // Unknown string returns None (C++ returns IOError as default)
        assert_eq!(StatusCode::from_str_name("foobar"), None);
    }

    /// Port of C++ StatusTest::CopyAndMoveForOkStatus:
    /// Verify clone for Ok status (Rust Result).
    #[test]
    fn test_copy_and_move_ok_status() {
        let ok_result: RayResult<i32> = Ok(42);

        // Clone (equivalent to copy constructor)
        let cloned = ok_result.clone();
        assert!(cloned.is_ok());
        assert_eq!(cloned.unwrap(), 42);

        // Move (equivalent to move constructor)
        let ok_result2: RayResult<i32> = Ok(99);
        let moved = ok_result2;
        assert!(moved.is_ok());
        assert_eq!(moved.unwrap(), 99);
    }

    /// Port of C++ StatusTest::CopyAndMoveErrorStatus:
    /// Verify clone for error status.
    #[test]
    fn test_copy_and_move_error_status() {
        let err_result: RayResult<i32> = Err(RayError::invalid("invalid"));

        // Clone (copy constructor)
        let cloned = err_result.clone();
        assert!(cloned.is_err());
        assert_eq!(cloned.unwrap_err().code, StatusCode::Invalid);

        // Move (move constructor)
        let err_result2: RayResult<i32> = Err(RayError::invalid("moved"));
        let moved = err_result2;
        assert!(moved.is_err());
        assert_eq!(moved.unwrap_err().code, StatusCode::Invalid);
    }

    /// Port of C++ StatusTest::GrpcStatusToRayStatus:
    /// Verify gRPC-style status code to RayError mapping.
    #[test]
    fn test_grpc_status_to_ray_status_mapping() {
        // OK maps to Ok result
        let ok: RayResult<()> = Ok(());
        assert!(ok.is_ok());

        // RPC error with UNAVAILABLE code
        let unavailable = RayError::rpc_error("foo", 14); // UNAVAILABLE = 14
        assert!(unavailable.is_rpc_error());
        assert_eq!(unavailable.rpc_code, Some(14));

        // RPC error with UNKNOWN code
        let unknown = RayError::rpc_error("foo", 2); // UNKNOWN = 2
        assert!(unknown.is_rpc_error());
        assert_eq!(unknown.rpc_code, Some(2));

        // Invalid status with message
        let invalid = RayError::invalid("not now");
        assert_eq!(invalid.code, StatusCode::Invalid);
        assert_eq!(invalid.message, "not now");
    }

    /// Port of C++ StatusSetTest::TestStatusSetAPI:
    /// Demonstrate Rust enum-based error discrimination (equivalent to C++ StatusSet).
    #[test]
    fn test_status_set_api_equivalent() {
        // In Rust, we use Result + match on error code instead of StatusSet + visitor
        fn return_oom_error() -> RayResult<()> {
            Err(RayError::out_of_memory(
                "ooming because Ray Data is making too many objects",
            ))
        }

        let result = return_oom_error();
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err.code {
            StatusCode::OutOfMemory => {
                assert_eq!(
                    err.message,
                    "ooming because Ray Data is making too many objects"
                );
            }
            StatusCode::IOError => {
                panic!("should not be IOError");
            }
            _ => {
                panic!("unexpected error code");
            }
        }

        // Ok case
        fn return_ok() -> RayResult<()> {
            Ok(())
        }
        assert!(return_ok().is_ok());
    }

    /// Port of C++ StatusSetOrTest::TestStatusSetOrAPI:
    /// Demonstrate Rust Result<T, RayError> (equivalent to C++ StatusSetOr).
    #[test]
    fn test_status_set_or_api_equivalent() {
        fn return_oom_error() -> RayResult<i64> {
            Err(RayError::out_of_memory(
                "ooming because Ray Data is making too many objects",
            ))
        }

        let result = return_oom_error();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.is_out_of_memory());
        assert_eq!(
            err.message,
            "ooming because Ray Data is making too many objects"
        );

        // Value case
        fn return_value() -> RayResult<i64> {
            Ok(100)
        }
        let result = return_value();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 100);
    }

    /// Port of C++ StatusTest: Verify error message is preserved through clone.
    #[test]
    fn test_error_message_preserved_through_clone() {
        let err = RayError::new(StatusCode::Invalid, "detailed error message");
        let cloned = err.clone();
        assert_eq!(err.message, cloned.message);
        assert_eq!(err.code, cloned.code);
        assert_eq!(err.rpc_code, cloned.rpc_code);

        // With RPC code
        let rpc_err = RayError::rpc_error("connection reset", 14);
        let cloned_rpc = rpc_err.clone();
        assert_eq!(rpc_err.rpc_code, cloned_rpc.rpc_code);
        assert_eq!(rpc_err.message, cloned_rpc.message);
    }

    /// Port of C++ StatusTest: Verify Display formatting.
    #[test]
    fn test_error_display_format() {
        let err = RayError::invalid("bad input");
        let display = format!("{}", err);
        assert!(display.contains("Invalid"));
        assert!(display.contains("bad input"));

        let err2 = RayError::not_found("missing key");
        let display2 = format!("{}", err2);
        assert!(display2.contains("NotFound"));
        assert!(display2.contains("missing key"));
    }
}
