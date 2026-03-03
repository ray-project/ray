// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Serialization bridge between Python objects and Ray's object protocol.
//!
//! Ray stores objects as (data, metadata) pairs in the object store.
//! The metadata encodes the serialization format so the receiver knows
//! how to deserialize:
//!
//! - `PICKLE5` — Python objects serialized with cloudpickle
//! - `RAW` — raw bytes (user explicitly stored bytes)
//! - `ARROW` — Apache Arrow format (numpy arrays, pandas DataFrames)
//!
//! This module provides the Rust-side constants and helpers for working
//! with these metadata tags. The actual pickle/unpickle is done in Python
//! via `cloudpickle`; this module just handles the metadata protocol.

/// Metadata tag for cloudpickle-serialized Python objects.
pub const METADATA_PICKLE5: &[u8] = b"PICKLE5";

/// Metadata tag for raw bytes.
pub const METADATA_RAW: &[u8] = b"RAW";

/// Metadata tag for Apache Arrow format (numpy, pandas).
pub const METADATA_ARROW: &[u8] = b"ARROW";

/// Metadata tag for error objects.
pub const METADATA_ERROR: &[u8] = b"ERROR";

/// Object serialization format, determined by metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerializationFormat {
    /// Python cloudpickle format.
    Pickle5,
    /// Raw bytes — no deserialization needed.
    Raw,
    /// Apache Arrow IPC format.
    Arrow,
    /// Error object — contains serialized exception.
    Error,
    /// Unknown metadata tag.
    Unknown,
}

impl SerializationFormat {
    /// Determine the format from metadata bytes.
    pub fn from_metadata(metadata: &[u8]) -> Self {
        if metadata == METADATA_PICKLE5 {
            SerializationFormat::Pickle5
        } else if metadata == METADATA_RAW {
            SerializationFormat::Raw
        } else if metadata == METADATA_ARROW {
            SerializationFormat::Arrow
        } else if metadata == METADATA_ERROR {
            SerializationFormat::Error
        } else if metadata.is_empty() {
            // Default: empty metadata means pickle5 (Python convention).
            SerializationFormat::Pickle5
        } else {
            SerializationFormat::Unknown
        }
    }

    /// Return the metadata bytes for this format.
    pub fn to_metadata(&self) -> &'static [u8] {
        match self {
            SerializationFormat::Pickle5 => METADATA_PICKLE5,
            SerializationFormat::Raw => METADATA_RAW,
            SerializationFormat::Arrow => METADATA_ARROW,
            SerializationFormat::Error => METADATA_ERROR,
            SerializationFormat::Unknown => b"",
        }
    }

    /// Whether this format requires Python deserialization (pickle/arrow).
    pub fn requires_python_deser(&self) -> bool {
        matches!(
            self,
            SerializationFormat::Pickle5 | SerializationFormat::Arrow | SerializationFormat::Error
        )
    }
}

/// Wrap raw bytes as a Ray object with `RAW` metadata.
pub fn wrap_raw_bytes(data: &[u8]) -> (Vec<u8>, Vec<u8>) {
    (data.to_vec(), METADATA_RAW.to_vec())
}

/// Wrap serialized Python object bytes with `PICKLE5` metadata.
pub fn wrap_pickle(data: &[u8]) -> (Vec<u8>, Vec<u8>) {
    (data.to_vec(), METADATA_PICKLE5.to_vec())
}

/// Wrap an error message with `ERROR` metadata.
pub fn wrap_error(data: &[u8]) -> (Vec<u8>, Vec<u8>) {
    (data.to_vec(), METADATA_ERROR.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_from_metadata() {
        assert_eq!(
            SerializationFormat::from_metadata(METADATA_PICKLE5),
            SerializationFormat::Pickle5,
        );
        assert_eq!(
            SerializationFormat::from_metadata(METADATA_RAW),
            SerializationFormat::Raw,
        );
        assert_eq!(
            SerializationFormat::from_metadata(METADATA_ARROW),
            SerializationFormat::Arrow,
        );
        assert_eq!(
            SerializationFormat::from_metadata(METADATA_ERROR),
            SerializationFormat::Error,
        );
        assert_eq!(
            SerializationFormat::from_metadata(b""),
            SerializationFormat::Pickle5,
        );
        assert_eq!(
            SerializationFormat::from_metadata(b"CUSTOM"),
            SerializationFormat::Unknown,
        );
    }

    #[test]
    fn test_format_roundtrip() {
        for fmt in [
            SerializationFormat::Pickle5,
            SerializationFormat::Raw,
            SerializationFormat::Arrow,
            SerializationFormat::Error,
        ] {
            let meta = fmt.to_metadata();
            assert_eq!(SerializationFormat::from_metadata(meta), fmt);
        }
    }

    #[test]
    fn test_requires_python_deser() {
        assert!(SerializationFormat::Pickle5.requires_python_deser());
        assert!(!SerializationFormat::Raw.requires_python_deser());
        assert!(SerializationFormat::Arrow.requires_python_deser());
        assert!(SerializationFormat::Error.requires_python_deser());
        assert!(!SerializationFormat::Unknown.requires_python_deser());
    }

    #[test]
    fn test_wrap_raw_bytes() {
        let (data, meta) = wrap_raw_bytes(b"hello");
        assert_eq!(data, b"hello");
        assert_eq!(meta, METADATA_RAW);
    }

    #[test]
    fn test_wrap_pickle() {
        let (data, meta) = wrap_pickle(b"\x80\x05\x95");
        assert_eq!(data, b"\x80\x05\x95");
        assert_eq!(meta, METADATA_PICKLE5);
    }

    #[test]
    fn test_wrap_error() {
        let (data, meta) = wrap_error(b"traceback...");
        assert_eq!(data, b"traceback...");
        assert_eq!(meta, METADATA_ERROR);
    }
}
