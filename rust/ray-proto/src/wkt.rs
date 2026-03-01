// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Serde-compatible replacements for protobuf well-known types.
//!
//! Prost's `prost_types` doesn't implement `serde::Serialize`/`Deserialize`.
//! These types are wire-compatible drop-in replacements that add serde support
//! for JSON golden-file conformance testing.
//!
//! Note: `prost::Message` derive generates `Debug` and `Default` impls, so we
//! must not also derive them explicitly.

use serde::{Deserialize, Serialize};

/// Replacement for `prost_types::Timestamp`.
#[derive(Clone, Copy, PartialEq, prost::Message, Serialize, Deserialize)]
pub struct Timestamp {
    #[prost(int64, tag = "1")]
    pub seconds: i64,
    #[prost(int32, tag = "2")]
    pub nanos: i32,
}

/// Replacement for `prost_types::Struct`.
#[derive(Clone, PartialEq, prost::Message, Serialize, Deserialize)]
pub struct Struct {
    #[prost(map = "string, message", tag = "1")]
    pub fields: ::std::collections::HashMap<::prost::alloc::string::String, Value>,
}

/// Replacement for `prost_types::Value`.
#[derive(Clone, PartialEq, prost::Message, Serialize, Deserialize)]
pub struct Value {
    #[prost(oneof = "value::Kind", tags = "1, 2, 3, 4, 5, 6")]
    pub kind: ::core::option::Option<value::Kind>,
}

pub mod value {
    use serde::{Deserialize, Serialize};

    /// Replacement for `prost_types::value::Kind`.
    #[derive(Clone, PartialEq, prost::Oneof, Serialize, Deserialize)]
    pub enum Kind {
        #[prost(enumeration = "super::NullValue", tag = "1")]
        NullValue(i32),
        #[prost(double, tag = "2")]
        NumberValue(f64),
        #[prost(string, tag = "3")]
        StringValue(::prost::alloc::string::String),
        #[prost(bool, tag = "4")]
        BoolValue(bool),
        #[prost(message, tag = "5")]
        StructValue(super::Struct),
        #[prost(message, tag = "6")]
        ListValue(super::ListValue),
    }
}

/// Replacement for `prost_types::ListValue`.
#[derive(Clone, PartialEq, prost::Message, Serialize, Deserialize)]
pub struct ListValue {
    #[prost(message, repeated, tag = "1")]
    pub values: ::prost::alloc::vec::Vec<Value>,
}

/// Replacement for `prost_types::NullValue`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum NullValue {
    NullValue = 0,
}

impl NullValue {
    pub fn as_str_name(&self) -> &'static str {
        match self {
            NullValue::NullValue => "NULL_VALUE",
        }
    }

    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "NULL_VALUE" => Some(Self::NullValue),
            _ => None,
        }
    }
}
