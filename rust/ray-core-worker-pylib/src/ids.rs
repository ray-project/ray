// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! PyO3-compatible ID type wrappers.
//!
//! Each wrapper provides `__new__`, `nil`, `from_hex`, `from_random`, `binary`,
//! `hex`, `is_nil`, `size`, `__repr__`, `__eq__`, `__hash__`.

use ray_common::id;
use std::hash::{Hash, Hasher};

/// Macro to generate a Python-facing wrapper for a Ray ID type.
macro_rules! py_id_wrapper {
    ($py_name:ident, $inner:ty) => {
        #[derive(Debug, Clone)]
        pub struct $py_name {
            inner: $inner,
        }

        impl $py_name {
            pub fn new(binary: &[u8]) -> Self {
                Self {
                    inner: <$inner>::from_binary(binary),
                }
            }

            pub fn nil() -> Self {
                Self {
                    inner: <$inner>::nil(),
                }
            }

            pub fn from_hex(hex_str: &str) -> Self {
                Self {
                    inner: <$inner>::from_hex(hex_str),
                }
            }

            pub fn from_random() -> Self {
                Self {
                    inner: <$inner>::from_random(),
                }
            }

            pub fn binary(&self) -> Vec<u8> {
                self.inner.binary()
            }

            pub fn hex(&self) -> String {
                self.inner.hex()
            }

            pub fn is_nil(&self) -> bool {
                self.inner.is_nil()
            }

            pub fn size(&self) -> usize {
                <$inner>::SIZE
            }

            pub fn repr(&self) -> String {
                format!("{}({})", stringify!($py_name), self.inner.hex())
            }

            pub fn inner(&self) -> &$inner {
                &self.inner
            }

            pub fn into_inner(self) -> $inner {
                self.inner
            }

            pub fn from_inner(inner: $inner) -> Self {
                Self { inner }
            }
        }

        impl PartialEq for $py_name {
            fn eq(&self, other: &Self) -> bool {
                self.inner == other.inner
            }
        }

        impl Eq for $py_name {}

        impl Hash for $py_name {
            fn hash<H: Hasher>(&self, state: &mut H) {
                self.inner.hash(state);
            }
        }
    };
}

py_id_wrapper!(PyObjectID, id::ObjectID);
py_id_wrapper!(PyTaskID, id::TaskID);
py_id_wrapper!(PyActorID, id::ActorID);
py_id_wrapper!(PyJobID, id::JobID);
py_id_wrapper!(PyWorkerID, id::WorkerID);
py_id_wrapper!(PyNodeID, id::NodeID);
py_id_wrapper!(PyPlacementGroupID, id::PlacementGroupID);

// Extra methods for PyJobID.
impl PyJobID {
    pub fn from_int(value: u32) -> Self {
        Self {
            inner: id::JobID::from_int(value),
        }
    }

    pub fn to_int(&self) -> u32 {
        self.inner.to_int()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_id_roundtrip() {
        let oid = PyObjectID::from_random();
        assert!(!oid.is_nil());
        let binary = oid.binary();
        let oid2 = PyObjectID::new(&binary);
        assert_eq!(oid, oid2);
        assert_eq!(oid.hex(), oid2.hex());
    }

    #[test]
    fn test_nil_id() {
        let oid = PyObjectID::nil();
        assert!(oid.is_nil());
        assert_eq!(oid.size(), id::ObjectID::SIZE);
    }

    #[test]
    fn test_from_hex() {
        let oid = PyObjectID::from_random();
        let hex = oid.hex();
        let oid2 = PyObjectID::from_hex(&hex);
        assert_eq!(oid, oid2);
    }

    #[test]
    fn test_job_id_int_conversion() {
        let jid = PyJobID::from_int(42);
        assert_eq!(jid.to_int(), 42);
        assert!(!jid.is_nil());
    }

    #[test]
    fn test_repr() {
        let tid = PyTaskID::from_random();
        let r = tid.repr();
        assert!(r.starts_with("PyTaskID("));
        assert!(r.ends_with(')'));
    }

    #[test]
    fn test_hash_equality() {
        use std::collections::HashSet;
        let oid = PyObjectID::from_random();
        let oid2 = PyObjectID::new(&oid.binary());
        let mut set = HashSet::new();
        set.insert(oid.clone());
        assert!(set.contains(&oid2));
    }
}
