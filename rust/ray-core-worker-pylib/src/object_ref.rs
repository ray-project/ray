// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! ObjectRef wrapper: an ObjectID with optional owner info and call site.

use std::hash::{Hash, Hasher};

use ray_common::id::ObjectID;
use ray_proto::ray::rpc::Address;

/// A reference to a Ray object, combining the object ID with optional
/// owner address and call site information.
#[derive(Debug, Clone)]
pub struct PyObjectRef {
    object_id: ObjectID,
    owner_address: Option<Address>,
    call_site: String,
}

impl PyObjectRef {
    pub fn new(object_id: ObjectID, owner_address: Option<Address>, call_site: String) -> Self {
        Self {
            object_id,
            owner_address,
            call_site,
        }
    }

    pub fn object_id(&self) -> &ObjectID {
        &self.object_id
    }

    pub fn owner_address(&self) -> Option<&Address> {
        self.owner_address.as_ref()
    }

    pub fn call_site(&self) -> &str {
        &self.call_site
    }

    pub fn binary(&self) -> Vec<u8> {
        self.object_id.binary()
    }

    pub fn hex(&self) -> String {
        self.object_id.hex()
    }

    pub fn is_nil(&self) -> bool {
        self.object_id.is_nil()
    }

    pub fn repr(&self) -> String {
        format!("ObjectRef({})", self.object_id.hex())
    }
}

impl PartialEq for PyObjectRef {
    fn eq(&self, other: &Self) -> bool {
        self.object_id == other.object_id
    }
}

impl Eq for PyObjectRef {}

impl Hash for PyObjectRef {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.object_id.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_ref_basic() {
        let oid = ObjectID::from_random();
        let oref = PyObjectRef::new(oid, None, "test.py:10".into());
        assert_eq!(*oref.object_id(), oid);
        assert_eq!(oref.call_site(), "test.py:10");
        assert!(!oref.is_nil());
        assert!(oref.repr().contains(&oid.hex()));
        assert!(oref.owner_address().is_none());
        // binary/hex delegate to the inner ObjectID.
        assert_eq!(oref.binary(), oid.binary());
        assert_eq!(oref.hex(), oid.hex());
    }

    #[test]
    fn test_object_ref_with_owner_address() {
        let oid = ObjectID::from_random();
        let addr = Address {
            node_id: vec![0u8; 28],
            ip_address: "10.0.0.1".to_string(),
            port: 8080,
            worker_id: vec![0u8; 28],
        };
        let oref = PyObjectRef::new(oid, Some(addr.clone()), "caller.py:5".into());
        let owner = oref.owner_address().unwrap();
        assert_eq!(owner.ip_address, "10.0.0.1");
        assert_eq!(owner.port, 8080);
    }

    #[test]
    fn test_object_ref_equality_same_id() {
        let oid = ObjectID::from_random();
        let a = PyObjectRef::new(oid, None, "a.py:1".into());
        let b = PyObjectRef::new(oid, None, "b.py:2".into());
        // Equality is based on object_id only.
        assert_eq!(a, b);
    }

    #[test]
    fn test_object_ref_inequality_different_id() {
        let a = PyObjectRef::new(ObjectID::from_random(), None, "a.py:1".into());
        let b = PyObjectRef::new(ObjectID::from_random(), None, "a.py:1".into());
        assert_ne!(a, b);
    }

    #[test]
    fn test_object_ref_hash_consistent_with_eq() {
        use std::collections::HashSet;
        let oid = ObjectID::from_random();
        let a = PyObjectRef::new(oid, None, "x.py:1".into());
        let b = PyObjectRef::new(oid, None, "y.py:2".into());
        let mut set = HashSet::new();
        set.insert(a);
        assert!(set.contains(&b));
    }
}
