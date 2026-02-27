// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Actor handle wrapping the protobuf `ActorHandle` message.

use std::sync::atomic::{AtomicU64, Ordering};

use prost::Message;

use ray_common::id::ActorID;
use ray_proto::ray::rpc::{self, Address};

use crate::error::{CoreWorkerError, CoreWorkerResult};

/// A handle to a remote actor.
///
/// Wraps the protobuf `ActorHandle` and adds a local sequence counter
/// for ordering actor task submissions.
pub struct ActorHandle {
    inner: rpc::ActorHandle,
    task_counter: AtomicU64,
}

impl ActorHandle {
    /// Create from a protobuf `ActorHandle`.
    pub fn from_proto(proto: rpc::ActorHandle) -> Self {
        Self {
            inner: proto,
            task_counter: AtomicU64::new(0),
        }
    }

    /// Convert to a protobuf `ActorHandle`.
    pub fn to_proto(&self) -> rpc::ActorHandle {
        self.inner.clone()
    }

    /// The actor's ID.
    pub fn actor_id(&self) -> ActorID {
        ActorID::from_binary(&self.inner.actor_id)
    }

    /// The address of the actor's owner.
    pub fn owner_address(&self) -> Option<&Address> {
        self.inner.owner_address.as_ref()
    }

    /// The actor's registered name (empty if anonymous).
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// The actor's Ray namespace.
    pub fn ray_namespace(&self) -> &str {
        &self.inner.ray_namespace
    }

    /// Max task retries for this actor.
    pub fn max_task_retries(&self) -> i64 {
        self.inner.max_task_retries
    }

    /// Whether this actor is detached (lifetime independent of creator).
    pub fn is_detached(&self) -> bool {
        self.inner.is_detached
    }

    /// Get the next task sequence number (atomically incremented).
    pub fn next_task_sequence_number(&self) -> u64 {
        self.task_counter.fetch_add(1, Ordering::Relaxed)
    }

    /// Serialize the handle to bytes via protobuf encoding.
    pub fn serialize(&self) -> Vec<u8> {
        self.inner.encode_to_vec()
    }

    /// Deserialize from protobuf bytes.
    pub fn deserialize(data: &[u8]) -> CoreWorkerResult<Self> {
        let proto = rpc::ActorHandle::decode(data)
            .map_err(|e| CoreWorkerError::Internal(format!("failed to decode ActorHandle: {e}")))?;
        Ok(Self::from_proto(proto))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_proto_handle() -> rpc::ActorHandle {
        let actor_id = ActorID::from_random();
        rpc::ActorHandle {
            actor_id: actor_id.binary(),
            name: "test_actor".to_string(),
            ray_namespace: "default".to_string(),
            max_task_retries: 3,
            is_detached: true,
            owner_address: Some(Address {
                node_id: vec![0u8; 28],
                ip_address: "10.0.0.1".to_string(),
                port: 5000,
                worker_id: vec![0u8; 28],
            }),
            ..Default::default()
        }
    }

    #[test]
    fn test_actor_handle_basic() {
        let proto = make_proto_handle();
        let aid = ActorID::from_binary(&proto.actor_id);
        let handle = ActorHandle::from_proto(proto);
        assert_eq!(handle.actor_id(), aid);
        assert_eq!(handle.name(), "test_actor");
        assert_eq!(handle.ray_namespace(), "default");
        assert_eq!(handle.max_task_retries(), 3);
        assert!(handle.is_detached());
        assert!(handle.owner_address().is_some());
    }

    #[test]
    fn test_task_sequence_numbers() {
        let handle = ActorHandle::from_proto(make_proto_handle());
        assert_eq!(handle.next_task_sequence_number(), 0);
        assert_eq!(handle.next_task_sequence_number(), 1);
        assert_eq!(handle.next_task_sequence_number(), 2);
    }

    #[test]
    fn test_serialize_deserialize_all_fields() {
        let handle = ActorHandle::from_proto(make_proto_handle());
        // Advance the counter before serialization.
        handle.next_task_sequence_number();
        handle.next_task_sequence_number();

        let data = handle.serialize();
        let handle2 = ActorHandle::deserialize(&data).unwrap();
        // All protobuf fields must survive roundtrip.
        assert_eq!(handle.actor_id(), handle2.actor_id());
        assert_eq!(handle.name(), handle2.name());
        assert_eq!(handle.ray_namespace(), handle2.ray_namespace());
        assert_eq!(handle.max_task_retries(), handle2.max_task_retries());
        assert_eq!(handle.is_detached(), handle2.is_detached());
        let addr1 = handle.owner_address().unwrap();
        let addr2 = handle2.owner_address().unwrap();
        assert_eq!(addr1.ip_address, addr2.ip_address);
        assert_eq!(addr1.port, addr2.port);
        // task_counter is NOT serialized — deserialized handle should start at 0.
        assert_eq!(handle2.next_task_sequence_number(), 0);
    }

    #[test]
    fn test_deserialize_invalid_bytes() {
        let result = ActorHandle::deserialize(b"not valid protobuf");
        assert!(result.is_err());
        // Use match instead of unwrap_err (ActorHandle doesn't impl Debug).
        match result {
            Err(CoreWorkerError::Internal(msg)) => {
                assert!(msg.contains("failed to decode"), "unexpected message: {msg}");
            }
            _ => panic!("expected CoreWorkerError::Internal"),
        }
    }
}
