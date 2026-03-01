// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Random ID generators for all Ray entity types.

use ray_common::id::*;

/// Create a random JobID for testing.
pub fn random_job_id() -> JobID {
    JobID::from_int(rand::random::<u16>() as u32 + 1)
}

/// Create a random ActorID for testing.
pub fn random_actor_id() -> ActorID {
    let job_id = random_job_id();
    let task_id = TaskID::from_random();
    ActorID::of(&job_id, &task_id, rand::random::<usize>())
}

/// Create a random TaskID for testing.
pub fn random_task_id() -> TaskID {
    TaskID::from_random()
}

/// Create a random NodeID for testing.
pub fn random_node_id() -> NodeID {
    NodeID::from_random()
}

/// Create a random WorkerID for testing.
pub fn random_worker_id() -> WorkerID {
    WorkerID::from_random()
}

/// Create a random ObjectID for testing.
pub fn random_object_id() -> ObjectID {
    let task_id = random_task_id();
    ObjectID::from_index(&task_id, rand::random::<u16>() as u32 + 1)
}

/// Create a random PlacementGroupID for testing.
pub fn random_placement_group_id() -> PlacementGroupID {
    let job_id = random_job_id();
    PlacementGroupID::of(&job_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_random_job_id() {
        let id = random_job_id();
        assert!(!id.is_nil());
        assert_eq!(id.data().len(), JobID::SIZE);
        assert!(id.to_int() > 0);
    }

    #[test]
    fn test_random_actor_id() {
        let id = random_actor_id();
        assert!(!id.is_nil());
        assert_eq!(id.data().len(), ActorID::SIZE);
        assert!(!id.job_id().is_nil());
    }

    #[test]
    fn test_random_task_id() {
        let id = random_task_id();
        assert!(!id.is_nil());
        assert_eq!(id.data().len(), TaskID::SIZE);
    }

    #[test]
    fn test_random_node_id() {
        let id = random_node_id();
        assert!(!id.is_nil());
        assert_eq!(id.data().len(), NodeID::SIZE);
    }

    #[test]
    fn test_random_worker_id() {
        let id = random_worker_id();
        assert!(!id.is_nil());
        assert_eq!(id.data().len(), WorkerID::SIZE);
    }

    #[test]
    fn test_random_object_id() {
        let id = random_object_id();
        assert!(!id.is_nil());
        assert_eq!(id.data().len(), ObjectID::SIZE);
        assert!(id.object_index() > 0);
    }

    #[test]
    fn test_random_placement_group_id() {
        let id = random_placement_group_id();
        assert!(!id.is_nil());
        assert_eq!(id.data().len(), PlacementGroupID::SIZE);
        assert!(!id.job_id().is_nil());
    }

    #[test]
    fn test_generators_produce_distinct_ids() {
        let id1 = random_node_id();
        let id2 = random_node_id();
        assert_ne!(id1, id2, "two random IDs should be distinct");
    }
}
