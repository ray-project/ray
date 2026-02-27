// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Builder pattern for constructing `TaskSpec` protobuf messages.

use ray_common::id::{ActorID, JobID, ObjectID, TaskID};
use ray_proto::ray::rpc::{
    self, ActorCreationTaskSpec, ActorTaskSpec, Address, FunctionDescriptor, TaskArg, TaskSpec,
};

/// Builder for constructing a `TaskSpec` protobuf message.
pub struct TaskSpecBuilder {
    spec: TaskSpec,
}

impl TaskSpecBuilder {
    pub fn new() -> Self {
        Self {
            spec: TaskSpec::default(),
        }
    }

    /// Set common fields shared by all task types.
    #[allow(clippy::too_many_arguments)]
    pub fn set_common_task_spec(
        &mut self,
        task_id: &TaskID,
        name: String,
        language: i32,
        function_descriptor: FunctionDescriptor,
        job_id: &JobID,
        parent_task_id: &TaskID,
        parent_counter: u64,
        caller_id: Vec<u8>,
        caller_address: Address,
        num_returns: u64,
    ) -> &mut Self {
        self.spec.task_id = task_id.binary();
        self.spec.name = name;
        self.spec.language = language;
        self.spec.function_descriptor = Some(function_descriptor);
        self.spec.job_id = job_id.binary();
        self.spec.parent_task_id = parent_task_id.binary();
        self.spec.parent_counter = parent_counter;
        self.spec.caller_id = caller_id;
        self.spec.caller_address = Some(caller_address);
        self.spec.num_returns = num_returns;
        self
    }

    /// Configure as a normal (non-actor) task.
    pub fn set_normal_task_spec(&mut self) -> &mut Self {
        self.spec.r#type = rpc::TaskType::NormalTask as i32;
        self
    }

    /// Configure as an actor creation task.
    #[allow(clippy::too_many_arguments)]
    pub fn set_actor_creation_task_spec(
        &mut self,
        actor_id: &ActorID,
        max_restarts: i64,
        max_task_retries: i64,
        max_concurrency: i32,
        is_detached: bool,
        name: String,
        ray_namespace: String,
    ) -> &mut Self {
        self.spec.r#type = rpc::TaskType::ActorCreationTask as i32;
        self.spec.actor_creation_task_spec = Some(ActorCreationTaskSpec {
            actor_id: actor_id.binary(),
            max_actor_restarts: max_restarts,
            max_task_retries,
            max_concurrency,
            is_detached,
            name,
            ray_namespace,
            ..Default::default()
        });
        self
    }

    /// Configure as an actor task.
    pub fn set_actor_task_spec(
        &mut self,
        actor_id: &ActorID,
        actor_creation_dummy_object_id: &ObjectID,
        sequence_number: u64,
    ) -> &mut Self {
        self.spec.r#type = rpc::TaskType::ActorTask as i32;
        self.spec.actor_task_spec = Some(ActorTaskSpec {
            actor_id: actor_id.binary(),
            actor_creation_dummy_object_id: actor_creation_dummy_object_id.binary(),
            sequence_number,
        });
        self
    }

    /// Add a task argument.
    pub fn add_arg(&mut self, arg: TaskArg) -> &mut Self {
        self.spec.args.push(arg);
        self
    }

    /// Add a return object ID (appended to `return_ids` in the spec).
    pub fn add_return_id(&mut self, object_id: &ObjectID) -> &mut Self {
        // TaskSpec doesn't have a separate return_ids field in all proto versions;
        // num_returns is the primary mechanism. This is a convenience for tracking.
        let _ = object_id;
        self.spec.num_returns += 1;
        self
    }

    /// Build the final `TaskSpec`.
    pub fn build(self) -> TaskSpec {
        self.spec
    }
}

impl Default for TaskSpecBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_normal_task() {
        let tid = TaskID::from_random();
        let jid = JobID::from_int(1);
        let parent = TaskID::from_random();
        let mut builder = TaskSpecBuilder::new();
        builder
            .set_common_task_spec(
                &tid,
                "my_func".into(),
                0, // PYTHON
                FunctionDescriptor::default(),
                &jid,
                &parent,
                0,
                vec![],
                Address::default(),
                1,
            )
            .set_normal_task_spec();

        let spec = builder.build();
        assert_eq!(spec.task_id, tid.binary());
        assert_eq!(spec.name, "my_func");
        assert_eq!(spec.r#type, rpc::TaskType::NormalTask as i32);
        assert_eq!(spec.num_returns, 1);
    }

    #[test]
    fn test_build_actor_creation_task() {
        let tid = TaskID::from_random();
        let jid = JobID::from_int(2);
        let aid = ActorID::from_random();
        let mut builder = TaskSpecBuilder::new();
        builder
            .set_common_task_spec(
                &tid,
                "MyActor.__init__".into(),
                0,
                FunctionDescriptor::default(),
                &jid,
                &TaskID::nil(),
                0,
                vec![],
                Address::default(),
                1,
            )
            .set_actor_creation_task_spec(&aid, 3, 0, 1, false, "MyActor".into(), "default".into());

        let spec = builder.build();
        assert_eq!(spec.r#type, rpc::TaskType::ActorCreationTask as i32);
        let creation = spec.actor_creation_task_spec.unwrap();
        assert_eq!(creation.actor_id, aid.binary());
        assert_eq!(creation.max_actor_restarts, 3);
        assert!(!creation.is_detached);
    }
}
