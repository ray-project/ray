// Copyright 2020-2023 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::{
    engine::{WasmFunc, WasmValue},
    runtime::{
        common_proto::{Language, TaskArg as TaskArgProto, TaskType},
        core::core_worker::{
            CoreWorker_SubmitActorTask, CoreWorker_SubmitTask, RayFunction_BuildWasm,
            RayFunction_Create, RayFunction_Destroy, TaskArg_Vec_Create, TaskArg_Vec_Destroy,
            TaskArg_Vec_PushByValue, TaskOptions_AddResource, TaskOptions_Create,
            TaskOptions_Destroy, TaskOptions_SetName, TaskOptions_SetSerializedRuntimeEnvInfo,
        },
        id::{ActorID, ObjectID},
    },
};
use core::panic;
use std::any::Any;
use std::collections::HashMap;
use tracing::{debug, error, info};

use rmp::encode::{write_i32, write_i64, write_u32, write_u64};

use super::core::core_worker::{CoreWorker_GetActor, RayFunction};

#[derive(Debug, PartialEq, Clone)]
pub enum PlacementStrategy {
    Pack = 0,
    Spread = 1,
    StrictPack = 2,
    StrictSpread = 3,
    Unrecognized = -1,
}

#[derive(Debug, PartialEq, Clone)]
pub enum PlacementGroupState {
    Pending = 0,
    Created = 1,
    Removed = 2,
    Rescheduled = 3,
    Unrecognized = -1,
}

#[derive(Debug, Clone)]
pub struct PlacementGroupCreationOptions {
    pub name: String,
    pub bundles: Vec<HashMap<String, f64>>,
    pub strategy: PlacementStrategy,
}

#[derive(Debug, Clone)]
pub struct PlacementGroup {
    pub id: String,
    pub options: PlacementGroupCreationOptions,
    pub state: PlacementGroupState,
    pub callback: fn(String, i32) -> bool,
}

impl PlacementGroup {
    pub fn new() -> Self {
        // TODO: implement this
        Self {
            id: String::new(),
            options: PlacementGroupCreationOptions {
                name: String::new(),
                bundles: Vec::new(),
                strategy: PlacementStrategy::Unrecognized,
            },
            state: PlacementGroupState::Unrecognized,
            callback: |_, _| false,
        }
    }

    pub fn get_id(&self) -> String {
        self.id.clone()
    }

    pub fn get_name(&self) -> String {
        self.options.name.clone()
    }

    pub fn get_bundles(&self) -> Vec<HashMap<String, f64>> {
        self.options.bundles.clone()
    }

    pub fn get_state(&self) -> PlacementGroupState {
        self.state.clone()
    }

    pub fn get_strategy(&self) -> PlacementStrategy {
        self.options.strategy.clone()
    }

    pub fn wait(&self, timeout_sec: i32) -> bool {
        (self.callback)(self.id.clone(), timeout_sec)
    }

    pub fn set_callback(&mut self, callback: fn(String, i32) -> bool) {
        self.callback = callback;
    }

    pub fn empty(&self) -> bool {
        self.id.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct CallOptions {
    pub name: String,
    pub resources: HashMap<String, f64>,
    pub group: PlacementGroup,
    pub bundle_index: i32,
    pub serialized_runtime_env_info: String,
}

impl CallOptions {
    pub fn new() -> Self {
        Self {
            name: String::new(),
            resources: HashMap::new(),
            group: PlacementGroup::new(),
            bundle_index: 0,
            serialized_runtime_env_info: String::new(),
        }
    }
}

pub enum TaskSubmitterType {
    Native,
    Local,
}

#[derive(Debug, Clone)]
pub struct RemoteFunctionHolder {
    pub module_name: String,
    pub function_name: String,
    pub class_name: String,
    pub lang_type: Language,
}

impl RemoteFunctionHolder {
    pub fn new() -> Self {
        Self {
            module_name: String::new(),
            function_name: String::new(),
            class_name: String::new(),
            lang_type: Language::Wasm,
        }
    }

    pub fn new_from_func(func: WasmFunc) -> Self {
        Self {
            module_name: func.module,
            function_name: func.name,
            class_name: "".to_string(),
            lang_type: Language::Wasm,
        }
    }
}

pub trait TaskArg {
    fn to_proto(&self, arg_proto: TaskArgProto);
}

pub struct InvocationSpec {
    pub task_type: TaskType,
    pub name: String,
    pub actor_id: ActorID,
    pub actor_counter: i32,
    pub remote_func_holder: RemoteFunctionHolder,
    pub args: Vec<WasmValue>,
}

impl InvocationSpec {
    pub fn new(
        task_type: TaskType,
        remote_func_holder: RemoteFunctionHolder,
        args: &[WasmValue],
        actor_id: Option<ActorID>,
    ) -> Self {
        let actor_id = match actor_id {
            Some(actor_id) => actor_id,
            None => ActorID::nil(),
        };
        InvocationSpec {
            task_type,
            name: "".to_string(),
            actor_id,
            actor_counter: 0,
            remote_func_holder,
            args: args.to_vec(),
        }
    }
}

pub trait TaskSubmitter {
    fn submit_task(&self, invocation: &InvocationSpec, call_options: &CallOptions) -> ObjectID;
    fn create_actor(&mut self, invocation: &InvocationSpec) -> ActorID;
    fn submit_actor_task(
        &mut self,
        invocation: &InvocationSpec,
        call_options: &CallOptions,
    ) -> ObjectID;
    fn get_actor(&mut self, actor_name: &String, ray_namespace: &String) -> ActorID;
    fn create_placement_group(&mut self) -> PlacementGroup;
    fn remove_placement_group(&mut self, group_id: &String);
    fn wait_placement_group_ready(&mut self, group_id: &String, timeout_sec: i64) -> bool;
}

pub struct TaskSubmitterFactory {}

impl TaskSubmitterFactory {
    pub fn create_task_submitter(
        submitter_type: TaskSubmitterType,
    ) -> Box<dyn TaskSubmitter + Send + Sync> {
        match submitter_type {
            TaskSubmitterType::Native => Box::new(NativeTaskSubmitter::new()),
            TaskSubmitterType::Local => Box::new(LocalTaskSubmitter::new()),
        }
    }
}

pub struct NativeTaskSubmitter {}

impl TaskSubmitter for NativeTaskSubmitter {
    fn submit_task(&self, invocation: &InvocationSpec, call_options: &CallOptions) -> ObjectID {
        unsafe {
            let mut obj_id: ObjectID = ObjectID::new();
            let mut obj_id_ptr = obj_id.id.as_mut_ptr();
            let mut obj_id_len: usize;
            obj_id_len = obj_id.id.len();

            let mut task_options = TaskOptions_Create();
            if task_options.is_null() {
                panic!("Failed to create task options");
            }
            TaskOptions_SetName(
                task_options,
                call_options.name.as_ptr(),
                call_options.name.len(),
            );
            for (key, value) in call_options.resources.iter() {
                let key_ptr = key.as_ptr();
                let key_len = key.len();
                let value = *value;
                TaskOptions_AddResource(task_options, key_ptr, key_len, value);
            }
            TaskOptions_SetSerializedRuntimeEnvInfo(
                task_options,
                call_options.serialized_runtime_env_info.as_ptr(),
                call_options.serialized_runtime_env_info.len(),
            );

            let mut ray_function = RayFunction::new();
            ray_function
                .build_wasm(
                    invocation.remote_func_holder.function_name.as_str(),
                    invocation.remote_func_holder.module_name.as_str(),
                )
                .unwrap();

            let mut task_args = TaskArg_Vec_Create();
            let arg_len = invocation.args.len();
            let mut bufs: Vec<Vec<_>> = Vec::with_capacity(arg_len);
            for arg in invocation.args.iter() {
                let mut tmpbuf = vec![];
                match arg {
                    crate::engine::WasmValue::I32(v) => {
                        write_i32(&mut tmpbuf, *v).unwrap();
                    }
                    crate::engine::WasmValue::I64(v) => {
                        write_i64(&mut tmpbuf, *v).unwrap();
                    }
                    crate::engine::WasmValue::F32(v) => {
                        write_u32(&mut tmpbuf, *v).unwrap();
                    }
                    crate::engine::WasmValue::F64(v) => {
                        write_u64(&mut tmpbuf, *v).unwrap();
                    }
                    crate::engine::WasmValue::V128(_) => {
                        unimplemented!("V128 is not supported yet");
                    }
                    crate::engine::WasmValue::FuncRef(_) => {
                        unimplemented!("FuncRef is not supported yet");
                    }
                    crate::engine::WasmValue::ExternRef(_) => {
                        unimplemented!("ExternRef is not supported yet");
                    }
                }
                bufs.push(tmpbuf);
            }
            // print the buffer
            info!("bufs: {:x?}", bufs);
            for buf in bufs.iter() {
                let rtn = TaskArg_Vec_PushByValue(
                    task_args,
                    buf.as_ptr() as *const u8,
                    buf.len(),
                    0 as *mut u8,
                    0,
                );
                if rtn != 0 {
                    panic!("Failed to push task arg");
                }
            }

            if invocation.task_type == TaskType::ActorTask {
                // submit an actor task
                let actor_id = invocation.actor_id.id.as_ptr();
                let actor_id_len = invocation.actor_id.id.len();

                if CoreWorker_SubmitActorTask(
                    actor_id,
                    actor_id_len,
                    ray_function.raw,
                    task_args,
                    task_options,
                    obj_id_ptr,
                    &mut obj_id_len,
                ) != 0
                {
                    panic!("Failed to submit actor task");
                }
            } else {
                // submit a task
                if CoreWorker_SubmitTask(
                    ray_function.raw,
                    task_args,
                    task_options,
                    obj_id_ptr,
                    &mut obj_id_len,
                ) != 0
                {
                    panic!("Failed to submit task");
                }
                // TODO: we do not support bundle yet.
            }
            if obj_id_len != obj_id.id.len() {
                panic!("invalid object id length");
            }

            TaskArg_Vec_Destroy(task_args);
            TaskOptions_Destroy(task_options);

            return obj_id;
        }
    }

    fn create_actor(&mut self, invocation: &InvocationSpec) -> ActorID {
        unimplemented!()
    }

    fn submit_actor_task(
        &mut self,
        invocation: &InvocationSpec,
        call_options: &CallOptions,
    ) -> ObjectID {
        self.submit_task(invocation, call_options)
    }

    fn get_actor(&mut self, actor_name: &String, ray_namespace: &String) -> ActorID {
        unsafe {
            let mut actor_id = ActorID::new();
            let mut actor_id_ptr = actor_id.id.as_mut_ptr();
            let mut actor_id_len = actor_id.id.len();

            let actor_name_ptr = actor_name.as_ptr();
            let actor_name_len = actor_name.len();
            let ray_namespace_ptr = ray_namespace.as_ptr();
            let ray_namespace_len = ray_namespace.len();
            if CoreWorker_GetActor(
                actor_name_ptr,
                actor_name_len,
                ray_namespace_ptr,
                ray_namespace_len,
                actor_id_ptr,
                &mut actor_id_len,
            ) != 0
            {
                panic!("Failed to get actor");
            }
            if actor_id_len != actor_id.id.len() {
                panic!("invalid actor id length");
            }
            return actor_id;
        }
    }

    fn create_placement_group(&mut self) -> PlacementGroup {
        unimplemented!()
    }

    fn remove_placement_group(&mut self, group_id: &String) {
        unimplemented!()
    }

    fn wait_placement_group_ready(&mut self, group_id: &String, timeout_sec: i64) -> bool {
        unimplemented!()
    }
}

impl NativeTaskSubmitter {
    pub fn new() -> Self {
        Self {}
    }
}

pub struct LocalTaskSubmitter {}

impl TaskSubmitter for LocalTaskSubmitter {
    fn submit_task(&self, invocation: &InvocationSpec, call_options: &CallOptions) -> ObjectID {
        unimplemented!()
    }

    fn create_actor(&mut self, invocation: &InvocationSpec) -> ActorID {
        unimplemented!()
    }

    fn submit_actor_task(
        &mut self,
        invocation: &InvocationSpec,
        call_options: &CallOptions,
    ) -> ObjectID {
        self.submit_task(invocation, call_options)
    }

    fn get_actor(&mut self, actor_name: &String, ray_namespace: &String) -> ActorID {
        todo!()
    }

    fn create_placement_group(&mut self) -> PlacementGroup {
        todo!()
    }

    fn remove_placement_group(&mut self, group_id: &String) {
        todo!()
    }

    fn wait_placement_group_ready(&mut self, group_id: &String, timeout_sec: i64) -> bool {
        todo!()
    }
}

impl LocalTaskSubmitter {
    pub fn new() -> Self {
        Self {}
    }
}
