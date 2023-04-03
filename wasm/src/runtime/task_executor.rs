// Copyright 2020-2021 The Ray Authors.
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
use crate::runtime::common_proto::{Address, TaskType};
use crate::runtime::core_worker::RayObject;
use anyhow::Result;
use std::collections::HashMap;
use tracing::info;

#[repr(C)]
pub struct Resource {
    pub name: *const u8,
    pub name_len: libc::size_t,
    pub value: f64,
}

#[repr(C)]
pub struct NamedRayObject {
    pub obj_id: *const u8,
    pub obj_id_len: libc::size_t,
    pub ray_object: *const libc::c_void,
    pub ray_object_len: libc::size_t,
}

#[repr(C)]
pub struct TaskExecutionInfo {
    pub caller_address: *const libc::c_void,
    pub task_type: TaskType,
    pub task_name: *const u8,
    pub task_name_len: libc::size_t,
    pub ray_function: *const libc::c_void,
    pub required_resources: *const Resource,
    pub required_resources_len: libc::size_t,
    pub args: *const libc::c_void,
    pub args_len: libc::size_t,
    pub debugger_breakpoint: *const u8,
    pub debugger_breakpoint_len: libc::size_t,
    pub serialized_retry_exception_allowlist: *const u8,
    pub serialized_retry_exception_allowlist_len: libc::size_t,
    pub returns: *const NamedRayObject,
    pub returns_len: libc::size_t,
    pub dynamic_returns: *const NamedRayObject,
    pub dynamic_returns_len: libc::size_t,
    pub creation_task_exception_pb_bytes: *const libc::c_void,
    pub is_retryable_error: *mut bool,
    pub is_application_error: *mut u8,
    pub is_application_error_len: *mut libc::size_t,
    pub defined_concurrency_groups: *const *const libc::c_void,
    pub defined_concurrency_groups_len: libc::size_t,
    pub name_of_concurrency_group_to_execute: *const u8,
    pub name_of_concurrency_group_to_execute_len: libc::size_t,
    pub is_reattempt: bool,
}

pub struct TaskExecutor {}

extern "C" {
    #[no_mangle]
    static mut execute_task_function: fn(&TaskExecutionInfo) -> i32;
} // extern "C"

impl TaskExecutor {
    pub fn new() -> Self {
        let res = TaskExecutor {};
        unsafe { execute_task_function = TaskExecutor::execute_task };
        res
    }

    pub fn execute_task(task_execution_info: &TaskExecutionInfo) -> i32 {
        info!("execute_task");
        -1
    }
}
