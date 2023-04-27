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
use crate::util::RayLog;
use anyhow::Result;
use libc::memcpy;
use rmp::encode::write_i32;
use std::collections::HashMap;
use tracing::info;

use super::{TASK_RESULT_RECEIVER, TASK_SENDER};

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
    // function name
    pub func_name: *const u8,
    pub func_name_len: libc::size_t,

    // args buffer
    pub args_buf_list: *mut *mut u8,
    pub args_buf_list_len: libc::size_t,
    pub args_buf_list_item_len: *mut libc::size_t,

    // return data
    pub return_obj: *const u8,
    pub return_obj_len: libc::size_t,
}

#[derive(Clone, Debug)]
pub struct WasmTaskExecutionInfo {
    pub func_name: String,
    pub args_buf_list: Vec<Vec<u8>>,
}

pub struct TaskExecutor {}

extern "C" {
    #[no_mangle]
    static mut exec_task_func: fn(&mut TaskExecutionInfo) -> i32;
} // extern "C"

impl TaskExecutor {
    pub fn new() -> Self {
        let res = TaskExecutor {};
        unsafe { exec_task_func = TaskExecutor::exec_task };
        res
    }

    pub fn exec_task(task_execution_info: &mut TaskExecutionInfo) -> i32 {
        let func_name: String = unsafe {
            std::slice::from_raw_parts(
                task_execution_info.func_name,
                task_execution_info.func_name_len,
            )
            .iter()
            .map(|&c| c as char)
            .collect()
        };
        let args_buf_list: Vec<&[u8]> = unsafe {
            std::slice::from_raw_parts(
                task_execution_info.args_buf_list,
                task_execution_info.args_buf_list_len,
            )
            .iter()
            .map(|&ptr| {
                std::slice::from_raw_parts(ptr, *task_execution_info.args_buf_list_item_len)
            })
            .collect()
        };

        // print all info
        RayLog::info(&format!("execute task: {}", func_name));
        RayLog::info(&format!("args_buf_list: {:x?}", args_buf_list));

        let wasm_task_info = WasmTaskExecutionInfo {
            func_name: func_name.clone(),
            args_buf_list: args_buf_list.iter().map(|&x| x.to_vec()).collect(),
        };
        match TASK_SENDER.lock() {
            Ok(sender) => {
                match sender.as_ref() {
                    Some(sender) => {
                        // RayLog::info("Notify new task execution info");
                        sender.send(wasm_task_info).unwrap();
                    }
                    None => {
                        RayLog::error("task sender is none");
                    }
                }
            }
            Err(e) => {
                RayLog::error(&format!("get task sender error: {}", e));
            }
        }

        let mut buf: Vec<u8> = vec![];
        // get data from result receiver
        match TASK_RESULT_RECEIVER.lock() {
            Ok(receiver) => {
                match receiver.as_ref() {
                    Some(r) => {
                        // copy data to buffer
                        let mut data = r.recv().unwrap();
                        buf.append(&mut data);
                    }
                    None => {
                        RayLog::error("task result receiver is none");
                    }
                }
            }
            Err(e) => {
                RayLog::error(&format!("get task result receiver error: {}", e));
            }
        }

        if buf.len() > task_execution_info.return_obj_len {
            RayLog::error(&format!(
                "return buffer is too small, required: {}, actual: {}",
                buf.len(),
                task_execution_info.return_obj_len
            ));
            return -1;
        }
        unsafe {
            // copy data to buffer
            memcpy(
                task_execution_info.return_obj as *mut libc::c_void,
                buf.as_ptr() as *const libc::c_void,
                buf.len(),
            );
            task_execution_info.return_obj_len = buf.len();
        }

        0
    }
}
