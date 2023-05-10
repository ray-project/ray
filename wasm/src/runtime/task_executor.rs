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
use crate::util::RayLog;
use libc::memcpy;
use rmp::decode::{read_i32, read_i64, read_marker, read_u32, read_u64};
use rmp::Marker;

use crate::engine::{WasmValue, TASK_RESULT_RECEIVER, TASK_SENDER};

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
#[derive(Clone, Debug)]
pub struct TaskExecutionInfo {
    // module name
    pub mod_fullname: *const u8,
    pub mod_fullname_len: libc::size_t,

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
    pub module_name: String,
    pub func_name: String,
    pub args: Vec<WasmValue>,
}

pub struct TaskExecutor {}

extern "C" {
    static mut exec_task_func: fn(&mut TaskExecutionInfo) -> i32;
} // extern "C"

impl TaskExecutor {
    pub fn new() -> Self {
        let res = TaskExecutor {};
        unsafe { exec_task_func = TaskExecutor::exec_task };
        res
    }

    pub fn exec_task(task_execution_info: &mut TaskExecutionInfo) -> i32 {
        // get module name & function name
        let module_name: String = unsafe {
            std::slice::from_raw_parts(
                task_execution_info.mod_fullname,
                task_execution_info.mod_fullname_len,
            )
            .iter()
            .map(|&c| c as char)
            .collect()
        };
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
        RayLog::info(&format!("module_name: {}", module_name));
        RayLog::info(&format!("func_name: {}", func_name));
        RayLog::info(&format!("args_buf_list: {:x?}", args_buf_list));

        // convert args_buf_list to WasmValue
        let mut args: Vec<WasmValue> = vec![];
        for mut arg_buf in args_buf_list {
            // read message pack data from buffer
            let marker_data = Vec::from(&arg_buf[0..1]);
            match read_marker(&mut marker_data.as_slice()) {
                Ok(m) => match m {
                    Marker::U32 => {
                        args.push(WasmValue::F32(read_u32(&mut arg_buf).unwrap()));
                        continue;
                    }
                    Marker::U64 => {
                        args.push(WasmValue::F64(read_u64(&mut arg_buf).unwrap()));
                        continue;
                    }
                    Marker::I32 => {
                        args.push(WasmValue::I32(read_i32(&mut arg_buf).unwrap()));
                        continue;
                    }
                    Marker::I64 => {
                        args.push(WasmValue::I64(read_i64(&mut arg_buf).unwrap()));
                        continue;
                    }
                    _ => {
                        RayLog::error(&format!("unsupported marker: {:?}", m));
                    }
                },
                Err(e) => {
                    RayLog::error(&format!("read marker error: {:?}", e));
                }
            }
            return -1;
        }

        let wasm_task_info = WasmTaskExecutionInfo {
            module_name: module_name.to_string(),
            func_name,
            args,
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
                        let res = r.recv().unwrap();
                        match res {
                            Ok(mut data) => {
                                buf.append(&mut data);
                            }
                            Err(e) => {
                                RayLog::error(&format!("get task result error: {:?}", e));
                                task_execution_info.return_obj_len = 0;
                                return -1;
                            }
                        }
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
            task_execution_info.return_obj_len = 0;
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
