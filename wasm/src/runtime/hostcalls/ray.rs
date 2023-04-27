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

use std::sync::Arc;
use std::thread::sleep;
use std::{sync::RwLock, vec};

use crate::runtime::common_proto::TaskType;

use crate::engine::WasmEngine;
use crate::runtime::{Base, ObjectID, RemoteFunctionHolder};
use crate::{
    engine::{Hostcalls, WasmContext, WasmType, WasmValue},
    runtime::RayRuntime,
};
use anyhow::{anyhow, Result};
use core::result::Result::Ok;
use tracing::{debug, error, info};

#[derive(Debug, Clone, Copy)]
struct RayBufferHolder {
    pub magic: u32,
    pub data_type: u32,
    pub ptr: u32,
    pub len: u32,
    pub cap: u32,
}

impl RayBufferHolder {
    pub fn new() -> Self {
        Self {
            magic: RAY_BUF_MAGIC,
            data_type: RayBufferDataType::Invalid as u32,
            ptr: 0,
            len: 0,
            cap: 0,
        }
    }
}

// define constants
const RAY_BUF_MAGIC: u32 = 0xc0de_550a;
const RAY_BUF_SIZE: usize = 32 * 5 / 8;

enum RayBufferDataType {
    Invalid = 0x0,
    ObjectID = 0x1,
    Data = 0x2,
}

fn ray_buffer_write_data(
    ctx: &mut dyn WasmContext,
    ray_buf_ptr: u32,
    data: &[u8],
    data_type: RayBufferDataType,
) -> Result<()> {
    let mut ray_buf = RayBufferHolder::new();
    match ctx.get_memory_region_mut(ray_buf_ptr as usize, RAY_BUF_SIZE) {
        Ok(v) => {
            ray_buf.data_type = data_type as u32;
            ray_buf.magic = u32::from_le_bytes([v[0], v[1], v[2], v[3]]);
            if ray_buf.magic != RAY_BUF_MAGIC {
                return Err(anyhow!("invalid magic code"));
            }
            ray_buf.ptr = u32::from_le_bytes([v[8], v[9], v[10], v[11]]);
            ray_buf.len = data.len() as u32;
            ray_buf.cap = u32::from_le_bytes([v[16], v[17], v[18], v[19]]);
            if ray_buf.cap < data.len() as u32 {
                return Err(anyhow!(format!(
                    "ray buffer is not big enough cap: {}, data len: {}",
                    ray_buf.cap,
                    data.len()
                )));
            }

            // write back len and data_type
            v[4..8].copy_from_slice(&ray_buf.data_type.to_le_bytes());
            v[12..16].copy_from_slice(&ray_buf.len.to_le_bytes());
        }
        Err(e) => {
            return Err(anyhow!(
                "invalid mutable access to ray buffer data structure"
            ));
        }
    }
    write_buffer(ctx, ray_buf.ptr, ray_buf.cap, data)
}

/// read data from wasm memory
fn load_buffer(ctx: &mut dyn WasmContext, buf_ptr: u32, buf_len: u32) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; buf_len as usize];
    match ctx.get_memory_region(buf_ptr as usize, buf_len as usize) {
        Ok(v) => {
            buf.copy_from_slice(&v);
        }
        Err(e) => {
            return Err(anyhow!("invalid access to ray buffer address"));
        }
    }
    Ok(buf)
}

/// write data to wasm memory
fn write_buffer(ctx: &mut dyn WasmContext, buf_ptr: u32, buf_cap: u32, data: &[u8]) -> Result<()> {
    if data.len() > buf_cap as usize {
        return Err(anyhow!(format!(
            "ray buffer length is not big enough, buf_cap: {}, data len: {}",
            buf_cap,
            data.len()
        )));
    }
    match ctx.get_memory_region_mut(buf_ptr as usize, data.len() as usize) {
        Ok(v) => {
            debug!(
                "write buffer, buf_ptr: {}, buf_cap: {}, data len: {}, data: {:x?}",
                buf_ptr,
                buf_cap,
                data.len(),
                data
            );
            v[0..data.len()].copy_from_slice(data);
        }
        Err(e) => {
            return Err(anyhow!("invalid mutable access to ray buffer address"));
        }
    }
    Ok(())
}

/// read ray buffer structure from the specified offset
fn read_ray_buffer(ctx: &mut dyn WasmContext, ray_buf_ptr: u32) -> Result<RayBufferHolder> {
    let mut ray_buf = RayBufferHolder::new();

    // make sure the magic code is correct
    match ctx.get_memory_region(ray_buf_ptr as usize, RAY_BUF_SIZE) {
        Ok(v) => {
            // convert u8 array to u32 array
            ray_buf.magic = u32::from_le_bytes([v[0], v[1], v[2], v[3]]);
            if ray_buf.magic != RAY_BUF_MAGIC {
                return Err(anyhow!("invalid magic code"));
            }
            ray_buf.data_type = u32::from_le_bytes([v[4], v[5], v[6], v[7]]);
            ray_buf.ptr = u32::from_le_bytes([v[8], v[9], v[10], v[11]]);
            ray_buf.len = u32::from_le_bytes([v[12], v[13], v[14], v[15]]);
            ray_buf.cap = u32::from_le_bytes([v[16], v[17], v[18], v[19]]);
        }
        Err(e) => {
            let msg = format!(
                "invalid ray buffer data structure region, ptr: {:x}",
                ray_buf_ptr
            );
            error!("{}", msg);
            return Err(anyhow!(msg));
        }
    }

    if ray_buf.len > ray_buf.cap {
        let msg = format!(
            "invalid ray buffer length, len: {}, cap: {}",
            ray_buf.len, ray_buf.cap
        );
        error!("{} ptr:{:x} val:{:x?}", msg, ray_buf_ptr, ray_buf);
        return Err(anyhow!(msg));
    }

    match ctx.get_memory_region(ray_buf.ptr as usize, ray_buf.cap as usize) {
        Ok(_) => {}
        Err(e) => {
            error!("invalid ray buffer region");
            return Err(anyhow!("invalid object id"));
        }
    }
    Ok(ray_buf)
}

pub fn register_ray_hostcalls(
    runtime: &Arc<RwLock<Box<dyn RayRuntime + Send + Sync>>>,
    engine: &Arc<RwLock<Box<dyn WasmEngine + Send + Sync>>>,
) -> Result<()> {
    let mut hostcalls = Hostcalls::new("ray", runtime.clone());
    hostcalls
        .add_hostcall("test", vec![], vec![], hc_test)
        .unwrap();
    hostcalls
        .add_hostcall("sleep", vec![WasmType::I32], vec![], hc_sleep)
        .unwrap();
    hostcalls
        .add_hostcall("init", vec![], vec![], hc_init)
        .unwrap();
    hostcalls
        .add_hostcall("shutdown", vec![], vec![], hc_shutdown)
        .unwrap();
    hostcalls
        .add_hostcall(
            "get",
            vec![WasmType::I32, WasmType::I32],
            vec![WasmType::I32],
            hc_get,
        )
        .unwrap();
    hostcalls
        .add_hostcall("put", vec![], vec![], hc_put)
        .unwrap();
    hostcalls
        .add_hostcall(
            "call",
            vec![WasmType::I32, WasmType::I32, WasmType::I32],
            vec![WasmType::I32],
            hc_call,
        )
        .unwrap();
    {
        let mut engine = engine.write().unwrap();
        engine.register_hostcalls(&hostcalls)?;
    }
    Ok(())
}

fn hc_test(ctx: &mut dyn WasmContext, params: &[WasmValue]) -> Result<Vec<WasmValue>> {
    info!("test function called");
    Ok(vec![])
}

fn hc_sleep(ctx: &mut dyn WasmContext, params: &[WasmValue]) -> Result<Vec<WasmValue>> {
    let sleep_time = match &params[0] {
        WasmValue::I32(v) => {
            sleep(std::time::Duration::from_secs(*v as u64));
        }
        _ => return Err(anyhow!("invalid param")),
    };
    Ok(vec![])
}

fn hc_init(ctx: &mut dyn WasmContext, params: &[WasmValue]) -> Result<Vec<WasmValue>> {
    Err(anyhow!("not implemented"))
}

fn hc_shutdown(ctx: &mut dyn WasmContext, params: &[WasmValue]) -> Result<Vec<WasmValue>> {
    Err(anyhow!("not implemented"))
}

fn hc_get(ctx: &mut dyn WasmContext, params: &[WasmValue]) -> Result<Vec<WasmValue>> {
    info!("ray_get: {:x?}", params);
    let obj_id_ptr = match &params[0] {
        WasmValue::I32(v) => v.clone(),
        _ => {
            error!("invalid param");
            return Ok(vec![WasmValue::I32(-1)]);
        }
    };
    let result_ptr = match &params[1] {
        WasmValue::I32(v) => v.clone(),
        _ => {
            error!("invalid param");
            return Ok(vec![WasmValue::I32(-1)]);
        }
    };

    let mut obj_buf = match read_ray_buffer(ctx, obj_id_ptr as u32) {
        Ok(v) => {
            debug!(
                "ray_get: obj_id_ptr: {:#08x} content: {:x?}, buffer {:x?}",
                obj_id_ptr,
                v,
                load_buffer(ctx, v.ptr, v.len)
            );
            v
        }
        Err(e) => {
            error!("{}", e.to_string());
            return Ok(vec![WasmValue::I32(-1)]);
        }
    };

    let mut res_buf = match read_ray_buffer(ctx, result_ptr as u32) {
        Ok(v) => {
            debug!(
                "ray_get: result_ptr: {:#08x} content: {:x?}, buffer {:x?}",
                result_ptr,
                v,
                load_buffer(ctx, v.ptr, v.len)
            );
            v
        }
        Err(e) => {
            error!("{}", e.to_string());
            return Ok(vec![WasmValue::I32(-1)]);
        }
    };

    match load_buffer(ctx, obj_buf.ptr, obj_buf.len) {
        Ok(v) => {
            let obj_id = ObjectID::from_binary(&v.as_slice());
            match ctx.get_object(&obj_id) {
                Ok(obj) => {
                    res_buf.len = obj.len() as u32;
                    let result = ray_buffer_write_data(
                        ctx,
                        result_ptr as u32,
                        obj.as_slice(),
                        RayBufferDataType::Data,
                    );
                    if result.is_err() {
                        error!("write result buffer failed: {}", result.err().unwrap());
                        return Ok(vec![WasmValue::I32(-1)]);
                    }
                }
                Err(e) => {
                    error!("get object failed: {}", e.to_string());
                    return Ok(vec![WasmValue::I32(-1)]);
                }
            }
        }
        Err(e) => {
            error!("invalid object buffer region");
            return Ok(vec![WasmValue::I32(-1)]);
        }
    }

    info!(
        "ray_get: result: {:x?}, buffer: {:x?}",
        res_buf,
        load_buffer(ctx, res_buf.ptr, res_buf.len)
    );
    return Ok(vec![WasmValue::I32(0)]);
}

fn hc_put(ctx: &mut dyn WasmContext, params: &[WasmValue]) -> Result<Vec<WasmValue>> {
    Err(anyhow!("not implemented"))
}

fn hc_call(ctx: &mut dyn WasmContext, params: &[WasmValue]) -> Result<Vec<WasmValue>> {
    info!("ray_call: {:x?}", params);
    let ray_buf_ptr = match &params[0] {
        WasmValue::I32(v) => v.clone(),
        _ => return Err(anyhow!("invalid param")),
    };

    let mut ray_buf = match read_ray_buffer(ctx, ray_buf_ptr as u32) {
        Ok(v) => {
            debug!("call: ray_buf_ptr: {:#08x} content: {:x?}", ray_buf_ptr, v);
            v
        }
        Err(e) => {
            return Err(anyhow!("invalid object id"));
        }
    };

    let func_ref_val = match &params[1] {
        WasmValue::I32(v) => v.clone(),
        _ => return Err(anyhow!("invalid param")),
    };
    let func = ctx.get_func_ref(func_ref_val as u32).unwrap();

    let args_ptr = match &params[2] {
        WasmValue::I32(v) => v.clone(),
        _ => return Err(anyhow!("invalid param")),
    };

    let mut updated_obj_id: Vec<u8> = Vec::new();
    match ctx.get_memory_region(args_ptr as usize, func.params_data_size().unwrap()) {
        Ok(v) => {
            debug!(
                "call: func_ref_val: {}, args_ptr: {:#08x} content: {:x?}",
                func_ref_val, args_ptr, v
            );
            let args = func.params_convert(v).unwrap();
            debug!("call: args: {:x?}", args);

            let remote_func = RemoteFunctionHolder::new_from_func(func);
            let result = ctx.invoke(&remote_func, args.as_slice());
            match result {
                Ok(v) => {
                    if v.len() != 1 {
                        error!("call: invalid return value");
                        return Ok(vec![WasmValue::I32(-1)]);
                    }
                    info!("call: return value: {:x?}", v);
                    // make sure object id data length can fit into the
                    // memory region
                    if ray_buf.cap < v[0].id.len() as u32 {
                        error!("call: invalid return value");
                        return Ok(vec![WasmValue::I32(-1)]);
                    }
                    updated_obj_id.resize(v[0].id.len() as usize, 0);
                    updated_obj_id[..v[0].id.len() as usize].copy_from_slice(&v[0].id);
                }
                Err(e) => {
                    error!("call: error: {:?}", e);
                    return Ok(vec![WasmValue::I32(-1)]);
                }
            }
        }
        Err(e) => {
            error!("cannot access memory region: {}", e);
            return Ok(vec![WasmValue::I32(-1)]);
        }
    }

    match ray_buffer_write_data(
        ctx,
        ray_buf_ptr as u32,
        updated_obj_id.as_slice(),
        RayBufferDataType::ObjectID,
    ) {
        Ok(v) => Ok(vec![WasmValue::I32(0)]),
        Err(e) => {
            error!("put: write data failed: {}", e);
            Ok(vec![WasmValue::I32(-1)])
        }
    }
}
