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

use crate::engine::WasmEngine;
use crate::runtime::{Base, ObjectID, RemoteFunctionHolder};
use crate::util::{SerDesFactory, SerDesType};
use crate::{
    engine::{Hostcalls, WasmContext, WasmType, WasmValue},
    runtime::RayRuntime,
};
use anyhow::{anyhow, Result};
use core::result::Result::Ok;
use tracing::{debug, error, info};

const RAY_BUF_MAGIC: u32 = 0xc0de_550a;
const RAY_BUF_SIZE: usize = 32 * 6 / 8;

// any modifications to the struct should update above related constants
#[derive(Debug, Clone, Copy)]
struct RayBufferHolder {
    pub magic: u32,
    pub data_type: u32,
    pub ptr: u32,
    pub len: u32,
    pub cap: u32,
    pub checksum: u32,
}

impl RayBufferHolder {
    pub fn new() -> Self {
        let mut res = Self {
            magic: RAY_BUF_MAGIC,
            data_type: RayBufferDataType::Invalid as u32,
            ptr: 0,
            len: 0,
            cap: 0,
            checksum: 0,
        };
        res.checksum = res.calc_checksum();
        res
    }

    pub fn is_valid(&self) -> bool {
        self.checksum == self.calc_checksum()
    }

    pub fn calc_checksum(&self) -> u32 {
        let mut checksum = 0u32;
        checksum ^= self.magic;
        checksum ^= self.data_type;
        checksum ^= self.ptr;
        checksum ^= self.len;
        checksum ^= self.cap;
        checksum
    }
}

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
            ray_buf.checksum = ray_buf.calc_checksum();

            // write back len and data_type
            v[4..8].copy_from_slice(&ray_buf.data_type.to_le_bytes());
            v[12..16].copy_from_slice(&ray_buf.len.to_le_bytes());
            v[20..24].copy_from_slice(&ray_buf.checksum.to_le_bytes());
        }
        Err(_) => {
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
        Err(_) => {
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
        Err(_) => {
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
            ray_buf.checksum = u32::from_le_bytes([v[20], v[21], v[22], v[23]]);
        }
        Err(_) => {
            let msg = format!(
                "invalid ray buffer data structure region, ptr: {:x}",
                ray_buf_ptr
            );
            error!("{}", msg);
            return Err(anyhow!(msg));
        }
    }

    // verify checksum
    if !ray_buf.is_valid() {
        let msg = format!("invalid ray buffer checksum, ptr: {:x}", ray_buf_ptr);
        error!("{}", msg);
        return Err(anyhow!(msg));
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
        Err(_) => {
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
        .add_hostcall("test", vec![], vec![], hc_ray_test)
        .unwrap();
    hostcalls
        .add_hostcall("sleep", vec![WasmType::I32], vec![], hc_ray_sleep)
        .unwrap();
    hostcalls
        .add_hostcall("init", vec![], vec![WasmType::I32], hc_ray_init)
        .unwrap();
    hostcalls
        .add_hostcall("shutdown", vec![], vec![], hc_ray_shutdown)
        .unwrap();
    hostcalls
        .add_hostcall(
            "get",
            vec![WasmType::I32, WasmType::I32, WasmType::I32],
            vec![WasmType::I32],
            hc_ray_get,
        )
        .unwrap();
    hostcalls
        .add_hostcall(
            "put",
            vec![WasmType::I32, WasmType::I32, WasmType::I32],
            vec![WasmType::I32],
            hc_ray_put,
        )
        .unwrap();
    hostcalls
        .add_hostcall(
            "call",
            vec![WasmType::I32, WasmType::I32, WasmType::I32],
            vec![WasmType::I32],
            hc_ray_call,
        )
        .unwrap();
    {
        let mut engine = engine.write().unwrap();
        engine.register_hostcalls(&hostcalls)?;
    }
    Ok(())
}

pub fn hc_ray_test(_ctx: &mut dyn WasmContext, _params: &[WasmValue]) -> Result<Vec<WasmValue>> {
    info!("test function called");
    Ok(vec![])
}

pub fn hc_ray_sleep(_ctx: &mut dyn WasmContext, params: &[WasmValue]) -> Result<Vec<WasmValue>> {
    match &params[0] {
        WasmValue::I32(v) => {
            sleep(std::time::Duration::from_secs(*v as u64));
        }
        _ => return Err(anyhow!("invalid param")),
    };
    Ok(vec![])
}

/// put sandbox binaries to object store
/// no parameter
/// return 0 if success, -1 if failed
pub fn hc_ray_init(ctx: &mut dyn WasmContext, params: &[WasmValue]) -> Result<Vec<WasmValue>> {
    // make sure there is not parameter
    if params.len() != 0 {
        error!("invalid parameter");
        return Ok(vec![WasmValue::I32(-1)]);
    }
    match ctx.submit_sandbox_binary() {
        Ok(_) => Ok(vec![WasmValue::I32(0)]),
        Err(e) => {
            error!("submit sandbox binary failed: {}", e);
            return Ok(vec![WasmValue::I32(-1)]);
        }
    }
}

pub fn hc_ray_shutdown(
    _ctx: &mut dyn WasmContext,
    _params: &[WasmValue],
) -> Result<Vec<WasmValue>> {
    Err(anyhow!("not implemented"))
}

/// get object from object store
/// params[0]: object id buffer pointer
/// params[1]: result buffer pointer
/// params[2]: result buffer length pointer
/// return 0 if success, -1 if failed
pub fn hc_ray_get(ctx: &mut dyn WasmContext, params: &[WasmValue]) -> Result<Vec<WasmValue>> {
    info!("ray_get: {:x?}", params);
    let obj_id_ptr = match &params[0] {
        WasmValue::I32(v) => v.clone(),
        _ => {
            error!("invalid param");
            return Ok(vec![WasmValue::I32(-1)]);
        }
    };
    let result_buf_ptr = match &params[1] {
        WasmValue::I32(v) => v.clone(),
        _ => {
            error!("invalid param");
            return Ok(vec![WasmValue::I32(-1)]);
        }
    };
    let result_len_ptr = match &params[2] {
        WasmValue::I32(v) => v.clone(),
        _ => {
            error!("invalid param");
            return Ok(vec![WasmValue::I32(-1)]);
        }
    };
    let result_len = match ctx.get_memory_region(result_len_ptr as usize, 4) {
        Ok(v) => {
            debug!(
                "ray_get: result_len_ptr: {:#08x} content: {:x?}",
                result_len_ptr, v
            );
            u32::from_le_bytes([v[0], v[1], v[2], v[3]])
        }
        Err(e) => {
            error!("cannot access memory region: {}", e);
            return Ok(vec![WasmValue::I32(-1)]);
        }
    };

    let obj_buf = match read_ray_buffer(ctx, obj_id_ptr as u32) {
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

    let data: Vec<u8>;
    match load_buffer(ctx, obj_buf.ptr, obj_buf.len) {
        Ok(v) => {
            let obj_id = ObjectID::from_binary(&v.as_slice());
            match ctx.get_object(&obj_id) {
                Ok(obj) => {
                    let serdes = SerDesFactory::create(SerDesType::MsgPack);
                    let deserialized_obj = match serdes.deserialize(obj.as_slice()) {
                        Ok(v) => {
                            debug!("ray_get: object: {:x?}", v);
                            v
                        }
                        Err(e) => {
                            error!("deserialize object failed: {}", e.to_string());
                            return Ok(vec![WasmValue::I32(-1)]);
                        }
                    };
                    if deserialized_obj.len() > result_len as usize {
                        error!(
                            "result buffer is not big enough, result_len: {}, obj_len: {}",
                            result_len,
                            deserialized_obj.len()
                        );
                        return Ok(vec![WasmValue::I32(-1)]);
                    }
                    data = Vec::from(deserialized_obj.as_slice());
                }
                Err(e) => {
                    error!("get object failed: {}", e.to_string());
                    return Ok(vec![WasmValue::I32(-1)]);
                }
            }
        }
        Err(_) => {
            error!("invalid object buffer region");
            return Ok(vec![WasmValue::I32(-1)]);
        }
    }

    match ctx.get_memory_region_mut(result_buf_ptr as usize, data.len()) {
        Ok(v) => {
            debug!(
                "ray_get: result_buf_ptr: {:#08x} content: {:x?}",
                result_buf_ptr, v
            );
            v.copy_from_slice(&data);
        }
        Err(e) => {
            error!("cannot access memory region: {}", e);
            return Ok(vec![WasmValue::I32(-1)]);
        }
    }

    match ctx.get_memory_region_mut(result_len_ptr as usize, 4) {
        Ok(v) => {
            debug!(
                "ray_get: result_len_ptr: {:#08x} content: {:x?}",
                result_len_ptr, v
            );
            v.copy_from_slice(&(data.len() as u32).to_le_bytes());
        }
        Err(e) => {
            error!("cannot access memory region: {}", e);
            return Ok(vec![WasmValue::I32(-1)]);
        }
    }

    info!(
        "ray_get: write result {:x?} to buffer",
        load_buffer(ctx, result_buf_ptr as u32, data.len() as u32)
    );
    return Ok(vec![WasmValue::I32(0)]);
}

/// call ray put to put data into object store
/// params[0]: object id buffer pointer
/// params[1]: data buffer pointer
/// params[2]: data length
/// return 0 if success, -1 if failed
pub fn hc_ray_put(ctx: &mut dyn WasmContext, params: &[WasmValue]) -> Result<Vec<WasmValue>> {
    info!("ray_put: {:x?}", params);
    let ray_buf_ptr = match &params[0] {
        WasmValue::I32(v) => v.clone(),
        _ => return Err(anyhow!("invalid param")),
    };
    let ray_buf = match read_ray_buffer(ctx, ray_buf_ptr as u32) {
        Ok(v) => {
            debug!("call: ray_buf_ptr: {:#08x} content: {:x?}", ray_buf_ptr, v);
            v
        }
        Err(_) => {
            return Err(anyhow!("invalid object id"));
        }
    };

    let data_ptr = match &params[1] {
        WasmValue::I32(v) => v.clone(),
        _ => return Err(anyhow!("invalid param")),
    };
    let data_len = match &params[2] {
        WasmValue::I32(v) => v.clone(),
        _ => return Err(anyhow!("invalid param")),
    };

    let data = match ctx.get_memory_region(data_ptr as usize, data_len as usize) {
        Ok(v) => {
            debug!("call: data_ptr: {:#08x} content: {:x?}", data_ptr, v);
            v.to_vec()
        }
        Err(e) => {
            error!("cannot access memory region: {}", e);
            return Ok(vec![WasmValue::I32(-1)]);
        }
    };
    let serdes = SerDesFactory::create(SerDesType::MsgPack);
    let serialized_data = match serdes.serialize(&data) {
        Ok(v) => v,
        Err(e) => {
            error!("serialize data failed: {}", e.to_string());
            return Ok(vec![WasmValue::I32(-1)]);
        }
    };
    let obj_id = match ctx.put_object(&serialized_data.as_slice()) {
        Ok(v) => v,
        Err(e) => {
            error!("put object failed: {}", e.to_string());
            return Ok(vec![WasmValue::I32(-1)]);
        }
    };

    let result = ray_buffer_write_data(
        ctx,
        ray_buf_ptr as u32,
        obj_id.id.as_slice(),
        RayBufferDataType::ObjectID,
    );
    if result.is_err() {
        error!("write object id buffer failed: {}", result.err().unwrap());
        return Ok(vec![WasmValue::I32(-1)]);
    }
    info!("ray_put: returns object_id {:x?}", obj_id);
    Ok(vec![WasmValue::I32(0)])
}

/// call ray call to invoke remote function
/// params[0]: object id buffer pointer for return value
/// params[1]: function reference value
/// params[2]: function arguments buffer pointer
/// return 0 if success, -1 if failed
pub fn hc_ray_call(ctx: &mut dyn WasmContext, params: &[WasmValue]) -> Result<Vec<WasmValue>> {
    info!("ray_call: {:x?}", params);
    let ray_buf_ptr = match &params[0] {
        WasmValue::I32(v) => v.clone(),
        _ => return Err(anyhow!("invalid param")),
    };

    let ray_buf = match read_ray_buffer(ctx, ray_buf_ptr as u32) {
        Ok(v) => {
            debug!("call: ray_buf_ptr: {:#08x} content: {:x?}", ray_buf_ptr, v);
            v
        }
        Err(_) => {
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
        Ok(_) => Ok(vec![WasmValue::I32(0)]),
        Err(e) => {
            error!("put: write data failed: {}", e);
            Ok(vec![WasmValue::I32(-1)])
        }
    }
}
