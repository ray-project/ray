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
use std::{sync::RwLock, vec};

use crate::engine::WasmEngine;
use crate::{
    engine::{Hostcalls, WasmContext, WasmType, WasmValue},
    runtime::RayRuntime,
};
use anyhow::{anyhow, Result};
use tracing::{debug, error, info};

pub fn register_ray_hostcalls(
    runtime: &Arc<RwLock<Box<dyn RayRuntime + Send + Sync>>>,
    engine: &Arc<RwLock<Box<dyn WasmEngine + Send + Sync>>>,
) -> Result<()> {
    let mut hostcalls = Hostcalls::new("ray", runtime.clone());
    hostcalls
        .add_hostcall("test", vec![], vec![], hc_test)
        .unwrap();
    hostcalls
        .add_hostcall("init", vec![], vec![], hc_init)
        .unwrap();
    hostcalls
        .add_hostcall("shutdown", vec![], vec![], hc_shutdown)
        .unwrap();
    hostcalls
        .add_hostcall("get", vec![], vec![], hc_get)
        .unwrap();
    hostcalls
        .add_hostcall("put", vec![], vec![], hc_put)
        .unwrap();
    hostcalls
        .add_hostcall(
            "call",
            vec![WasmType::I32, WasmType::I32],
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

fn hc_init(ctx: &mut dyn WasmContext, params: &[WasmValue]) -> Result<Vec<WasmValue>> {
    Err(anyhow!("not implemented"))
}

fn hc_shutdown(ctx: &mut dyn WasmContext, params: &[WasmValue]) -> Result<Vec<WasmValue>> {
    Err(anyhow!("not implemented"))
}

fn hc_get(ctx: &mut dyn WasmContext, params: &[WasmValue]) -> Result<Vec<WasmValue>> {
    Err(anyhow!("not implemented"))
}

fn hc_put(ctx: &mut dyn WasmContext, params: &[WasmValue]) -> Result<Vec<WasmValue>> {
    Err(anyhow!("not implemented"))
}

fn hc_call(ctx: &mut dyn WasmContext, params: &[WasmValue]) -> Result<Vec<WasmValue>> {
    debug!("call: {:?}", params);
    let func_ref = match &params[0] {
        WasmValue::I32(v) => v,
        _ => return Err(anyhow!("invalid param")),
    };
    let args_ptr = match &params[1] {
        WasmValue::I32(v) => v,
        _ => return Err(anyhow!("invalid param")),
    };
    match ctx.get_memory_region(*args_ptr as usize, 10) {
        Ok(v) => {
            info!(
                "call: func_ref: {}, args_ptr: {:#08x} content: {:x?}",
                func_ref, args_ptr, v
            );
        }
        Err(e) => {
            error!("cannot access memory region: {}", e);
            return Ok(vec![WasmValue::I32(-1)]);
        }
    }
    Ok(vec![WasmValue::I32(0)])
}
