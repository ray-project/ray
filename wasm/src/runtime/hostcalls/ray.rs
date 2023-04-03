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

use std::vec;

use crate::engine::{Hostcalls, WasmContext, WasmType, WasmValue};
use anyhow::{anyhow, Result};
use tracing::info;

pub fn new_ray_hostcalls() -> Hostcalls {
    let mut hostcalls = Hostcalls::new("ray");
    hostcalls
        .register_function("test", vec![], vec![], hc_test)
        .unwrap();
    hostcalls
        .register_function("init", vec![], vec![], hc_init)
        .unwrap();
    hostcalls
        .register_function("shutdown", vec![], vec![], hc_shutdown)
        .unwrap();
    hostcalls
        .register_function("get", vec![], vec![], hc_get)
        .unwrap();
    hostcalls
        .register_function("put", vec![], vec![], hc_put)
        .unwrap();
    hostcalls
        .register_function(
            "call",
            vec![WasmType::I32, WasmType::I32],
            vec![WasmType::I32],
            hc_call,
        )
        .unwrap();
    hostcalls
}

fn hc_test(ctx: &mut dyn WasmContext, params: &[WasmValue]) -> Result<Vec<WasmValue>> {
    info!("test");
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
    info!("call: {:?}", params);
    let func_ref = match &params[0] {
        WasmValue::I32(v) => v,
        _ => return Err(anyhow!("invalid param")),
    };
    let args_ptr = match &params[1] {
        WasmValue::I32(v) => v,
        _ => return Err(anyhow!("invalid param")),
    };
    match ctx.get_memory_region(*args_ptr as usize, 10) {
        Ok(v) => info!("call: args_ptr: {:x?}", v),
        Err(e) => info!("call: args_ptr: {:?}", e),
    }
    info!("call: func_ref: {}, args_ptr: {:#08x}", func_ref, args_ptr);
    Ok(vec![WasmValue::I32(0)])
}
