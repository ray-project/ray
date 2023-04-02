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

use super::{Hostcall, Hostcalls};
use crate::engine::{WasmEngine, WasmType, WasmValue};
use anyhow::Result;
use tracing::info;

pub fn new_ray_hostcalls() -> Hostcalls {
    let mut hostcalls = Hostcalls::new("ray");
    hostcalls.register_function("test", vec![], vec![], test);
    hostcalls.register_function("init", vec![], vec![], init);
    hostcalls.register_function("shutdown", vec![], vec![], shutdown);
    hostcalls.register_function("get", vec![], vec![], get);
    hostcalls.register_function("put", vec![], vec![], put);
    hostcalls
}

fn test(params: &[WasmValue]) -> Result<Vec<WasmValue>> {
    info!("test");
    Ok(vec![])
}

fn init(params: &[WasmValue]) -> Result<Vec<WasmValue>> {
    Err(anyhow::anyhow!("not implemented"))
}

fn shutdown(params: &[WasmValue]) -> Result<Vec<WasmValue>> {
    Err(anyhow::anyhow!("not implemented"))
}

fn get(params: &[WasmValue]) -> Result<Vec<WasmValue>> {
    Err(anyhow::anyhow!("not implemented"))
}

fn put(params: &[WasmValue]) -> Result<Vec<WasmValue>> {
    Err(anyhow::anyhow!("not implemented"))
}
