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

use anyhow::Result;
use wasmtime::{Val, ValRaw};

mod wasmedge_engine;
mod wasmtime_engine;

use crate::engine::wasmedge_engine::WasmEdgeEngine;
use crate::engine::wasmtime_engine::WasmtimeEngine;
use crate::runtime::{Hostcall, Hostcalls};

pub trait WasmEngine {
    fn init(&self) -> Result<()>;

    fn compile(&mut self, name: &str, wasm_bytes: &[u8]) -> Result<Box<&dyn WasmModule>>;

    fn create_sandbox(&mut self, name: &str) -> Result<Box<&dyn WasmSandbox>>;
    fn instantiate(
        &mut self,
        sandbox_name: &str,
        module_name: &str,
        instance_name: &str,
    ) -> Result<Box<&dyn WasmInstance>>;
    fn execute(
        &mut self,
        sandbox_name: &str,
        instance_name: &str,
        func_name: &str,
        args: Vec<WasmValue>,
    ) -> Result<Vec<WasmValue>>;

    fn list_modules(&self) -> Result<Vec<Box<&dyn WasmModule>>>;
    fn list_sandboxes(&self) -> Result<Vec<Box<&dyn WasmSandbox>>>;
    fn list_instances(&self, sandbox_name: &str) -> Result<Vec<Box<&dyn WasmInstance>>>;

    fn register_hostcalls(&mut self, hostcalls: &Hostcalls) -> Result<()>;
}

pub trait WasmModule {}

pub trait WasmSandbox {
    // convert to original type
}

pub trait WasmInstance {}

pub enum WasmEngineType {
    WASMEDGE,
    WASMTIME,
    WAMR,
    WAVM,
}

// factory pattern for wasm engine
pub struct WasmEngineFactory {}

impl WasmEngineFactory {
    pub fn create_engine(engine_type: WasmEngineType) -> Box<dyn WasmEngine> {
        match engine_type {
            WasmEngineType::WASMTIME => Box::new(WasmtimeEngine::new()),
            WasmEngineType::WASMEDGE => Box::new(WasmEdgeEngine::new()),
            _ => panic!("not supported engine type"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WasmValue {
    I32(i32),
    I64(i64),
    F32(u32),
    F64(u64),
    V128(u128),
    FuncRef(usize),
    ExternRef(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WasmType {
    I32,
    I64,
    F32,
    F64,
    V128,
    FuncRef,
    ExternRef,
}
