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

mod wasmedge_engine;
mod wasmtime_engine;

use std::any::Any;
use std::sync::Arc;
use std::sync::RwLock;

use crate::engine::wasmtime_engine::WasmtimeEngine;
use crate::runtime::InvocationSpec;
use crate::runtime::ObjectID;
use crate::runtime::RemoteFunctionHolder;
use crate::{engine::wasmedge_engine::WasmEdgeEngine, runtime::RayRuntime};

use anyhow::{anyhow, Ok, Result};

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
    pub fn create_engine(engine_type: WasmEngineType) -> Box<dyn WasmEngine + Send + Sync> {
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

#[derive(Clone)]
pub struct Hostcalls {
    pub module_name: String,
    pub functions: Vec<Hostcall>,
    pub runtime: Arc<RwLock<Box<dyn RayRuntime + Send + Sync>>>,
}

#[derive(Clone)]
pub struct Hostcall {
    pub name: String,
    pub params: Vec<WasmType>,
    pub results: Vec<WasmType>,
    pub func: fn(&mut dyn WasmContext, &[WasmValue]) -> Result<Vec<WasmValue>>,
}

#[derive(Debug, Clone)]
pub struct WasmFunc {
    pub name: String,
    pub module: String,
    pub params: Vec<WasmType>,
    pub results: Vec<WasmType>,
}

impl WasmFunc {
    /// Get the size of the data needed by all the parameters
    pub fn params_data_size(&self) -> Result<usize> {
        let mut total_size = 0;
        for i in self.params.iter() {
            match i {
                WasmType::I32 => {
                    total_size += 4;
                }
                WasmType::I64 => {
                    total_size += 8;
                }
                WasmType::F32 => {
                    total_size += 4;
                }
                WasmType::F64 => {
                    total_size += 8;
                }
                _ => {
                    return Err(anyhow!("unsupported argument type"));
                }
            }
        }
        Ok(total_size)
    }

    /// Create a vector of all the parsed parameters from raw memory data
    pub fn params_convert(&self, data: &[u8]) -> Result<Vec<WasmValue>> {
        if data.len() != self.params_data_size()? {
            return Err(anyhow!("invalid data size"));
        }
        // iterate all arguments and put them into a arguments vector
        let mut args = vec![];
        let mut param_offset = 0;
        for i in self.params.iter() {
            match i {
                WasmType::I32 => {
                    // convert 4 bytes to i32
                    let val = i32::from_ne_bytes(
                        data[param_offset..param_offset + 4].try_into().unwrap(),
                    );
                    let arg = WasmValue::I32(val);
                    args.push(arg);
                    param_offset += 4;
                }
                WasmType::I64 => {
                    // convert 8 bytes to i64
                    let val = i64::from_ne_bytes(
                        data[param_offset..param_offset + 8].try_into().unwrap(),
                    );
                    let arg = WasmValue::I64(val);
                    args.push(arg);
                    param_offset += 8;
                }
                WasmType::F32 => {
                    // convert 4 bytes to u32
                    let val = u32::from_ne_bytes(
                        data[param_offset..param_offset + 4].try_into().unwrap(),
                    );
                    let arg = WasmValue::F32(val);
                    args.push(arg);
                    param_offset += 4;
                }
                WasmType::F64 => {
                    // convert 8 bytes to u64
                    let val = u64::from_ne_bytes(
                        data[param_offset..param_offset + 8].try_into().unwrap(),
                    );
                    let arg = WasmValue::F64(val);
                    args.push(arg);
                    param_offset += 8;
                }
                _ => {
                    return Err(anyhow!("unsupported argument type"));
                }
            }
        }
        Ok(args)
    }
}

impl Hostcalls {
    pub fn new(module_name: &str, runtime: Arc<RwLock<Box<dyn RayRuntime + Send + Sync>>>) -> Self {
        Hostcalls {
            module_name: module_name.to_string(),
            functions: Vec::new(),
            runtime,
        }
    }

    pub fn add_hostcall(
        &mut self,
        name: &str,
        params: Vec<WasmType>,
        results: Vec<WasmType>,
        func: fn(&mut dyn WasmContext, &[WasmValue]) -> Result<Vec<WasmValue>>,
    ) -> Result<()> {
        if self.functions.iter().any(|f| f.name == name) {
            return Err(anyhow!("Hostcall {} already exists", name));
        }
        self.functions.push(Hostcall {
            name: name.to_string(),
            params,
            results,
            func,
        });
        Ok(())
    }

    pub fn remove_hostcall(&mut self, name: &str) -> Result<()> {
        if self.functions.iter().any(|f| f.name == name) {
            return Err(anyhow!("Hostcall {} does not exist", name));
        }
        self.functions.retain(|f| f.name != name);
        Ok(())
    }
}

pub trait WasmContext {
    fn get_memory_region(&mut self, off: usize, len: usize) -> Result<&[u8]>;
    fn get_memory_region_mut(&mut self, off: usize, len: usize) -> Result<&mut [u8]>;
    fn get_func_ref(&mut self, func_idx: u32) -> Result<WasmFunc>;
    fn invoke(
        &mut self,
        remote_func: &RemoteFunctionHolder,
        args: &[WasmValue],
    ) -> Result<Vec<ObjectID>>;
    fn get_object(&mut self, object_id: &ObjectID) -> Result<Vec<u8>>;
}
