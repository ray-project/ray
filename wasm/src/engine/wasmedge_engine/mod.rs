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
use crate::engine::{Hostcalls, WasmEngine, WasmInstance, WasmModule, WasmSandbox, WasmValue};
use crate::runtime::RayRuntime;
use anyhow::Result;
use std::sync::{Arc, RwLock};

// temp Val
pub struct Value {}

pub struct WasmEdgeEngine {}

impl WasmEdgeEngine {
    pub fn new() -> Self {
        WasmEdgeEngine {}
    }
}

impl WasmEngine for WasmEdgeEngine {
    fn init(&self) -> Result<()> {
        unimplemented!()
    }

    fn compile(&mut self, _name: &str, _wasm_bytes: &[u8]) -> Result<Box<&dyn WasmModule>> {
        unimplemented!()
    }

    fn create_sandbox(&mut self, _name: &str) -> Result<Box<&dyn WasmSandbox>> {
        unimplemented!()
    }

    fn instantiate(
        &mut self,
        _sandbox_name: &str,
        _module_name: &str,
        _instance_name: &str,
    ) -> Result<Box<&dyn WasmInstance>> {
        unimplemented!()
    }

    fn execute(
        &mut self,
        _sandbox_name: &str,
        _instance_name: &str,
        _func_name: &str,
        _args: Vec<WasmValue>,
    ) -> Result<Vec<WasmValue>> {
        unimplemented!()
    }

    fn has_instance(&self, _sandbox_name: &str, _instance_name: &str) -> Result<bool> {
        unimplemented!()
    }

    fn has_module(&self, _name: &str) -> Result<bool> {
        unimplemented!()
    }

    fn has_sandbox(&self, _name: &str) -> Result<bool> {
        unimplemented!()
    }

    fn list_modules(&self) -> Result<Vec<Box<&dyn WasmModule>>> {
        unimplemented!()
    }

    fn list_sandboxes(&self) -> Result<Vec<Box<&dyn WasmSandbox>>> {
        unimplemented!()
    }

    fn list_instances(&self, _sandbox_name: &str) -> Result<Vec<Box<&dyn WasmInstance>>> {
        unimplemented!()
    }

    fn register_hostcalls(&mut self, _hostcalls: &Hostcalls) -> Result<()> {
        unimplemented!()
    }

    fn task_loop_once(
        &mut self,
        _rt: &Arc<RwLock<Box<dyn RayRuntime + Send + Sync>>>,
    ) -> Result<()> {
        unimplemented!()
    }
}

#[derive(Clone)]
struct WasmEdgeModule {}

impl WasmEdgeModule {
    pub fn new() -> Self {
        WasmEdgeModule {}
    }
}

impl WasmModule for WasmEdgeModule {}

#[derive(Clone)]
struct WasmEdgeSandbox {}

impl WasmEdgeSandbox {
    pub fn new() -> Self {
        WasmEdgeSandbox {}
    }
}

impl WasmSandbox for WasmEdgeSandbox {
    // TODO: implement WasmSandbox
}

#[derive(Clone)]
struct WasmEdgeInstance {}

impl WasmEdgeInstance {
    pub fn new() -> Self {
        WasmEdgeInstance {}
    }
}

impl WasmInstance for WasmEdgeInstance {
    // TODO: implement WasmInstance
}
