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
use crate::engine::{WasmEngine, WasmInstance, WasmModule, WasmSandbox, WasmValue};
use crate::ray::{Hostcall, Hostcalls};
use anyhow::Result;

pub struct WasmEdgeEngine {}

impl WasmEdgeEngine {
    pub fn new() -> Self {
        WasmEdgeEngine {}
    }
}

impl WasmEngine for WasmEdgeEngine {
    fn init(&self) -> Result<()> {
        Err(anyhow::anyhow!("WasmEdgeEngine::init is not implemented"))
    }

    fn compile(&mut self, name: &str, wasm_bytes: &[u8]) -> Result<Box<&dyn WasmModule>> {
        Err(anyhow::anyhow!(
            "WasmEdgeEngine::compile is not implemented"
        ))
    }

    fn create_sandbox(&mut self, name: &str) -> Result<Box<&dyn WasmSandbox>> {
        Err(anyhow::anyhow!(
            "WasmEdgeEngine::create_sandbox is not implemented"
        ))
    }

    fn instantiate(
        &mut self,
        sandbox_name: &str,
        module_name: &str,
        instance_name: &str,
    ) -> Result<Box<&dyn WasmInstance>> {
        Err(anyhow::anyhow!(
            "WasmEdgeEngine::instantiate is not implemented"
        ))
    }

    fn execute(
        &mut self,
        instance_name: &str,
        func_name: &str,
        args: Vec<Box<dyn WasmValue>>,
    ) -> Result<Vec<Box<dyn WasmValue>>> {
        Err(anyhow::anyhow!(
            "WasmEdgeEngine::execute is not implemented"
        ))
    }

    fn list_modules(&self) -> Result<Vec<Box<&dyn WasmModule>>> {
        Err(anyhow::anyhow!(
            "WasmEdgeEngine::list_modules is not implemented"
        ))
    }

    fn list_sandboxes(&self) -> Result<Vec<Box<&dyn WasmSandbox>>> {
        Err(anyhow::anyhow!(
            "WasmEdgeEngine::list_sandboxes is not implemented"
        ))
    }

    fn list_instances(&self, sandbox_name: &str) -> Result<Vec<Box<&dyn WasmInstance>>> {
        Err(anyhow::anyhow!(
            "WasmEdgeEngine::list_instances is not implemented"
        ))
    }

    fn register_hostcalls(&self, hostcalls: &mut Hostcalls) -> Result<()> {
        Err(anyhow::anyhow!(
            "WasmEdgeEngine::register_hostcalls is not implemented"
        ))
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

struct WasmEdgeValue {}

impl WasmEdgeValue {
    pub fn new() -> Self {
        WasmEdgeValue {}
    }
}

impl WasmValue for WasmEdgeValue {
    // TODO: implement WasmValue
}
