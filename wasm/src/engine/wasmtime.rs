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
use anyhow::{Error, Result};
use wasmtime::{Engine, Module, Store};

pub struct WasmtimeEngine {
    engine: Engine,
    modules: Vec<Module>,
}

impl WasmtimeEngine {
    pub fn new() -> Self {
        WasmtimeEngine {
            engine: Engine::default(),
            modules: Vec::new(),
        }
    }
}

impl WasmEngine for WasmtimeEngine {
    fn compile(&mut self, wasm_bytes: &[u8]) -> Result<Box<dyn WasmModule>> {
        let module = Module::new(&self.engine, wasm_bytes)?;
        let abs_module = WasmtimeModule::new(module.clone());
        self.modules.push(module);
        Ok(Box::new(abs_module))
    }

    fn create_sandbox(&self) -> Result<Box<dyn WasmSandbox>> {
        Ok(Box::new(WasmtimeSandbox::new()))
    }

    fn instantiate(
        &self,
        sandbox: Box<dyn WasmSandbox>,
        wasm_module: Box<dyn WasmModule>,
    ) -> Result<Box<dyn WasmInstance>> {
        Ok(Box::new(WasmtimeInstance::new()))
    }

    fn execute(
        &self,
        wasm_instance: Box<dyn WasmInstance>,
        func_name: &str,
        args: Vec<Box<dyn WasmValue>>,
    ) -> Result<Vec<Box<dyn WasmValue>>> {
        Ok(vec![Box::new(WasmtimeValue::new())])
    }

    fn list_modules(&self) -> Result<Vec<Box<dyn WasmModule>>> {
        Err(anyhow::anyhow!("Not implemented"))
    }

    fn list_sandboxes(&self) -> Result<Vec<Box<dyn WasmSandbox>>> {
        Err(anyhow::anyhow!("Not implemented"))
    }

    fn list_instances(&self, sandbox: Box<dyn WasmSandbox>) -> Result<Vec<Box<dyn WasmInstance>>> {
        Err(anyhow::anyhow!("Not implemented"))
    }

    fn register_hostcalls(&self, hostcalls: &mut Hostcalls) -> Result<()> {
        Err(anyhow::anyhow!("Not implemented"))
    }
}

struct WasmtimeModule {
    module: Module,
}

impl WasmtimeModule {
    pub fn new(m: Module) -> Self {
        WasmtimeModule {
            module: Module::new(&Engine::default(), &[]).unwrap(),
        }
    }
}

impl WasmModule for WasmtimeModule {}

struct WasmtimeSandbox {}

impl WasmtimeSandbox {
    // TODO:
    fn new() -> Self {
        WasmtimeSandbox {}
    }
}

impl WasmSandbox for WasmtimeSandbox {}

struct WasmtimeInstance {}

impl WasmtimeInstance {
    // TODO:
    fn new() -> Self {
        WasmtimeInstance {}
    }
}

impl WasmInstance for WasmtimeInstance {}

struct WasmtimeValue {}

impl WasmtimeValue {
    // TODO:
    fn new() -> Self {
        WasmtimeValue {}
    }
}

impl WasmValue for WasmtimeValue {}
