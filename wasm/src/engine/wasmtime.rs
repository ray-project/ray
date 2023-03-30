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
use std::collections::HashMap;
use wasmtime::{Engine, Instance, Linker, Module, Store};
use wasmtime_wasi::sync::WasiCtxBuilder;

pub struct WasmtimeEngine {
    engine: Engine,
    modules: HashMap<String, WasmtimeModule>,
    sandboxes: HashMap<String, WasmtimeSandbox>,
}

impl WasmtimeEngine {
    pub fn new() -> Self {
        WasmtimeEngine {
            engine: Engine::default(),
            modules: HashMap::new(),
            sandboxes: HashMap::new(),
        }
    }
}

impl WasmEngine for WasmtimeEngine {
    fn init(&self) -> Result<()> {
        Ok(())
    }

    fn compile(&mut self, name: &str, wasm_bytes: &[u8]) -> Result<Box<&dyn WasmModule>> {
        let module = WasmtimeModule::new(&self.engine, wasm_bytes);
        self.modules.insert(name.to_string(), module);
        Ok(Box::new(&self.modules[name]))
    }

    fn create_sandbox(&mut self, name: &str) -> Result<Box<&dyn WasmSandbox>> {
        let sandbox = WasmtimeSandbox::new(&self.engine);
        self.sandboxes.insert(name.to_string(), sandbox);
        Ok(Box::new(&self.sandboxes[name]))
    }

    fn instantiate(
        &mut self,
        sandbox_name: &str,
        module_name: &str,
        instance_name: &str,
    ) -> Result<Box<&dyn WasmInstance>> {
        // convert sandbox to wasmtime store
        let sandbox = &mut self.sandboxes.get_mut(sandbox_name).unwrap();
        let module = &mut self.modules.get_mut(module_name).unwrap();
        let instance = WasmtimeInstance::new(sandbox, module);
        sandbox
            .instances
            .insert(instance_name.to_string(), instance);
        Ok(Box::new(
            &self.sandboxes[sandbox_name].instances[instance_name],
        ))
    }

    fn execute(
        &mut self,
        instance_name: &str,
        func_name: &str,
        args: Vec<Box<dyn WasmValue>>,
    ) -> Result<Vec<Box<dyn WasmValue>>> {
        Ok(vec![Box::new(WasmtimeValue::new())])
    }

    fn list_modules(&self) -> Result<Vec<Box<&dyn WasmModule>>> {
        Err(anyhow::anyhow!("Not implemented"))
    }

    fn list_sandboxes(&self) -> Result<Vec<Box<&dyn WasmSandbox>>> {
        Err(anyhow::anyhow!("Not implemented"))
    }

    fn list_instances(&self, sandbox_name: &str) -> Result<Vec<Box<&dyn WasmInstance>>> {
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
    pub fn new(engine: &Engine, bytes: &[u8]) -> Self {
        WasmtimeModule {
            module: Module::from_binary(engine, bytes).unwrap(),
        }
    }
}

impl WasmModule for WasmtimeModule {}

#[derive(Clone)]
struct WasmtimeStoreData {
    wasi: wasmtime_wasi::sync::WasiCtx,
}

struct WasmtimeSandbox {
    store: Store<WasmtimeStoreData>,
    linker: Linker,
    instances: HashMap<String, WasmtimeInstance>,
}

impl WasmtimeSandbox {
    fn new(engine: &Engine) -> Self {
        let mut linker = Linker::new(engine);
        wasmtime_wasi::add_to_linker(&mut linker, |s| s)?;
        let wasi = WasiCtxBuilder::new()
            .inherit_stdio()
            .inherit_args()
            .unwrap()
            .build();
        WasmtimeSandbox {
            store: Store::new(engine, WasmtimeStoreData { wasi }),
            instances: HashMap::new(),
            linker,
        }
    }
}

impl WasmSandbox for WasmtimeSandbox {}

#[derive(Clone)]
struct WasmtimeInstance {
    instance: Instance,
}

impl WasmtimeInstance {
    fn new(sandbox: &mut WasmtimeSandbox, module: &mut WasmtimeModule) -> Self {
        match Instance::new(&mut sandbox.store, &module.module, &[]) {
            Ok(instance) => {
                return WasmtimeInstance { instance };
            }
            Err(e) => {
                panic!("failed to instantiate module: {}", e);
            }
        }
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
