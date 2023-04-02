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

use crate::engine::{WasmEngine, WasmInstance, WasmModule, WasmSandbox, WasmType, WasmValue};
use crate::ray::{Hostcall, Hostcalls};
use anyhow::Result;
use std::collections::HashMap;
use wasmtime::{Engine, FuncType, Instance, Linker, Module, Store, Val, ValRaw, ValType};

mod data;
use data::*;

use super::wasmedge_engine;

pub struct WasmtimeEngine {
    engine: Engine,
    linker: Linker<WasmtimeStoreData>,
    modules: HashMap<String, WasmtimeModule>,
    sandboxes: HashMap<String, WasmtimeSandbox>,
}

impl WasmtimeEngine {
    pub fn new() -> Self {
        let engine = Engine::default();
        WasmtimeEngine {
            engine: engine.clone(),
            linker: Linker::new(&engine),
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
        let instance = self
            .linker
            .instantiate(&mut sandbox.store, &module.module)
            .unwrap();
        sandbox
            .instances
            .insert(instance_name.to_string(), WasmtimeInstance { instance });
        Ok(Box::new(
            &self.sandboxes[sandbox_name].instances[instance_name],
        ))
    }

    fn execute(
        &mut self,
        sandbox_name: &str,
        instance_name: &str,
        func_name: &str,
        args: Vec<WasmValue>,
    ) -> Result<Vec<WasmValue>> {
        let func;
        {
            let sandbox = self.sandboxes.get_mut(sandbox_name).unwrap();
            let store = &mut sandbox.store;
            let instance = sandbox.instances.get(instance_name).unwrap();
            func = instance.instance.get_func(store, func_name).unwrap();
        }

        {
            let sandbox = self.sandboxes.get_mut(sandbox_name).unwrap();
            let store = &mut sandbox.store;
            // execute function
            let args = args
                .iter()
                .map(|arg| to_wasmtime_value(arg))
                .collect::<Vec<Val>>();
            let mut results = vec![Val::I32(0); func.ty(&store).results().len()];

            func.call(store, &args.as_slice(), &mut results).unwrap();

            let mut returns = Vec::new();
            for r in results.iter() {
                returns.push(from_wasmtime_value(&WasmType::I32, r));
            }
            Ok(returns)
        }
    }

    fn list_modules(&self) -> Result<Vec<Box<&dyn WasmModule>>> {
        unimplemented!()
    }

    fn list_sandboxes(&self) -> Result<Vec<Box<&dyn WasmSandbox>>> {
        unimplemented!()
    }

    fn list_instances(&self, sandbox_name: &str) -> Result<Vec<Box<&dyn WasmInstance>>> {
        unimplemented!()
    }

    fn register_hostcalls(&mut self, hostcalls: &Hostcalls) -> Result<()> {
        // iterate all hostcalls
        for hostcall in hostcalls.functions.iter() {
            // register hostcall to wasmtime linker
            let ft = FuncType::new(
                hostcall
                    .params
                    .iter()
                    .map(|p| wasmtime_type(p))
                    .collect::<Vec<wasmtime::ValType>>(),
                hostcall
                    .results
                    .iter()
                    .map(|r| wasmtime_type(r))
                    .collect::<Vec<wasmtime::ValType>>(),
            );
            unsafe {
                let module_name = hostcalls.module_name.clone();
                let hostcall = hostcall.clone();
                let res = self.linker.func_new_unchecked(
                    &module_name,
                    &hostcall.name,
                    ft,
                    move |caller, args| -> Result<()> {
                        // iterate params and convert them to WasmValue
                        let mut index = 0;
                        let mut params = Vec::new();
                        if args.len() < hostcall.params.len()
                            || args.len() != hostcall.params.len() + hostcall.results.len()
                        {
                            return Err(anyhow::anyhow!("Not enough params"));
                        }
                        for param in hostcall.params.iter() {
                            params.push(from_wasmtime_raw_value(param, &args[index]));
                            index += 1;
                        }

                        // call hostcall
                        let result = (hostcall.func)(&params);
                        if result.is_err() {
                            return Err(anyhow::anyhow!("Failed to call hostcall"));
                        }

                        let result = result.unwrap();

                        // iterate results and convert them to ValRaw
                        for result_type in hostcall.results.iter() {
                            args[index] = to_wasmtime_raw_value(&result[index]);
                            index += 1;
                        }
                        Ok(())
                    },
                );
                if res.is_err() {
                    return Err(anyhow::anyhow!("Failed to register hostcall"));
                }
            }
        }
        Ok(())
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
struct WasmtimeStoreData {}

struct WasmtimeSandbox {
    store: Store<WasmtimeStoreData>,
    instances: HashMap<String, WasmtimeInstance>,
}

impl WasmtimeSandbox {
    fn new(engine: &Engine) -> Self {
        let store = Store::new(engine, WasmtimeStoreData {});
        WasmtimeSandbox {
            store,
            instances: HashMap::new(),
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
