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

use crate::engine::{TASK_RECEIVER, TASK_RESULT_SENDER};
use crate::runtime::common_proto::TaskType;
use crate::runtime::{CallOptions, ObjectID};
use crate::{
    engine::{
        Hostcalls, WasmEngine, WasmFunc, WasmInstance, WasmModule, WasmSandbox, WasmType, WasmValue,
    },
    runtime::{InvocationSpec, RayRuntime, RemoteFunctionHolder},
};
use anyhow::{anyhow, Result};
use std::time::Duration;
use std::{
    collections::HashMap,
    fmt::format,
    sync::{Arc, RwLock},
};
use tokio::sync::watch::error;
use tracing::{debug, error, info};
use wasmtime::{
    AsContextMut, Caller, Engine, FuncType, Instance, Linker, Module, Store, Val, ValType,
};

use crate::util::RayLog;
use rmp::encode::write_i32;

mod data;
use data::*;

use super::WasmContext;

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

            match func.call(store, &args.as_slice(), &mut results) {
                Ok(_) => {}
                Err(e) => {
                    error!("Failed to execute function: {}, error: {:?}", func_name, e);
                    return Err(anyhow!(
                        "Failed to execute function: {}, error: {:?}",
                        func_name,
                        e
                    ));
                }
            }

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
                    .collect::<Vec<ValType>>(),
                hostcall
                    .results
                    .iter()
                    .map(|r| wasmtime_type(r))
                    .collect::<Vec<ValType>>(),
            );
            unsafe {
                let module_name = hostcalls.module_name.clone();
                let hostcall = hostcall.clone();
                let runtime = hostcalls.runtime.clone();
                debug!(
                    "Registered hostcall: {}, params# {} result# {}, func_type: {:?}",
                    hostcall.name,
                    hostcall.params.len(),
                    hostcall.results.len(),
                    ft
                );
                let res = self.linker.func_new_unchecked(
                    &module_name,
                    &hostcall.name,
                    ft,
                    move |mut caller, args| -> Result<()> {
                        // iterate params and convert them to WasmValue
                        let mut params = Vec::new();
                        if args.len()
                            != std::cmp::max(hostcall.params.len(), hostcall.results.len())
                        {
                            return Err(anyhow!(
                                "Invalid number of arguments. given {} vs expected {} + {}",
                                args.len(),
                                hostcall.params.len(),
                                hostcall.results.len()
                            ));
                        }
                        for index in 0..hostcall.params.len() {
                            params.push(from_wasmtime_raw_value(
                                &hostcall.params[index],
                                &args[index],
                            ));
                        }

                        let mut tmp_caller = caller;
                        let mut ctx = WasmtimeContext {
                            caller: &mut tmp_caller,
                            runtime: &runtime,
                        };

                        // call hostcall
                        let result = (hostcall.func)(&mut ctx, &params);
                        if result.is_err() {
                            return Err(anyhow!("Failed to call hostcall"));
                        }

                        let result = result.unwrap();

                        // iterate results and convert them to ValRaw
                        for index in 0..hostcall.results.len() {
                            args[index] = to_wasmtime_raw_value(&result[index]);
                        }
                        Ok(())
                    },
                );
                if res.is_err() {
                    return Err(anyhow!("Failed to register hostcall"));
                }
            }
        }
        Ok(())
    }

    fn task_loop_once(&mut self) -> Result<()> {
        let mut buf: Vec<u8> = vec![];
        // receive task from channel with a timeout
        let receiver = TASK_RECEIVER.lock().ok().unwrap();
        match receiver.as_ref() {
            Some(rx) => match rx.recv_timeout(Duration::from_millis(100)) {
                Ok(task) => {
                    RayLog::info(
                        format!("task_loop_once: executing wasm task: {:?}", task).as_str(),
                    );

                    // TODO: run task
                    write_i32(&mut buf, 0x12345678).unwrap();
                }
                Err(e) => {
                    return Ok(()); // timeout
                }
            },
            None => {
                RayLog::error("task_loop_once: channel not initialized");
                return Err(anyhow!("task_loop_once: channel not initialized"));
            }
        }

        // we got result here
        let sender = TASK_RESULT_SENDER.lock().ok().unwrap();
        match sender.as_ref() {
            Some(tx) => {
                tx.send(buf).unwrap();
            }
            None => {
                RayLog::error("task_loop_once: channel not initialized");
                return Err(anyhow!("task_loop_once: channel not initialized"));
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

pub struct WasmtimeContext<'a> {
    caller: &'a mut Caller<'a, WasmtimeStoreData>,
    runtime: &'a Arc<RwLock<Box<dyn RayRuntime + Send + Sync>>>,
}

impl WasmContext for WasmtimeContext<'_> {
    fn get_memory_region(&mut self, off: usize, len: usize) -> Result<&[u8]> {
        let memory = self
            .caller
            .get_export("memory")
            .unwrap()
            .into_memory()
            .unwrap();
        // check memory size
        if memory.data_size(self.caller.as_context_mut()) < off + len {
            return Err(anyhow!("Invalid memory access"));
        }
        let data = memory.data(self.caller.as_context_mut());
        Ok(&data[off..off + len])
    }

    fn get_memory_region_mut(&mut self, off: usize, len: usize) -> Result<&mut [u8]> {
        let memory = self
            .caller
            .get_export("memory")
            .unwrap()
            .into_memory()
            .unwrap();
        // check memory size
        if memory.data_size(self.caller.as_context_mut()) < off + len {
            return Err(anyhow!("Invalid memory access"));
        }
        let data = memory.data_mut(self.caller.as_context_mut());
        Ok(&mut data[off..off + len])
    }

    fn get_func_ref(&mut self, func_idx: u32) -> Result<WasmFunc> {
        let func_tbl = self
            .caller
            .get_export("__indirect_function_table")
            .unwrap()
            .into_table()
            .unwrap();
        let binding = func_tbl
            .get(self.caller.as_context_mut(), func_idx)
            .unwrap();
        let func = binding.unwrap_funcref().unwrap();
        let params: Vec<WasmType> = func
            .ty(self.caller.as_context_mut())
            .params()
            .map(|p| from_wasmtime_type(&p))
            .collect();
        let results: Vec<WasmType> = func
            .ty(self.caller.as_context_mut())
            .results()
            .map(|p| from_wasmtime_type(&p))
            .collect();
        Ok(WasmFunc {
            name: format!("{}", func_idx), // TODO: we need to get the real name
            module: "n/a".to_string(),
            params,
            results,
        })
    }

    fn invoke(
        &mut self,
        remote_func_holder: &RemoteFunctionHolder,
        args: &[WasmValue],
    ) -> Result<Vec<ObjectID>> {
        let call_opt = CallOptions::new();
        let invoke_spec =
            InvocationSpec::new(TaskType::NormalTask, remote_func_holder.clone(), args, None);
        let result = self.runtime.read().unwrap().call(&invoke_spec, &call_opt);
        match result {
            Ok(result) => {
                if result.len() != 1 {
                    return Err(anyhow!("Invalid result length"));
                }
                Ok(result)
            }
            Err(e) => Err(anyhow!("Failed to invoke remote function: {}", e)),
        }
    }

    fn get_object(&mut self, object_id: &ObjectID) -> Result<Vec<u8>> {
        debug!("get_object: {:x?}", object_id);
        let object = match self.runtime.read().unwrap().get(object_id) {
            Ok(object) => object,
            Err(e) => {
                error!("Failed to get object: {}", e);
                return Err(anyhow!("Failed to get object: {}", e));
            }
        };
        Ok(object)
    }
}
