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

use crate::engine::{
    Hostcalls, WasmEngine, WasmFunc, WasmInstance, WasmModule, WasmSandbox, WasmType, WasmValue,
};
use crate::engine::{TASK_RECEIVER, TASK_RESULT_SENDER};
use crate::runtime::RayRuntime;
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tracing::debug;
use wasmtime::{Engine, FuncType, Instance, Linker, Module, Store, ValRaw, ValType};

use crate::util::RayLog;

mod data;
use data::*;
mod context;
use context::*;

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

struct WasmtimeModule {
    name: String,
    module: Module,
    bytes: Vec<u8>,
}

impl WasmtimeModule {
    pub fn new(engine: &Engine, module_name: &str, bytes: &[u8]) -> Self {
        WasmtimeModule {
            name: module_name.to_string(),
            module: Module::from_binary(engine, bytes).unwrap(),
            bytes: bytes.to_vec(),
        }
    }
}

impl WasmModule for WasmtimeModule {}

#[derive(Clone)]
pub struct WasmtimeStoreData {
    sandbox_name: String,
    module_bytes: HashMap<String, Vec<u8>>,
    submitted_modules: HashMap<String, String>, // module_name -> module_hash
}

struct WasmtimeSandbox {
    store: Store<WasmtimeStoreData>,
    instances: HashMap<String, WasmtimeInstance>,
}

impl WasmtimeSandbox {
    fn new(engine: &Engine, name: &str) -> Self {
        let store = Store::new(
            engine,
            WasmtimeStoreData {
                sandbox_name: name.to_string(),
                module_bytes: HashMap::new(),
                submitted_modules: HashMap::new(),
            },
        );
        WasmtimeSandbox {
            store,
            instances: HashMap::new(),
        }
    }
}

impl WasmSandbox for WasmtimeSandbox {}

#[derive(Clone)]
struct WasmtimeInstance {
    name: String,
    module_name: String,
    instance: Instance,
}

impl WasmtimeInstance {
    fn new(name: &str, module_name: &str, instance: Instance) -> Self {
        WasmtimeInstance {
            name: name.to_string(),
            module_name: module_name.to_string(),
            instance,
        }
    }
}

impl WasmInstance for WasmtimeInstance {}

impl WasmEngine for WasmtimeEngine {
    fn init(&self) -> Result<()> {
        Ok(())
    }

    fn compile(&mut self, module_name: &str, wasm_bytes: &[u8]) -> Result<Box<&dyn WasmModule>> {
        let module = WasmtimeModule::new(&self.engine, module_name, wasm_bytes);
        self.modules.insert(module_name.to_string(), module);
        Ok(Box::new(&self.modules[module_name]))
    }

    fn create_sandbox(&mut self, name: &str) -> Result<Box<&dyn WasmSandbox>> {
        let sandbox = WasmtimeSandbox::new(&self.engine, name);
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
        let sandbox = match self.sandboxes.get_mut(sandbox_name) {
            Some(s) => s,
            None => {
                return Err(anyhow!("Failed to get sandbox: {}", sandbox_name));
            }
        };
        let module = match self.modules.get_mut(module_name) {
            Some(m) => m,
            None => {
                return Err(anyhow!("Failed to get module: {}", module_name));
            }
        };
        let instance = match self.linker.instantiate(&mut sandbox.store, &module.module) {
            Ok(instance) => instance,
            Err(e) => {
                return Err(anyhow!("Failed to instantiate module: {}", e));
            }
        };
        sandbox.instances.insert(
            instance_name.to_string(),
            WasmtimeInstance::new(instance_name, module_name, instance),
        );
        sandbox
            .store
            .data_mut()
            .module_bytes
            .insert(module_name.to_string(), module.bytes.clone());
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
        let sandbox = match self.sandboxes.get_mut(sandbox_name) {
            Some(s) => s,
            None => {
                RayLog::error(format!("Failed to get sandbox: {}", sandbox_name).as_str());
                return Err(anyhow!("Failed to get sandbox: {}", sandbox_name));
            }
        };
        let mut store = &mut sandbox.store;
        // execute function
        let args = args
            .iter()
            .map(|arg| to_wasmtime_raw_value(arg))
            .collect::<Vec<ValRaw>>();
        let mut results: Vec<ValRaw>;

        let instance = match sandbox.instances.get(instance_name) {
            Some(i) => i,
            None => {
                return Err(anyhow!("Failed to get instance: {}", instance_name));
            }
        };
        match func_name.parse::<u32>() {
            Ok(n) => {
                // get indirect function table and get function by index
                let func_tbl = match instance
                    .instance
                    .get_export(&mut store, "__indirect_function_table")
                {
                    Some(f) => match f.into_table() {
                        Some(t) => t,
                        None => {
                            RayLog::error("Failed to get indirect function table");
                            return Err(anyhow!("Failed to get indirect function table"));
                        }
                    },
                    None => {
                        RayLog::error("Failed to get indirect function table");
                        return Err(anyhow!("Failed to get indirect function table"));
                    }
                };
                match func_tbl.get(&mut store, n) {
                    Some(f) => {
                        let func = match f.unwrap_funcref() {
                            Some(f) => f,
                            None => {
                                RayLog::error("Failed to get function");
                                return Err(anyhow!("Failed to get function"));
                            }
                        };
                        let mut params_and_returns: Vec<ValRaw> = Vec::new();
                        // iterate all args and push them to params_and_returns
                        for arg in args.iter() {
                            params_and_returns.push(arg.clone());
                        }
                        // get the number of return values
                        let return_types = func
                            .ty(&store)
                            .results()
                            .map(|r| r.clone())
                            .collect::<Vec<ValType>>();
                        if return_types.len() > args.len() {
                            // push dummy values to params_and_returns
                            for _ in 0..(return_types.len() - args.len()) {
                                params_and_returns.push(ValRaw::i32(0));
                            }
                        }

                        results = vec![ValRaw::i32(0); func.ty(&store).results().len()];
                        if let Err(e) =
                            unsafe { func.call_unchecked(store, params_and_returns.as_mut_ptr()) }
                        {
                            RayLog::error(
                                format!(
                                    "Failed to execute function: {}, error: {:?}",
                                    func_name, e
                                )
                                .as_str(),
                            );
                            return Err(anyhow!(
                                "Failed to execute function: {}, error: {:?}",
                                func_name,
                                e
                            ));
                        }
                        // add results to args
                        for i in 0..return_types.len() {
                            results[i] = params_and_returns[i].clone();
                        }

                        let mut returns = Vec::new();
                        for i in 0..results.len() {
                            returns.push(from_wasmtime_raw_value(
                                &from_wasmtime_type(&return_types[i]),
                                &results[i].clone(),
                            ));
                        }
                        return Ok(returns);
                    }
                    None => {
                        RayLog::error(format!("Failed to get function: {}", func_name).as_str());
                        return Err(anyhow!("Failed to get function: {}", func_name));
                    }
                }
            }
            Err(_) => {
                // this is a normal function
                let func = instance
                    .instance
                    .get_export(&mut store, func_name)
                    .unwrap()
                    .into_func()
                    .unwrap();

                let mut params_and_returns: Vec<ValRaw> = Vec::new();
                // iterate all args and push them to params_and_returns
                for arg in args.iter() {
                    params_and_returns.push(arg.clone());
                }
                // get the number of return values
                let return_types = func
                    .ty(&store)
                    .results()
                    .map(|r| r.clone())
                    .collect::<Vec<ValType>>();
                if return_types.len() > args.len() {
                    // push dummy values to params_and_returns
                    for _ in 0..(return_types.len() - args.len()) {
                        params_and_returns.push(ValRaw::i32(0));
                    }
                }

                results = vec![ValRaw::i32(0); func.ty(&store).results().len()];
                if let Err(e) =
                    unsafe { func.call_unchecked(store, params_and_returns.as_mut_ptr()) }
                {
                    RayLog::error(
                        format!("Failed to execute function: {}, error: {:?}", func_name, e)
                            .as_str(),
                    );
                    return Err(anyhow!(
                        "Failed to execute function: {}, error: {:?}",
                        func_name,
                        e
                    ));
                }
                // add results to args
                for i in 0..return_types.len() {
                    results[i] = params_and_returns[i].clone();
                }

                let mut returns = Vec::new();
                for i in 0..results.len() {
                    returns.push(from_wasmtime_raw_value(
                        &from_wasmtime_type(&return_types[i]),
                        &results[i].clone(),
                    ));
                }
                return Ok(returns);
            }
        };
    }

    fn has_instance(&self, _sandbox_name: &str, _instance_name: &str) -> Result<bool> {
        unimplemented!()
    }

    fn has_module(&self, name: &str) -> Result<bool> {
        Ok(self.modules.contains_key(name))
    }

    fn has_sandbox(&self, name: &str) -> Result<bool> {
        Ok(self.sandboxes.contains_key(name))
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
                    move |caller, args| -> Result<()> {
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

    fn task_loop_once(
        &mut self,
        rt: &Arc<RwLock<Box<dyn RayRuntime + Send + Sync>>>,
    ) -> Result<()> {
        let mut result_buf: Vec<u8> = vec![];

        // receive task from channel with a timeout
        match TASK_RECEIVER.lock().ok().unwrap().as_ref() {
            Some(rx) => match rx.recv_timeout(Duration::from_millis(100)) {
                Ok(task) => {
                    RayLog::info(
                        format!("task_loop_once: executing wasm task: {:?}", task).as_str(),
                    );
                    let mod_name = task.module_name.as_str();
                    let func_name = task.func_name.as_str();
                    let args = task.args;

                    // check if module is already compiled
                    if !self.has_module(mod_name).unwrap() {
                        RayLog::info(
                            format!("task_loop_once: compiling wasm module: {:?}", mod_name)
                                .as_str(),
                        );
                        match rt.as_ref().read().unwrap().kv_get("", mod_name) {
                            Ok(obj) => match self.compile(mod_name, &obj.as_slice()) {
                                Ok(_) => {}
                                Err(e) => {
                                    RayLog::error(
                                        format!("task_loop_once: wasm compile error: {:?}", e)
                                            .as_str(),
                                    );
                                }
                            },
                            Err(e) => {
                                RayLog::error(
                                    format!("task_loop_once: wasm compile error: {:?}", e).as_str(),
                                );
                            }
                        }
                        match self.create_sandbox("sandbox") {
                            Ok(_) => {}
                            Err(e) => {
                                RayLog::error(
                                    format!("task_loop_once: wasm sandbox error: {:?}", e).as_str(),
                                );
                            }
                        }
                        match self.instantiate("sandbox", mod_name, "instance") {
                            Ok(_) => {}
                            Err(e) => {
                                RayLog::error(
                                    format!("task_loop_once: wasm instantiate error: {:?}", e)
                                        .as_str(),
                                );
                            }
                        }
                    }

                    match self.execute("sandbox", "instance", func_name, args) {
                        Ok(results) => {
                            RayLog::info(
                                format!("task_loop_once: wasm task result: {:?}", results).as_str(),
                            );

                            if results.len() != 1 {
                                RayLog::error("task_loop_once: invalid length of result");
                            } else {
                                match results[0].as_msgpack_vec() {
                                    Ok(v) => {
                                        result_buf = v;
                                    }
                                    Err(e) => {
                                        RayLog::error(
                                            format!("task_loop_once: wasm task error: {:?}", e)
                                                .as_str(),
                                        );
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            RayLog::error(
                                format!("task_loop_once: wasm task error: {:?}", e).as_str(),
                            );
                        }
                    }
                }
                Err(_) => {
                    return Ok(()); // timeout
                }
            },
            None => {
                RayLog::error("task_loop_once: channel not initialized");
                return Err(anyhow!("task_loop_once: channel not initialized"));
            }
        }

        // we got result here
        match TASK_RESULT_SENDER.lock().ok().unwrap().as_ref() {
            Some(tx) => {
                if result_buf.len() != 0 {
                    RayLog::info(
                        format!("task_loop_once: sending wasm task result: {:?}", result_buf)
                            .as_str(),
                    );
                    tx.send(Ok(result_buf)).unwrap();
                } else {
                    tx.send(Err(anyhow!("Invalid result"))).unwrap();
                }
            }
            None => {
                RayLog::error("task_loop_once: channel not initialized");
                return Err(anyhow!("task_loop_once: channel not initialized"));
            }
        }
        Ok(())
    }
}
