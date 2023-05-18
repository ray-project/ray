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
#![feature(never_type)]
use super::{Hostcall, WasmContext, WasmFunc, WasmType};
use crate::engine::{
    Hostcalls, WasmEngine, WasmInstance, WasmModule, WasmSandbox, WasmValue, TASK_RECEIVER,
    TASK_RESULT_RECEIVER, TASK_RESULT_SENDER,
};
use crate::runtime::{common_proto::TaskType, CallOptions, InvocationSpec, RayRuntime};
use crate::util::RayLog;
use std::time::Duration;

use anyhow::{anyhow, Result};

use lazy_static::lazy_static;

use sha256::digest;

use std::collections::HashMap;
use std::fmt::format;
use std::result::Result::Ok;
use std::sync::{Arc, Mutex, RwLock};

use tracing::{error, info};

use wasmedge_macro::sys_host_function;
use wasmedge_sdk as sdk;
use wasmedge_sys::{
    AsImport, CallingFrame, Config, Executor, FuncType, Function, ImportModule, ImportObject,
    Instance, Loader, Module, Store, Validator, WasiModule, WasmValue as WasmEdgeWasmValue,
};
use wasmedge_types::{error::HostFuncError, ValType};

mod data;
use data::*;

use crate::runtime as rt;
// macro_rules! to convert wasmedge call to our call
macro_rules! hostcall_wrapper {
    ($func_name:ident) => {
        #[sys_host_function]
        fn $func_name(
            frame: CallingFrame,
            input: Vec<WasmEdgeWasmValue>,
        ) -> Result<Vec<WasmEdgeWasmValue>, HostFuncError> {
            // convert input from wasmedge type to our type
            let mut args = vec![];
            for arg in input.iter() {
                args.push(from_wasmedge_value(arg));
            }
            // TODO: move this initialization out of the hostcall
            let mut ctx = WasmEdgeContext { frame };
            match rt::$func_name(&mut ctx, &args) {
                Ok(rets) => {
                    let mut results = vec![];
                    for ret in rets.iter() {
                        results.push(to_wasmedge_value(ret));
                    }
                    return Ok(results);
                }
                Err(e) => {
                    return Err(HostFuncError::Runtime(1));
                }
            }
        }
    };
}

type WasmEdgeHostCallType =
    fn(CallingFrame, Vec<WasmEdgeWasmValue>) -> Result<Vec<WasmEdgeWasmValue>, HostFuncError>;

hostcall_wrapper!(hc_ray_test);
hostcall_wrapper!(hc_ray_sleep);
hostcall_wrapper!(hc_ray_init);
hostcall_wrapper!(hc_ray_shutdown);
hostcall_wrapper!(hc_ray_get);
hostcall_wrapper!(hc_ray_put);
hostcall_wrapper!(hc_ray_call);

struct CurrentTaskInfo {
    sandbox_name: String,
    module_hash: String,
    runtime: Option<Arc<RwLock<Box<dyn RayRuntime + Send + Sync>>>>,
    bytes: Vec<u8>,
}

lazy_static! {
    static ref HOSTCALLS: HashMap<&'static str, WasmEdgeHostCallType> = {
        let mut m: HashMap<&str, WasmEdgeHostCallType> = HashMap::new();
        m.insert("test", hc_ray_test);
        m.insert("sleep", hc_ray_sleep);
        m.insert("init", hc_ray_init);
        m.insert("shutdown", hc_ray_shutdown);
        m.insert("get", hc_ray_get);
        m.insert("put", hc_ray_put);
        m.insert("call", hc_ray_call);
        m
    };

    // TODO: fix this temporary hack
    static ref CURRENT_TASK: Arc<RwLock<CurrentTaskInfo>> = Arc::new(RwLock::new(CurrentTaskInfo {
        sandbox_name: "".to_string(),
        module_hash: "".to_string(),
        runtime: None,
        bytes: vec![],
    }));

    static ref SUBMITTED_MODULES: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));
}

pub struct WasmEdgeEngine {
    cfg: Config,
    sandboxes: HashMap<String, WasmEdgeSandbox>,
    modules: HashMap<String, WasmEdgeModule>,
    imports: HashMap<String, ImportObject>,
    hostcalls_map: HashMap<String, Hostcalls>,
}

impl WasmEdgeEngine {
    pub fn new() -> Self {
        let cfg = match Config::create() {
            Ok(mut cfg) => {
                cfg.wasi(true);
                cfg
            }
            Err(e) => {
                error!("Failed to create config: {:?}", e);
                panic!();
            }
        };

        match WasiModule::create(None, None, None) {
            Ok(mut wasi) => {
                wasi.init_wasi(None, None, None);
                let mut engine = WasmEdgeEngine {
                    cfg,
                    sandboxes: HashMap::new(),
                    modules: HashMap::new(),
                    imports: HashMap::from([(wasi.name().to_string(), ImportObject::Wasi(wasi))]),
                    hostcalls_map: HashMap::new(),
                };
                engine
            }
            Err(e) => {
                error!("Failed to create wasi: {:?}", e);
                panic!();
            }
        }
    }
}

impl WasmEngine for WasmEdgeEngine {
    fn init(&self) -> Result<()> {
        Ok(())
    }

    fn compile(&mut self, name: &str, wasm_bytes: &[u8]) -> Result<Box<&dyn WasmModule>> {
        let module = WasmEdgeModule::new(&self.cfg, name, wasm_bytes);
        self.modules.insert(name.to_string(), module);
        Ok(Box::new(&self.modules[name]))
    }

    fn create_sandbox(&mut self, name: &str) -> Result<Box<&dyn WasmSandbox>> {
        let sandbox = WasmEdgeSandbox::new(self.cfg.clone(), name);
        self.sandboxes.insert(name.to_string(), sandbox);
        Ok(Box::new(&self.sandboxes[name]))
    }

    fn register_hostcalls(&mut self, hostcalls: &Hostcalls) -> Result<()> {
        // add to hostcalls list
        self.hostcalls_map
            .insert(hostcalls.module_name.clone(), hostcalls.clone());

        Ok(())
    }

    fn instantiate(
        &mut self,
        sandbox_name: &str,
        module_name: &str,
        instance_name: &str,
    ) -> Result<Box<&dyn WasmInstance>> {
        let mut sandbox = self.sandboxes[sandbox_name].clone();

        // iterate hostcalls_map and register all hostcalls
        for (name, hostcalls) in self.hostcalls_map.iter() {
            if self.imports.contains_key(name) {
                continue;
            }
            info!("creating import module {} from hostcalls", name);

            let hc_module_name = hostcalls.module_name.clone();
            let mut import_module = ImportModule::create(hc_module_name.as_str())?;

            // This is a hack to set the current runtime
            let mut tmp = Arc::clone(&hostcalls.runtime);
            let mut current_task = CURRENT_TASK.write().unwrap();
            current_task.runtime = Some(tmp);

            for hostcall in hostcalls.functions.iter() {
                let params = &hostcall.params;
                let results = &hostcall.results;
                let func_ty = FuncType::create(
                    params
                        .iter()
                        .map(|x| wasmtype_to_wasmedgetype(x))
                        .collect::<Vec<ValType>>(),
                    results
                        .iter()
                        .map(|x| wasmtype_to_wasmedgetype(x))
                        .collect::<Vec<ValType>>(),
                )?;

                // check if hostcall is supported
                if !HOSTCALLS.contains_key(hostcall.name.as_str()) {
                    return Err(anyhow!("Hostcall not supported"));
                }
                let supported_call = HOSTCALLS[hostcall.name.as_str()];

                let func = unsafe {
                    Function::create_with_data(
                        &func_ty,
                        Box::new(supported_call),
                        std::ptr::null_mut(), // TODO: move this part to instance init so that we have access to sandbox?
                        0,
                    )
                }?;
                import_module.add_func(hostcall.name.as_str(), func);
            }
            self.imports.insert(
                hostcalls.module_name.clone(),
                ImportObject::Import(import_module),
            );
        }

        // register all import objects
        for (import_module_name, import_obj) in self.imports.iter() {
            let import_obj = import_obj.clone();
            match sandbox
                .executor
                .register_import_object(&mut sandbox.store, &import_obj)
            {
                Ok(_) => {
                    info!("Registered import module: {}", import_module_name);
                }
                Err(e) => {
                    return Err(anyhow!("Failed to register import module: {:?}", e));
                }
            }
        }

        let module = &self.modules[module_name];
        match CURRENT_TASK.write() {
            Ok(mut current_task) => {
                current_task.bytes = module.bytes.clone();
                current_task.sandbox_name = sandbox_name.to_string();
            }
            Err(e) => {
                error!("Failed to write current task: {:?}", e);
                panic!();
            }
        }

        // create instance
        let module = &self.modules[module_name].module;
        match sandbox
            .executor
            .register_active_module(&mut sandbox.store, module)
        {
            Ok(instance) => {
                let wi = WasmEdgeInstance::new(instance);
                self.sandboxes
                    .get_mut(sandbox_name)
                    .unwrap()
                    .instances
                    .insert(instance_name.to_string(), wi);
                info!("Registered instance: {}", instance_name);
                Ok(Box::new(
                    &self.sandboxes[sandbox_name].instances[instance_name],
                ))
            }
            Err(e) => Err(anyhow!("Failed to register module: {:?}", e)),
        }
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

        let args = args
            .iter()
            .map(|arg| to_wasmedge_value(arg))
            .collect::<Vec<WasmEdgeWasmValue>>();

        let instance = sandbox.instances.get(instance_name).unwrap();
        match func_name.parse::<u32>() {
            Ok(idx) => match instance.instance.get_table("__indirect_function_table") {
                Ok(table) => match table.get_data(idx) {
                    Ok(val) => match val.func_ref() {
                        Some(func) => match sandbox.executor.call_func_ref(&func, args) {
                            Ok(rtn) => {
                                let mut results = rtn
                                    .iter()
                                    .map(|x| from_wasmedge_value(x))
                                    .collect::<Vec<WasmValue>>();
                                return Ok(results);
                            }
                            Err(e) => {
                                info!("Failed to run function: {:?}", e);
                                return Err(anyhow!("Failed to run function"));
                            }
                        },
                        None => {
                            return Err(anyhow!("Failed to get function"));
                        }
                    },
                    Err(_) => {
                        return Err(anyhow!("Failed to get function"));
                    }
                },
                Err(_) => {
                    return Err(anyhow!("Failed to get table"));
                }
            },
            Err(_) => {
                let func = instance
                    .instance
                    .get_func(func_name)
                    .expect("Failed to get function");
                match sandbox.executor.call_func(&func, args) {
                    Ok(rtn) => match rtn.len() {
                        0 => {
                            info!("Completed executing function \"{}\"", func_name);
                            return Ok(vec![]);
                        }
                        1 => {
                            let val = rtn[0].to_i32();
                            if val != 0 {
                                return Err(anyhow!("Failed to run function"));
                            }
                            info!("Completed executing function \"{}\"", func_name);
                            return Ok(vec![WasmValue::I32(val)]);
                        }
                        _ => {
                            info!("Invalid return value. {}", rtn.len());
                            return Err(anyhow!("Invalid return value"));
                        }
                    },
                    Err(e) => {
                        info!("Failed to run function: {:?}", e);
                        return Err(anyhow!("Failed to run function"));
                    }
                }
            }
        }
    }

    fn has_instance(&self, sandbox_name: &str, instance_name: &str) -> Result<bool> {
        return Ok(self
            .sandboxes
            .get(sandbox_name)
            .unwrap()
            .instances
            .contains_key(instance_name));
    }

    fn has_module(&self, name: &str) -> Result<bool> {
        return Ok(self.modules.contains_key(name));
    }

    fn has_sandbox(&self, name: &str) -> Result<bool> {
        return Ok(self.sandboxes.contains_key(name));
    }

    fn list_modules(&self) -> Result<Vec<Box<&dyn WasmModule>>> {
        return Ok(self
            .modules
            .values()
            .map(|m| Box::new(m as &dyn WasmModule) as Box<&dyn WasmModule>)
            .collect());
    }

    fn list_sandboxes(&self) -> Result<Vec<Box<&dyn WasmSandbox>>> {
        return Ok(self
            .sandboxes
            .values()
            .map(|s| Box::new(s as &dyn WasmSandbox) as Box<&dyn WasmSandbox>)
            .collect());
    }

    fn list_instances(&self, sandbox_name: &str) -> Result<Vec<Box<&dyn WasmInstance>>> {
        return Ok(self.sandboxes[sandbox_name]
            .instances
            .values()
            .map(|i| Box::new(i as &dyn WasmInstance) as Box<&dyn WasmInstance>)
            .collect());
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

                    RayLog::info("before function execution");
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
                    RayLog::error("timeout");
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

#[derive(Clone)]
struct WasmEdgeModule {
    name: String,
    module: Module,
    bytes: Vec<u8>,
}

impl WasmEdgeModule {
    pub fn new(cfg: &Config, module_name: &str, wasm_bytes: &[u8]) -> Self {
        let loader = Loader::create(Some(&cfg)).unwrap();
        let module = loader.from_bytes(wasm_bytes).unwrap();
        match Validator::create(Some(&cfg)) {
            Ok(mut validator) => {
                validator.validate(&module).unwrap();
            }
            Err(e) => {
                error!("Failed to create validator: {:?}", e);
                panic!();
            }
        }
        WasmEdgeModule {
            name: module_name.to_string(),
            module,
            bytes: wasm_bytes.to_vec(),
        }
    }
}

impl WasmModule for WasmEdgeModule {}

#[derive(Clone)]
struct WasmEdgeSandbox {
    name: String,
    store: Store,
    executor: Executor,
    instances: HashMap<String, WasmEdgeInstance>,
}

impl WasmEdgeSandbox {
    pub fn new(cfg: Config, name: &str) -> Self {
        match Store::create() {
            Ok(mut store) => WasmEdgeSandbox {
                name: name.to_string(),
                store,
                executor: Executor::create(Some(&cfg), None).unwrap(),
                instances: HashMap::new(),
            },
            Err(e) => {
                error!("Failed to create store: {:?}", e);
                panic!();
            }
        }
    }
}

impl WasmSandbox for WasmEdgeSandbox {
    // TODO: implement WasmSandbox
}

#[derive(Clone)]
struct WasmEdgeInstance {
    instance: Instance,
}

impl WasmEdgeInstance {
    pub fn new(instance: Instance) -> Self {
        WasmEdgeInstance { instance }
    }
}

impl WasmInstance for WasmEdgeInstance {
    // TODO: implement WasmInstance
}

struct WasmEdgeCallData {}

struct WasmEdgeContext {
    frame: CallingFrame,
}

impl WasmEdgeContext {}

impl WasmContext for WasmEdgeContext {
    fn get_memory_region(&mut self, off: usize, len: usize) -> Result<&[u8]> {
        match self
            .frame
            .memory_mut(0)
            .unwrap()
            .data_pointer(off as u32, len as u32)
        {
            Ok(data) => Ok(unsafe { std::slice::from_raw_parts(data, len) }),
            Err(e) => Err(anyhow!("Failed to get memory region")),
        }
    }

    fn get_memory_region_mut(&mut self, off: usize, len: usize) -> Result<&mut [u8]> {
        match self
            .frame
            .memory_mut(0)
            .unwrap()
            .data_pointer_mut(off as u32, len as u32)
        {
            Ok(data) => Ok(unsafe { std::slice::from_raw_parts_mut(data, len) }),
            Err(e) => Err(anyhow!("Failed to get memory region")),
        }
    }

    fn get_func_ref(&mut self, func_idx: u32) -> Result<super::WasmFunc> {
        let module = &self.frame.module_instance().unwrap();
        let tbl = module.get_table("__indirect_function_table").unwrap();
        let func = tbl.get_data(func_idx).unwrap().func_ref().unwrap();
        let func_ty = func.ty().unwrap();
        let params = func_ty
            .params_type_iter()
            .map(|x| wasmedgetype_to_wasmtype(&x))
            .collect();
        let results = func_ty
            .returns_type_iter()
            .map(|x| wasmedgetype_to_wasmtype(&x))
            .collect();

        let sb = CURRENT_TASK.read().unwrap().sandbox_name.clone();
        let hash = match SUBMITTED_MODULES.lock().unwrap().get(&sb) {
            Some(bytes) => bytes.clone(),
            None => return Err(anyhow!("Module is not submitted")),
        };

        Ok(WasmFunc {
            sandbox: sb,
            module: hash,
            name: format!("{}", func_idx),
            params,
            results,
        })
    }

    fn invoke(
        &mut self,
        remote_func_holder: &crate::runtime::RemoteFunctionHolder,
        args: &[WasmValue],
    ) -> Result<Vec<crate::runtime::ObjectID>> {
        let call_opt = CallOptions::new();
        let invoke_spec =
            InvocationSpec::new(TaskType::NormalTask, remote_func_holder.clone(), args, None);
        let mut rt = &CURRENT_TASK.write().unwrap().runtime;
        match rt {
            Some(rt) => {
                let mut rt = rt.write().unwrap();
                match rt.call(&invoke_spec, &call_opt) {
                    Ok(results) => {
                        if results.len() != 1 {
                            return Err(anyhow!("Invalid result length"));
                        }
                        Ok(results)
                    }
                    Err(e) => Err(anyhow!("Failed to call")),
                }
            }
            None => Err(anyhow!("No runtime")),
        }
    }

    fn get_object(&mut self, object_id: &crate::runtime::ObjectID) -> Result<Vec<u8>> {
        let mut rt = &CURRENT_TASK.write().unwrap().runtime;
        match rt.as_ref() {
            Some(rt) => {
                let mut rt = rt.write().unwrap();
                match rt.get(object_id) {
                    Ok(object) => {
                        info!("get_object: {:x?} {:x?}", object_id, object);
                        Ok(object)
                    }
                    Err(e) => Err(anyhow!("Failed to get object")),
                }
            }
            None => Err(anyhow!("No runtime")),
        }
    }

    fn put_object(&mut self, data: &[u8]) -> Result<crate::runtime::ObjectID> {
        let mut rt = &CURRENT_TASK.write().unwrap().runtime;
        match rt.as_ref() {
            Some(rt) => {
                let mut rt = rt.write().unwrap();
                match rt.put(data.to_vec()) {
                    Ok(object_id) => Ok(object_id),
                    Err(e) => Err(anyhow!("Failed to put object")),
                }
            }
            None => Err(anyhow!("No runtime")),
        }
    }

    fn submit_sandbox_binary(&mut self) -> Result<()> {
        let bytes = match &CURRENT_TASK.read() {
            Ok(current_task) => current_task.bytes.clone(),
            Err(e) => return Err(anyhow!("Failed to read current task")),
        };
        if bytes.len() == 0 {
            return Err(anyhow!("No module bytes"));
        }

        let sandbox_name = match &CURRENT_TASK.read() {
            Ok(current_task) => current_task.sandbox_name.clone(),
            Err(e) => return Err(anyhow!("Failed to read current task")),
        };
        let hash = digest(bytes.as_slice());
        let mut rt = &CURRENT_TASK.write().unwrap().runtime;
        match rt.as_ref() {
            Some(rt) => {
                let mut rt = rt.write().unwrap();
                match rt.kv_put("", hash.as_str(), bytes.as_slice()) {
                    Ok(_) => {
                        SUBMITTED_MODULES
                            .lock()
                            .unwrap()
                            .insert(sandbox_name.clone(), hash);
                        Ok(())
                    }
                    Err(e) => Err(anyhow!("Failed to put module bytes")),
                }
            }
            None => Err(anyhow!("No runtime")),
        }
    }
}
