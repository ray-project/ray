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
use super::{from_wasmtime_type, WasmFunc, WasmType, WasmValue, WasmtimeStoreData};

use crate::runtime::{CallOptions, RayRuntime};
use crate::runtime::{InvocationSpec, ObjectID, RemoteFunctionHolder};

use crate::engine::WasmContext;
use crate::runtime::common_proto::TaskType;
use anyhow::{anyhow, Result};
use core::result::Result::Ok;

use sha256::digest;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use wasmtime::AsContextMut;
use wasmtime::Caller;

use tracing::{error, info};

pub struct WasmtimeContext<'a> {
    pub caller: &'a mut Caller<'a, WasmtimeStoreData>,
    pub runtime: &'a Arc<RwLock<Box<dyn RayRuntime + Send + Sync>>>,
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
        let mut k = self.caller.data_mut().submitted_modules.keys();
        if k.len() == 0 {
            return Err(anyhow!("No module submitted to object store yet"));
        }
        if k.len() > 1 {
            return Err(anyhow!(
                "More than one module submitted to object store, it is not supported yet"
            ));
        }
        let kstr = k.next().unwrap().clone();
        let sb = self.caller.as_context_mut().data().sandbox_name.clone();
        let mod_hash = self
            .caller
            .as_context_mut()
            .data()
            .submitted_modules
            .get(kstr.as_str())
            .unwrap()
            .clone();
        Ok(WasmFunc::new(
            sb,
            mod_hash,
            format!("{}", func_idx),
            params,
            results,
        ))
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
        let object = match self.runtime.read().unwrap().get(object_id) {
            Ok(object) => object,
            Err(e) => {
                error!("Failed to get object: {}", e);
                return Err(anyhow!("Failed to get object: {}", e));
            }
        };
        info!("get_object: {:x?} {:x?}", object_id, object);
        Ok(object)
    }

    fn put_object(&mut self, data: &[u8]) -> Result<ObjectID> {
        self.runtime.write().unwrap().put(data.to_vec())
    }

    fn submit_sandbox_binary(&mut self) -> Result<()> {
        let mut new_obj_hash = HashMap::new();
        self.caller.data().module_bytes.keys().for_each(|k| {
            match self.caller.data().module_bytes.get(k) {
                Some(v) => {
                    // hash the bytes
                    let hash = digest(v.as_slice());

                    match self
                        .runtime
                        .write()
                        .unwrap()
                        .kv_put("", hash.as_str(), v.as_slice())
                    {
                        Ok(_) => {
                            info!("submit_sandbox_binary: {:x?} {:x?}", k, hash);
                            new_obj_hash.insert(k.clone(), hash);
                        }
                        Err(e) => {
                            error!("Failed to submit sandbox binary: {}", e);
                        }
                    };
                }
                None => {}
            };
        });
        new_obj_hash.iter().for_each(|(k, v)| {
            self.caller
                .data_mut()
                .submitted_modules
                .insert(k.clone(), v.clone());
        });
        Ok(())
    }
}
