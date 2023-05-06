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
        Ok(WasmFunc::new(
            self.caller.as_context_mut().data().sandbox_name.as_str(),
            "",
            format!("{}", func_idx).as_str(),
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
}
