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
use crate::engine::{WasmEngine, WasmType, WasmValue};
use anyhow::Result;

#[derive(Clone)]
pub struct Hostcalls {
    pub module_name: String,
    pub functions: Vec<Hostcall>,
}

#[derive(Clone)]
pub struct Hostcall {
    pub name: String,
    pub params: Vec<WasmType>,
    pub results: Vec<WasmType>,
    pub func: fn(&[WasmValue]) -> Result<Vec<WasmValue>>,
}

impl Hostcalls {
    pub fn new(module_name: &str) -> Self {
        Hostcalls {
            module_name: module_name.to_string(),
            functions: Vec::new(),
        }
    }

    pub fn register_function(
        &mut self,
        name: &str,
        params: Vec<WasmType>,
        results: Vec<WasmType>,
        func: fn(&[WasmValue]) -> Result<Vec<WasmValue>>,
    ) -> Result<()> {
        if self.functions.iter().any(|f| f.name == name) {
            return Err(anyhow::anyhow!("Hostcall {} already exists", name));
        }
        self.functions.push(Hostcall {
            name: name.to_string(),
            params,
            results,
            func,
        });
        Ok(())
    }

    pub fn unregister_function(&mut self, name: &str) -> Result<()> {
        if self.functions.iter().any(|f| f.name == name) {
            return Err(anyhow::anyhow!("Hostcall {} does not exist", name));
        }
        self.functions.retain(|f| f.name != name);
        Ok(())
    }
}
