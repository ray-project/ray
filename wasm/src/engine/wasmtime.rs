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
use anyhow::Result;
use crate::ray::{Hostcalls, Hostcall};

pub struct WasmtimeEngine {}

impl WasmtimeEngine {
    pub fn new() -> Self {
        WasmtimeEngine {}
    }
}

impl WasmEngine for WasmtimeEngine {
    fn compile(&self, wasm_bytes: &[u8]) -> Result<Box<dyn WasmModule>> {
        Ok(Box::new(WasmtimeModule::new()))
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
        Ok(vec![Box::new(WasmtimeModule::new())])
    }

    fn list_sandboxes(&self) -> Result<Vec<Box<dyn WasmSandbox>>> {
        Ok(vec![Box::new(WasmtimeSandbox::new())])
    }

    fn list_instances(&self, sandbox: Box<dyn WasmSandbox>) -> Result<Vec<Box<dyn WasmInstance>>> {
        Ok(vec![Box::new(WasmtimeInstance::new())])
    }

    fn register_hostcalls(&self, hostcalls: &mut Hostcalls) -> Result<()> {
        Ok(())
    }
}

struct WasmtimeModule {}

impl WasmModule for WasmtimeModule {
    fn new() -> Self {
        WasmtimeModule {}
    }
}

struct WasmtimeSandbox {}

impl WasmSandbox for WasmtimeSandbox {
    fn new() -> Self {
        WasmtimeSandbox {}
    }
}

struct WasmtimeInstance {}

impl WasmInstance for WasmtimeInstance {
    fn new() -> Self {
        WasmtimeInstance {}
    }
}

struct WasmtimeValue {}

impl WasmValue for WasmtimeValue {
    fn new() -> Self {
        WasmtimeValue {}
    }
}
