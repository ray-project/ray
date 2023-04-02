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

use crate::config::ConfigInternal;
use crate::config::RunMode;
use crate::engine::{WasmEngine, WasmType, WasmValue};
use crate::ray::common_proto::WorkerType;
use crate::ray::ClusterHelper;
use anyhow::Result;
use tracing::info;

use crate::ray::{new_ray_hostcalls, Hostcalls};

pub trait RayRuntime {
    fn do_init(&mut self) -> Result<()>;
    fn do_shutdown(&mut self) -> Result<()>;
    fn launch_task_loop(&mut self) -> Result<()>;
    fn setup_hostcalls(&mut self, engine: &mut Box<dyn WasmEngine>) -> Result<()>;
}

pub struct RayRuntimeFactory {}

impl RayRuntimeFactory {
    pub fn create_runtime(internal_cfg: ConfigInternal) -> Result<Box<dyn RayRuntime>> {
        let run_mode = internal_cfg.run_mode;
        match run_mode {
            RunMode::Cluster => Ok(Box::new(RayRuntimeClusterMode::new(internal_cfg))),
            RunMode::SingleProcess => Ok(Box::new(RayRuntimeSingleProcessMode::new(internal_cfg))),
        }
    }
}

pub struct RayRuntimeClusterMode {
    internal_cfg: ConfigInternal,
    hostcalls: Hostcalls,
}

impl RayRuntimeClusterMode {
    pub fn new(internal_cfg: ConfigInternal) -> Self {
        Self {
            internal_cfg: internal_cfg,
            hostcalls: new_ray_hostcalls(),
        }
    }

    pub fn load_binary_from_paths(&self, paths: Vec<String>) {
        info!("load_binary_from_paths: {:?}", paths);
    }
}

impl RayRuntime for RayRuntimeClusterMode {
    fn do_init(&mut self) -> Result<()> {
        ClusterHelper::do_init();
        ClusterHelper::ray_start(&mut self.internal_cfg);
        info!("native ray runtime started.");

        // check worker type from config internal
        match self.internal_cfg.worker_type {
            WorkerType::Worker => {}
            _ => {
                let code_search_path = self.internal_cfg.code_search_path.clone();
                info!("code_search_path: {:?}", code_search_path);
                self.load_binary_from_paths(code_search_path);
            }
        };
        Ok(())
    }

    fn do_shutdown(&mut self) -> Result<()> {
        ClusterHelper::ray_stop(&self.internal_cfg);
        Ok(())
    }

    fn launch_task_loop(&mut self) -> Result<()> {
        info!("launch_task_loop");
        unsafe {
            crate::ray::core::core_worker::CoreWorkerProcess_RunTaskExecutionLoop();
        }
        info!("launch_task_loop done");
        Ok(())
    }

    fn setup_hostcalls(&mut self, engine: &mut Box<dyn WasmEngine>) -> Result<()> {
        engine.register_hostcalls(&mut self.hostcalls)
    }
}

pub struct RayRuntimeSingleProcessMode {
    internal_cfg: ConfigInternal,
}

impl RayRuntimeSingleProcessMode {
    pub fn new(internal_cfg: ConfigInternal) -> Self {
        Self {
            internal_cfg: internal_cfg,
        }
    }

    pub fn load_binary_from_paths(&self, paths: Vec<String>) {
        info!("load_binary_from_paths: {:?}", paths);
    }
}

impl RayRuntime for RayRuntimeSingleProcessMode {
    fn do_init(&mut self) -> Result<()> {
        // check worker type from config internal
        match self.internal_cfg.worker_type {
            WorkerType::Worker => {}
            _ => {
                let code_search_path = self.internal_cfg.code_search_path.clone();
                info!("code_search_path: {:?}", code_search_path);
                self.load_binary_from_paths(code_search_path);
            }
        };
        Ok(())
    }

    fn do_shutdown(&mut self) -> Result<()> {
        // do nothing
        Ok(())
    }

    fn launch_task_loop(&mut self) -> Result<()> {
        Err(anyhow::anyhow!("not implemented"))
    }

    fn setup_hostcalls(&mut self, engine: &mut Box<dyn WasmEngine>) -> Result<()> {
        Err(anyhow::anyhow!("not implemented"))
    }
}
