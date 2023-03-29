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
use crate::ray::common_proto::WorkerType;
use crate::ray::ClusterHelper;
use anyhow::Result;
use lazy_static::lazy_static;
use std::sync::Arc;
use std::sync::RwLock;
use tracing::info;

pub trait RayRuntime {
    fn do_init(&mut self) -> Result<()>;
    fn do_shutdown(&mut self) -> Result<()>;
    fn launch_task_loop(&mut self) -> Result<()>;
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
}

impl RayRuntimeClusterMode {
    pub fn new(internal_cfg: ConfigInternal) -> Self {
        Self {
            internal_cfg: internal_cfg,
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
        ClusterHelper::ray_stop();
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
}

// pub struct RayRuntime {
//     private: Option<RayRuntimeImpl>,
// }

// lazy_static! {
//     /// Runtime implementation instance.
//     static ref RAY_RUNTIME: Arc<RwLock<RayRuntime>> = {
//         let runtime = RayRuntime {
//             private: None,
//         };
//         Arc::new(RwLock::new(runtime))
//     };
// }

// impl RayRuntime {
//     /// Return a mutex protected instance of the runtime.
//     pub fn instance() -> Arc<RwLock<RayRuntime>> {
//         RAY_RUNTIME.clone()
//     }

//     pub async fn do_init(&mut self) -> Result<()> {
//         let cfg_w_lock = crate::ConfigInternal::instance();
//         let run_mode;
//         {
//             run_mode = cfg_w_lock.read().unwrap().run_mode;
//         }
//         match run_mode {
//             crate::config::RunMode::SingleProcess => {
//                 self.private = Some(RayRuntimeImpl::new(false));
//             }
//             crate::config::RunMode::Cluster => {
//                 {
//                     ClusterHelper::instance().write().unwrap().do_init();
//                 }
//                 {
//                     ClusterHelper::instance().write().unwrap().ray_start();
//                 }
//                 info!("native ray runtime started.");
//                 self.private = Some(RayRuntimeImpl::new(true));
//             }
//         };

//         {
//             // check worker type from config internal
//             match cfg_w_lock.read().unwrap().worker_type {
//                 WorkerType::Worker => {}
//                 _ => {
//                     let code_search_path;
//                     {
//                         code_search_path = cfg_w_lock.read().unwrap().code_search_path.clone();
//                     }
//                     info!("code_search_path: {:?}", code_search_path);
//                     self.load_binary_from_paths(code_search_path);
//                 }
//             };
//         }
//         Ok(())
//     }

//     pub async fn launch_task_loop(&self) -> Result<()> {
//         info!("launch_task_loop");
//         unsafe {
//             crate::ray::core::core_worker::CoreWorkerProcess_RunTaskExecutionLoop();
//         }
//         info!("launch_task_loop done");
//         Ok(())
//     }

//     pub async fn do_shutdown(&self) {
//         // check if we are running in cluster mode
//         if crate::ConfigInternal::instance()
//             .read()
//             .unwrap()
//             .run_mode
//             == crate::config::RunMode::Cluster
//         {
//             // TODO: terminate cluster mode
//         }
//     }

//     fn load_binary_from_paths(&self, paths: Vec<String>) {
//         info!("load_binary_from_paths: {:?}", paths);
//     }
// }

// struct RayRuntimeImpl {
//     is_native: bool,
// }

// impl RayRuntimeImpl {
//     pub fn new(is_native: bool) -> Self {
//         let mut rt = RayRuntimeImpl { is_native };
//         if is_native {
//             rt.init_cluster_mode();
//         } else {
//             rt.init_single_process_mode();
//         }
//         rt
//     }

//     fn init_single_process_mode(&self) {
//         info!("single process mode");
//     }

//     fn init_cluster_mode(&self) {
//         info!("cluster mode");
//         // TODO: initialize object store, task submitter and task executor, etc.
//     }
// }
