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
use crate::runtime::common_proto::WorkerType;
use crate::runtime::ClusterHelper;
use crate::runtime::InvocationSpec;
use crate::runtime::ObjectStore;
use crate::runtime::TaskExecutor;
use crate::util::get_node_ip_address;
use crate::util::RayLog;
use tokio::task;

use anyhow::{anyhow, Result};
use tracing::{debug, error, info};

use super::core::gcs_client::GcsClient;
use super::core::global_state_accessor::GlobalStateAccessor;
use super::CallOptions;
use super::ObjectID;
use super::ObjectStoreFactory;
use super::ObjectStoreType;
use super::TaskSubmitter;
use super::TaskSubmitterFactory;

pub trait RayRuntime {
    fn do_init(&mut self) -> Result<()>;
    fn do_shutdown(&mut self) -> Result<()>;
    fn spawn_task_loop(&mut self) -> Result<()>;
    fn is_running(&self) -> bool;

    // object get/put related
    fn put_with_id(&mut self, data: Vec<u8>, obj_id: ObjectID) -> Result<()>;
    fn put(&mut self, data: Vec<u8>) -> Result<ObjectID>;
    fn get(&self, obj_id: &ObjectID) -> Result<Vec<u8>>;
    fn gets(&self, obj_ids: Vec<ObjectID>) -> Result<Vec<Vec<u8>>>;

    // execution type related
    fn exec_type(&self) -> WorkerType;

    // task submit related
    fn wait(&self, obj_ids: Vec<ObjectID>, num_obj: i32, timeout: i32) -> Result<Vec<Vec<u8>>>;
    fn call(
        &self,
        invoke_spec: &InvocationSpec,
        task_options: &CallOptions,
    ) -> Result<Vec<ObjectID>>;

    // internal kv related operations
    fn kv_put(&self, ns: &str, key: &str, value: &[u8]) -> Result<()>;
    fn kv_get(&self, ns: &str, key: &str) -> Result<Vec<u8>>;
}

pub struct RayRuntimeFactory {}

impl RayRuntimeFactory {
    pub fn create_runtime(
        internal_cfg: ConfigInternal,
    ) -> Result<Box<dyn RayRuntime + Send + Sync>> {
        let run_mode = internal_cfg.run_mode;
        match run_mode {
            RunMode::Cluster => Ok(Box::new(RayRuntimeClusterMode::new(internal_cfg))),
            RunMode::SingleProcess => Ok(Box::new(RayRuntimeSingleProcessMode::new(internal_cfg))),
        }
    }
}

#[allow(dead_code)]
pub struct RayRuntimeClusterMode {
    internal_cfg: ConfigInternal,
    object_store: Box<dyn ObjectStore + Send + Sync>,
    task_executor: TaskExecutor,
    task_submitter: Box<dyn TaskSubmitter + Send + Sync>,
    global_state_accessor: GlobalStateAccessor,
    gcs_client: GcsClient,

    // core worker task loop handle for progress checking
    task_loop_handle: Option<task::JoinHandle<()>>,
}

impl RayRuntimeClusterMode {
    pub fn new(internal_cfg: ConfigInternal) -> Self {
        let mut bootstrap_address = internal_cfg.bootstrap_ip.clone();
        if bootstrap_address.is_empty() {
            bootstrap_address = get_node_ip_address("");
        }
        bootstrap_address = format!("{}:{}", bootstrap_address, internal_cfg.bootstrap_port);
        Self {
            internal_cfg,
            object_store: ObjectStoreFactory::create_object_store(ObjectStoreType::Native),
            task_executor: TaskExecutor::new(),
            task_submitter: TaskSubmitterFactory::create_task_submitter(
                super::TaskSubmitterType::Native,
            ),
            global_state_accessor: GlobalStateAccessor::new(bootstrap_address.as_str()),
            gcs_client: GcsClient::new(bootstrap_address.as_str()),
            task_loop_handle: None,
        }
    }
}

impl RayRuntime for RayRuntimeClusterMode {
    fn do_init(&mut self) -> Result<()> {
        ClusterHelper::do_init()?;
        ClusterHelper::ray_start(&mut self.internal_cfg)?;
        debug!("native ray runtime started.");

        // check worker type from config internal
        match self.internal_cfg.worker_type {
            WorkerType::Worker => {
                let code_search_path = self.internal_cfg.code_search_path.clone();
                info!("code_search_path: {:?}", code_search_path);
                RayLog::info(format!("code_search_path: {:?}", code_search_path).as_str());
                ClusterHelper::search_wasm(code_search_path);
            }
            _ => {}
        };

        Ok(())
    }

    fn do_shutdown(&mut self) -> Result<()> {
        ClusterHelper::ray_stop(&self.internal_cfg)?;
        Ok(())
    }

    fn spawn_task_loop(&mut self) -> Result<()> {
        let handle = task::spawn(async move {
            unsafe {
                crate::runtime::core::core_worker::CoreWorkerProcess_RunTaskExecutionLoop();
            }
        });

        self.task_loop_handle = Some(handle);
        Ok(())
    }

    /// check if task loop is running
    fn is_running(&self) -> bool {
        if let Some(handle) = &self.task_loop_handle {
            return !handle.is_finished();
        }
        false
    }

    fn put_with_id(&mut self, data: Vec<u8>, obj_id: ObjectID) -> Result<()> {
        match self
            .object_store
            .put_with_object_id(data.as_slice(), &obj_id)
        {
            Ok(_) => Ok(()),
            Err(e) => {
                error!("runtime putting object failed: {:?}", e);
                Err(anyhow!("runtime putting object failed: {:?}", e))
            }
        }
    }

    fn put(&mut self, data: Vec<u8>) -> Result<ObjectID> {
        match self.object_store.put(data.as_slice()) {
            Ok(obj_id) => Ok(obj_id),
            Err(e) => {
                error!("runtime putting object failed: {:?}", e);
                Err(anyhow!("runtime putting object failed: {:?}", e))
            }
        }
    }

    fn get(&self, obj_id: &ObjectID) -> Result<Vec<u8>> {
        match self.object_store.get(obj_id, -1) {
            Ok(data) => Ok(data),
            Err(e) => {
                error!("runtime getting object failed: {:?}", e);
                Err(anyhow!("runtime getting object failed: {:?}", e))
            }
        }
    }

    fn gets(&self, _obj_ids: Vec<ObjectID>) -> Result<Vec<Vec<u8>>> {
        unimplemented!()
    }

    fn wait(&self, _obj_ids: Vec<ObjectID>, _num_obj: i32, _timeout: i32) -> Result<Vec<Vec<u8>>> {
        unimplemented!()
    }

    fn call(
        &self,
        invoke_spec: &InvocationSpec,
        task_options: &CallOptions,
    ) -> Result<Vec<ObjectID>> {
        let obj_id = self.task_submitter.submit_task(invoke_spec, task_options);
        Ok(vec![obj_id])
    }

    fn exec_type(&self) -> WorkerType {
        self.internal_cfg.worker_type
    }

    fn kv_put(&self, ns: &str, key: &str, value: &[u8]) -> Result<()> {
        if self.gcs_client.internal_kv.put(ns, key, value) {
            Ok(())
        } else {
            Err(anyhow!("put kv failed"))
        }
    }

    fn kv_get(&self, ns: &str, key: &str) -> Result<Vec<u8>> {
        self.gcs_client.internal_kv.get(ns, key)
    }
}

pub struct RayRuntimeSingleProcessMode {
    internal_cfg: ConfigInternal,
}

impl RayRuntimeSingleProcessMode {
    pub fn new(internal_cfg: ConfigInternal) -> Self {
        Self { internal_cfg }
    }
}

impl RayRuntime for RayRuntimeSingleProcessMode {
    fn do_init(&mut self) -> Result<()> {
        // check worker type from config internal
        match self.internal_cfg.worker_type {
            WorkerType::Worker => {
                let code_search_path = self.internal_cfg.code_search_path.clone();
                info!("code_search_path: {:?}", code_search_path);
                ClusterHelper::search_wasm(code_search_path);
            }
            _ => {}
        };
        Ok(())
    }

    fn do_shutdown(&mut self) -> Result<()> {
        // do nothing
        Ok(())
    }

    fn spawn_task_loop(&mut self) -> Result<()> {
        info!("spawn_task_loop");
        unsafe {
            crate::runtime::core::core_worker::CoreWorkerProcess_RunTaskExecutionLoop();
        }
        info!("spawn_task_loop done");
        Ok(())
    }

    fn is_running(&self) -> bool {
        unimplemented!()
    }

    fn put_with_id(&mut self, _data: Vec<u8>, _obj_id: ObjectID) -> Result<()> {
        unimplemented!()
    }

    fn put(&mut self, _data: Vec<u8>) -> Result<ObjectID> {
        unimplemented!()
    }

    fn get(&self, _obj_id: &ObjectID) -> Result<Vec<u8>> {
        unimplemented!()
    }

    fn gets(&self, _obj_ids: Vec<ObjectID>) -> Result<Vec<Vec<u8>>> {
        unimplemented!()
    }

    fn wait(&self, _obj_ids: Vec<ObjectID>, _num_obj: i32, _timeout: i32) -> Result<Vec<Vec<u8>>> {
        unimplemented!()
    }

    fn call(
        &self,
        _invoke_spec: &InvocationSpec,
        _task_options: &CallOptions,
    ) -> Result<Vec<ObjectID>> {
        todo!()
    }

    fn exec_type(&self) -> WorkerType {
        self.internal_cfg.worker_type
    }

    fn kv_put(&self, _ns: &str, _key: &str, _value: &[u8]) -> Result<()> {
        unimplemented!()
    }

    fn kv_get(&self, _ns: &str, _key: &str) -> Result<Vec<u8>> {
        unimplemented!()
    }
}
