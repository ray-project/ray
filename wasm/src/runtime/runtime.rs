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
use crate::engine::WasmEngine;
use crate::runtime::common_proto::WorkerType;
use crate::runtime::ClusterHelper;
use crate::runtime::InvocationSpec;
use crate::runtime::ObjectStore;
use crate::runtime::TaskExecutor;
use crate::util::get_node_ip_address;
use crate::util::RayLog;
use rmp::encode::write_i32;
use std::sync::Arc;
use std::task::Context;
use tokio::task;
use tracing_subscriber::fmt::format;

use anyhow::{anyhow, Result};
use libc::c_void;
use tracing::{debug, error, info};

use crate::engine::Hostcalls;

use super::core::global_state_accessor::GlobalStateAccessor;
use super::CallOptions;
use super::ObjectID;
use super::ObjectStoreFactory;
use super::ObjectStoreType;
use super::RemoteFunctionHolder;
use super::TaskArg;
use super::TaskSubmitter;
use super::TaskSubmitterFactory;
use super::WasmTaskExecutionInfo;
use lazy_static::lazy_static;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Mutex;
use std::thread::sleep;
use std::time::Duration;

// a channel for sending task to task executor
pub type TaskSender = Sender<WasmTaskExecutionInfo>;
pub type TaskReceiver = Receiver<WasmTaskExecutionInfo>;

// a channel for receiving task result from task executor
pub type TaskResultSender = Sender<Vec<u8>>;
pub type TaskResultReceiver = Receiver<Vec<u8>>;

// static channel variable for sending task to task executor
lazy_static! {
    pub static ref TASK_SENDER: Mutex<Option<TaskSender>> = Mutex::new(None);
    pub static ref TASK_RECEIVER: Mutex<Option<TaskReceiver>> = Mutex::new(None);
    pub static ref TASK_RESULT_SENDER: Mutex<Option<TaskResultSender>> = Mutex::new(None);
    pub static ref TASK_RESULT_RECEIVER: Mutex<Option<TaskResultReceiver>> = Mutex::new(None);
}

pub trait RayRuntime {
    fn do_init(&mut self) -> Result<()>;
    fn do_shutdown(&mut self) -> Result<()>;
    fn launch_task_loop(&mut self) -> Result<()>;

    // object get/put related
    fn put_with_id(&self, data: Vec<u8>, obj_id: ObjectID) -> Result<()>;
    fn put(&self, data: Vec<u8>) -> Result<ObjectID>;
    fn get(&self, obj_id: &ObjectID) -> Result<Vec<u8>>;
    fn gets(&self, obj_ids: Vec<ObjectID>) -> Result<Vec<Vec<u8>>>;

    // task submit related
    fn wait(&self, obj_ids: Vec<ObjectID>, num_obj: i32, timeout: i32) -> Result<Vec<Vec<u8>>>;
    fn call(
        &self,
        invoke_spec: &InvocationSpec,
        task_options: &CallOptions,
    ) -> Result<Vec<ObjectID>>;
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

pub struct RayRuntimeClusterMode {
    internal_cfg: ConfigInternal,
    object_store: Box<dyn ObjectStore + Send + Sync>,
    task_executor: TaskExecutor,
    task_submitter: Box<dyn TaskSubmitter + Send + Sync>,
    global_state_accessor: GlobalStateAccessor,
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
        }
    }

    pub fn load_binary_from_paths(&self, paths: Vec<String>) {
        info!("load_binary_from_paths: {:?}", paths);
    }
}

impl RayRuntime for RayRuntimeClusterMode {
    fn do_init(&mut self) -> Result<()> {
        ClusterHelper::do_init()?;
        ClusterHelper::ray_start(&mut self.internal_cfg)?;
        debug!("native ray runtime started.");

        // check worker type from config internal
        match self.internal_cfg.worker_type {
            WorkerType::Worker => {}
            _ => {
                let code_search_path = self.internal_cfg.code_search_path.clone();
                info!("code_search_path: {:?}", code_search_path);
                self.load_binary_from_paths(code_search_path);
            }
        };

        // init channels
        let (tx, rx): (TaskSender, TaskReceiver) = channel();
        TASK_SENDER.lock().unwrap().replace(tx);
        TASK_RECEIVER.lock().unwrap().replace(rx);

        let (tx, rx): (TaskResultSender, TaskResultReceiver) = channel();
        TASK_RESULT_SENDER.lock().unwrap().replace(tx);
        TASK_RESULT_RECEIVER.lock().unwrap().replace(rx);

        Ok(())
    }

    fn do_shutdown(&mut self) -> Result<()> {
        ClusterHelper::ray_stop(&self.internal_cfg)?;
        Ok(())
    }

    fn launch_task_loop(&mut self) -> Result<()> {
        debug!("launch_task_loop");
        let handle = task::spawn(async move {
            unsafe {
                crate::runtime::core::core_worker::CoreWorkerProcess_RunTaskExecutionLoop();
            }
        });

        // loop until handle finished
        loop {
            if handle.is_finished() {
                RayLog::info("launch_task_loop: CoreWorkerProcess task execution loop finished");
                break;
            }

            let mut buf: Vec<u8> = vec![];
            // receive task from channel with a timeout
            let receiver = TASK_RECEIVER.lock().ok().unwrap();
            match receiver.as_ref() {
                Some(rx) => match rx.recv_timeout(Duration::from_millis(100)) {
                    Ok(task) => {
                        RayLog::info(
                            format!("launch_task_loop: executing wasm task: {:?}", task).as_str(),
                        );

                        // TODO: run task
                        write_i32(&mut buf, 0x12345678).unwrap();
                    }
                    Err(e) => {
                        continue;
                    }
                },
                None => {
                    RayLog::error("launch_task_loop: channel not initialized");
                    break;
                }
            }

            // we got result here
            let sender = TASK_RESULT_SENDER.lock().ok().unwrap();
            match sender.as_ref() {
                Some(tx) => {
                    tx.send(buf).unwrap();
                }
                None => {
                    RayLog::error("launch_task_loop: channel not initialized");
                    break;
                }
            }
        }
        debug!("launch_task_loop done");
        Ok(())
    }

    fn put_with_id(&self, data: Vec<u8>, obj_id: ObjectID) -> Result<()> {
        unimplemented!()
    }

    fn put(&self, data: Vec<u8>) -> Result<ObjectID> {
        unimplemented!()
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

    fn gets(&self, obj_ids: Vec<ObjectID>) -> Result<Vec<Vec<u8>>> {
        unimplemented!()
    }

    fn wait(&self, obj_ids: Vec<ObjectID>, num_obj: i32, timeout: i32) -> Result<Vec<Vec<u8>>> {
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
}

pub struct RayRuntimeSingleProcessMode {
    internal_cfg: ConfigInternal,
}

impl RayRuntimeSingleProcessMode {
    pub fn new(internal_cfg: ConfigInternal) -> Self {
        Self { internal_cfg }
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
        info!("launch_task_loop");
        unsafe {
            crate::runtime::core::core_worker::CoreWorkerProcess_RunTaskExecutionLoop();
        }
        info!("launch_task_loop done");
        Ok(())
    }

    fn put_with_id(&self, data: Vec<u8>, obj_id: ObjectID) -> Result<()> {
        unimplemented!()
    }

    fn put(&self, data: Vec<u8>) -> Result<ObjectID> {
        unimplemented!()
    }

    fn get(&self, obj_id: &ObjectID) -> Result<Vec<u8>> {
        unimplemented!()
    }

    fn gets(&self, obj_ids: Vec<ObjectID>) -> Result<Vec<Vec<u8>>> {
        unimplemented!()
    }

    fn wait(&self, obj_ids: Vec<ObjectID>, num_obj: i32, timeout: i32) -> Result<Vec<Vec<u8>>> {
        unimplemented!()
    }

    fn call(
        &self,
        invoke_spec: &InvocationSpec,
        task_options: &CallOptions,
    ) -> Result<Vec<ObjectID>> {
        todo!()
    }
}
