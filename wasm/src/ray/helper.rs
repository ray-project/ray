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

use lazy_static::lazy_static;
use prost::Message;
use std::ffi::CStr;
use std::process::Command;
use std::sync::Arc;
use std::sync::RwLock;
use tracing::{debug, error, info};

use crate::config::ConfigInternal;
use crate::ray::common_proto::{GcsNodeInfo, JobConfig, Language, RuntimeEnvInfo, WorkerType};
use crate::ray::core::core_worker::*;
use crate::ray::core::global_state_accessor::*;
use crate::ray::RuntimeEnv;
use crate::util::get_node_ip_address;

pub struct ClusterHelper {}

// lazy_static! {
//     /// Cluster helper implementation instance.
//     static ref CLUSTER_HELPER: Arc<RwLock<ClusterHelper>> = {
//         let helper = ClusterHelper {};
//         Arc::new(RwLock::new(helper))
//     };
// }

impl ClusterHelper {
    // /// Return a mutex protected instance of the cluster helper.
    // pub fn instance() -> Arc<RwLock<ClusterHelper>> {
    //     CLUSTER_HELPER.clone()
    // }

    pub fn do_init() {
        // TODO: nothing to do for now
    }

    pub fn ray_start(internal_cfg: &mut ConfigInternal) {
        let mut bootstrap_ip = internal_cfg.bootstrap_ip.clone();
        let bootstrap_port = internal_cfg.bootstrap_port;
        let worker_type = internal_cfg.worker_type;

        if worker_type == WorkerType::Driver && bootstrap_ip.is_empty() {
            bootstrap_ip = get_node_ip_address("");
            debug!("bootstrap ip: {}", bootstrap_ip);

            let redis_password = internal_cfg.redis_password.clone();
            let head_args = internal_cfg.head_args.clone();
            ClusterHelper::ray_node_start(
                &bootstrap_ip,
                bootstrap_port,
                &redis_password,
                &head_args,
            );
        }

        let bootstrap_address = format!("{}:{}", bootstrap_ip, bootstrap_port);
        let mut node_ip = internal_cfg.node_ip_address.clone();
        if node_ip.is_empty() {
            if !bootstrap_ip.is_empty() {
                node_ip = get_node_ip_address(&bootstrap_address);
            } else {
                node_ip = get_node_ip_address("");
            }
        }

        ClusterHelper::create_global_state_accessor(bootstrap_address.as_str());

        // get worker type
        let worker_type = internal_cfg.worker_type;
        if worker_type == WorkerType::Driver {
            let mut buffer: [u8; 1024] = [0; 1024];
            let mut buffer_size: u32 = buffer.len() as u32;
            unsafe {
                GlobalStateAccessor_GetNodeToConnectForDriver(
                    node_ip.as_ptr(),
                    buffer.as_mut_ptr(),
                    &mut buffer_size,
                );
            }
            // protobuf decode using prost
            let node_info = GcsNodeInfo::decode(&buffer[..buffer_size as usize]).unwrap();

            // set config internal variables
            info!("node info: {:?}", node_info);
            internal_cfg.raylet_socket_name = node_info.raylet_socket_name;
            internal_cfg.plasma_store_socket_name = node_info.object_store_socket_name;
            internal_cfg.node_manager_port = node_info.node_manager_port;
        }

        if worker_type == WorkerType::Driver {
            let mut buffer: [u8; 1024] = [0; 1024];
            let mut buffer_size: u32 = buffer.len() as u32;
            let ns = "session".to_string();
            let key = "session_dir".to_string();
            unsafe {
                // call GlobalStateAccessor_GetInternalKV
                GlobalStateAccessor_GetInternalKV(
                    ns.as_ptr(),
                    ns.len() as u32,
                    key.as_ptr(),
                    key.len() as u32,
                    buffer.as_mut_ptr(),
                    &mut buffer_size,
                );
            }
            // convert c string buffer to rust string
            let session_dir = unsafe {
                CStr::from_ptr(buffer.as_ptr() as *const i8)
                    .to_str()
                    .unwrap()
                    .to_string()
            };
            internal_cfg.update_session_dir(session_dir.as_str());
        }

        ClusterHelper::prepare_and_initialize_core_worker(
            internal_cfg,
            bootstrap_address.as_str(),
            node_ip.as_str(),
        );

        // TODO: rest of the startup code
    }

    pub fn ray_stop() {
        unimplemented!()
    }

    pub fn ray_node_start(
        node_ip_address: &str,
        port: i32,
        redis_password: &str,
        head_args: &Vec<String>,
    ) {
        // get the ray start command
        let mut cmd = Command::new("ray");
        let child = cmd
            .arg("start")
            .arg("--head")
            .arg("--port")
            .arg(port.to_string())
            .arg("--redis-password")
            .arg(redis_password)
            .arg("--node-ip-address")
            .arg(node_ip_address);
        if head_args.len() > 0 {
            for arg in head_args {
                child.arg(arg);
            }
        }
        // info! the whole command line
        info!("ray start command: {:?}", child);

        // wait for process to finish
        let output = child.output().expect("failed to execute process");
        if output.status.success() {
            info!("ray start command executed successfully");
        } else {
            error!("ray start command failed");
        }
        println!("stdout: \n{}", String::from_utf8_lossy(&output.stdout));
        println!("stderr: \n{}", String::from_utf8_lossy(&output.stderr));
    }

    pub fn prepare_and_initialize_core_worker(
        internal_cfg: &ConfigInternal,
        bootstrap_address: &str,
        node_ip: &str,
    ) {
        unsafe {
            CoreWorkerProcessOptions_UpdateGcsClientOptions(
                bootstrap_address.as_ptr(),
                bootstrap_address.len() as u32,
            );
        }
        // get worker type and set it to the config
        {
            let worker_type = internal_cfg.worker_type;
            unsafe {
                CoreWorkerProcessOptions_SetWorkerType(worker_type as WorkerType);
            }
        }
        // set language to wasm
        unsafe {
            CoreWorkerProcessOptions_SetLanguage(Language::Wasm);
        }
        // set store socket
        let store_socket = internal_cfg.plasma_store_socket_name.clone();
        unsafe {
            CoreWorkerProcessOptions_SetStoreSocket(
                store_socket.as_ptr(),
                store_socket.len() as u32,
            );
        }
        // set raylet socket
        let raylet_socket = internal_cfg.raylet_socket_name.clone();
        unsafe {
            CoreWorkerProcessOptions_SetRayletSocket(
                raylet_socket.as_ptr(),
                raylet_socket.len() as u32,
            );
        }
        let worker_type = internal_cfg.worker_type;
        if worker_type == WorkerType::Driver {
            // check if config job id is empty
            let job_id = internal_cfg.job_id.clone();
            if !job_id.is_empty() {
                // get job id string from config
                let job_id_hex;
                {
                    job_id_hex = internal_cfg.job_id.clone();
                }
                // set core worker process options job id
                unsafe {
                    CoreWorkerProcessOptions_SetJobID_Hex(
                        job_id_hex.as_ptr(),
                        job_id_hex.len() as u32,
                    );
                }
            } else {
                // get next job id and save in a buffer
                let mut buffer: [u8; 1024] = [0; 1024];
                let mut buffer_size: u32 = buffer.len() as u32;
                unsafe {
                    GlobalStateAccessor_GetNextJobID_Hex(buffer.as_mut_ptr(), &mut buffer_size);
                    CoreWorkerProcessOptions_SetJobID_Hex(buffer.as_ptr(), buffer_size as u32);
                }
            }
        }
        // set enable logging
        unsafe {
            CoreWorkerProcessOptions_SetEnableLogging(true);
        }
        // set log dir
        let log_dir = internal_cfg.logs_dir.clone();
        unsafe {
            CoreWorkerProcessOptions_SetLogDir(log_dir.as_ptr(), log_dir.len() as u32);
            CoreWorkerProcessOptions_SetInstallFailureSignalHandler(true);
            CoreWorkerProcessOptions_SetNodeIpAddress(node_ip.as_ptr(), node_ip.len() as u32);
        }
        // set node manager port
        let node_manager_port = internal_cfg.node_manager_port;
        unsafe {
            CoreWorkerProcessOptions_SetNodeManagerPort(node_manager_port);
            CoreWorkerProcessOptions_SetRayletIpAddress(node_ip.as_ptr(), node_ip.len() as u32);
            CoreWorkerProcessOptions_SetDriverName(
                "wasm_worker".as_ptr(),
                "wasm_worker".len() as u32,
            );
            CoreWorkerProcessOptions_SetMetricsAgentPort(-1);
        }
        // set callback
        unsafe {
            CoreWorkerProcessOptions_SetTaskExecutionCallback();
        }
        // set startup token
        let startup_token = internal_cfg.startup_token;
        unsafe {
            CoreWorkerProcessOptions_SetStartupToken(startup_token);
        }
        // set runtime env hash
        let runtime_env_hash = internal_cfg.runtime_env_hash.clone();
        unsafe {
            CoreWorkerProcessOptions_SetRuntimeEnvHash(runtime_env_hash);
        }

        let mut job_config = JobConfig::default();
        // set default actor life
        {
            let default_actor_lifetime = internal_cfg.default_actor_lifetime;
            job_config.default_actor_lifetime = default_actor_lifetime.into();
        }
        // add code search path
        let search_path = internal_cfg.code_search_path.clone();
        // iterate paths and add them to the job config
        for path in search_path {
            job_config.code_search_path.push(path);
        }
        // set job confignamespace
        {
            let namespace = internal_cfg.ray_namespace.clone();
            job_config.ray_namespace = namespace;
        }
        // if runtime_env is not empty, set the job config runtime env
        {
            let runtime_env = internal_cfg.runtime_env.clone();
            match runtime_env {
                Some(env) => {
                    let mut runtime_env_info = RuntimeEnvInfo::default();
                    runtime_env_info.serialized_runtime_env = RuntimeEnv::serialize_to_json(&env);
                    job_config.runtime_env_info = Some(runtime_env_info);
                }
                None => {}
            }
        }
        // if job_config_metadata is not empty, replace duplicate keys with the new values
        {
            let job_config_metadata = internal_cfg.job_config_metadata.clone();
            match job_config_metadata {
                Some(metadata) => {
                    // iterate the hashmap and replace old values in the job config
                    for (key, value) in metadata {
                        job_config.metadata.insert(key, value);
                    }
                }
                None => {}
            }
        }
        // serialize job config to a vector of bytes
        let job_config_vec = job_config.encode_to_vec();
        // convert the vector to a buffer
        let job_config_buffer = job_config_vec.as_slice();
        unsafe {
            CoreWorkerProcessOptions_SetSerializedJobConfig(
                job_config_buffer.as_ptr(),
                job_config_buffer.len() as u32,
            );
        }

        let res = unsafe { crate::ray::core::core_worker::CoreWorkerProcess_Initialize() };
        if res == 0 {
            info!("core worker process initialized successfully");
        } else {
            error!("core worker process initialization failed");
        }
    }

    pub fn ray_node_stop() {
        // get the ray stop command
        let mut cmd = Command::new("ray");
        let child = cmd.arg("stop");

        // info! print the command line
        info!("ray stop command: {:?}", child);

        let output = child.output().expect("failed to execute process");
        if output.status.success() {
            info!("ray stop command executed successfully");
        } else {
            error!("ray stop command failed");
        }
    }

    pub fn create_global_state_accessor(gcs_address: &str) {
        info!(
            "initializing global state accessor using gcs address: {}",
            gcs_address
        );
        unsafe {
            GcsClientOptions_Update(gcs_address.as_ptr(), gcs_address.len() as u32);
            GlobalStateAccessor_Init();
        };
    }
}
