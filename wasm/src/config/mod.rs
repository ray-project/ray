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
use crate::runtime;
use crate::util;
use serde_json::Value;
use tracing::debug;
use tracing::info;
use uuid::Uuid;

use std::collections::HashMap;

use runtime::common_proto::job_config::ActorLifetime;
use runtime::common_proto::WorkerType;

type StartupToken = i64;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RunMode {
    SingleProcess,
    Cluster,
}

#[derive(Clone, Debug)]
pub struct ConfigInternal {
    pub worker_type: WorkerType,
    pub run_mode: RunMode,
    pub bootstrap_ip: String,
    pub bootstrap_port: i32,
    pub redis_password: String,
    pub node_manager_port: i32,
    pub code_search_path: Vec<String>,
    pub plasma_store_socket_name: String,
    pub raylet_socket_name: String,
    pub session_dir: String,
    pub job_id: String,
    pub logs_dir: String,
    pub node_ip_address: String,
    pub startup_token: StartupToken,
    pub head_args: Vec<String>,
    pub runtime_env: Option<runtime::RuntimeEnv>,
    pub runtime_env_hash: i32,
    pub default_actor_lifetime: ActorLifetime,
    pub job_config_metadata: Option<HashMap<String, String>>,
    pub ray_namespace: String,
}

impl ConfigInternal {
    pub fn new() -> Self {
        ConfigInternal {
            worker_type: WorkerType::Driver,
            run_mode: RunMode::SingleProcess,
            bootstrap_ip: String::new(),
            bootstrap_port: 6379,
            redis_password: String::from("5241590000000000"),
            node_manager_port: 0,
            code_search_path: vec![],
            plasma_store_socket_name: String::new(),
            raylet_socket_name: String::new(),
            session_dir: String::new(),
            job_id: String::new(),
            logs_dir: String::new(),
            node_ip_address: String::new(),
            startup_token: 0,
            head_args: vec![],
            runtime_env: None,
            runtime_env_hash: 0,
            default_actor_lifetime: ActorLifetime::NonDetached,
            job_config_metadata: None,
            ray_namespace: String::new(),
        }
    }

    /// process RayConfig and set the variables
    fn process_config(&mut self, config: &runtime::RayConfig) {
        if !config.address.is_empty() {
            self.set_bootstrap_address(&config.address);
        }
        self.run_mode = if config.local_mode {
            RunMode::SingleProcess
        } else {
            RunMode::Cluster
        };
        if !config.code_search_path.is_empty() {
            self.code_search_path = config.code_search_path.clone();
        }
        match config.redis_password {
            Some(ref password) => self.redis_password = password.clone(),
            None => {}
        }
        if !config.head_args.is_empty() {
            self.head_args = config.head_args.clone();
        }
        if config.default_actor_lifetime == runtime::ActorLifetime::Detached {
            self.default_actor_lifetime = ActorLifetime::Detached;
        }
        match config.runtime_env {
            Some(ref runtime_env) => self.runtime_env = Some(runtime_env.clone()),
            None => {}
        }
        self.worker_type = if config.is_worker {
            WorkerType::Worker
        } else {
            WorkerType::Driver
        };
    }

    /// process command line arguments and set the variables
    fn process_arguments(&mut self, args: &util::WorkerParameters) {
        // compare command line arguments and set the variables
        match args.ray_code_search_path {
            Some(ref path) => {
                // create vector of sting from colon separated string
                debug!("The code search path is: {}", path);
                self.code_search_path = path.split(':').map(|s| s.to_string()).collect();
            }
            None => {}
        }
        match args.ray_address {
            Some(ref address) => self.set_bootstrap_address(address),
            None => {}
        }
        match args.ray_redis_password {
            Some(ref password) => self.redis_password = password.clone(),
            None => {}
        }
        match args.ray_job_id {
            Some(ref job_id) => self.job_id = job_id.clone(),
            None => {}
        }
        match args.ray_node_manager_port {
            Some(port) => self.node_manager_port = port,
            None => {}
        }
        match args.ray_raylet_socket_name {
            Some(ref name) => self.raylet_socket_name = name.clone(),
            None => {}
        }
        match args.ray_plasma_store_socket_name {
            Some(ref name) => self.plasma_store_socket_name = name.clone(),
            None => {}
        }
        match args.ray_session_dir {
            Some(ref dir) => self.update_session_dir(dir),
            None => {}
        }
        match args.ray_logs_dir {
            Some(ref dir) => self.logs_dir = dir.clone(),
            None => {}
        }
        match args.ray_node_ip_address {
            Some(ref address) => self.node_ip_address = address.clone(),
            None => {}
        }
        match args.ray_head_args {
            Some(ref args) => {
                // create vector of sting from space separated string
                self.head_args = args.split(' ').map(|s| s.to_string()).collect();
            }
            None => {}
        }
        match args.startup_token {
            Some(token) => self.startup_token = token,
            None => {}
        }
        match &args.ray_default_actor_lifetime {
            Some(lifetime) => {
                // check lifetime string ignoring case
                if lifetime.to_lowercase() == "detached" {
                    self.default_actor_lifetime = ActorLifetime::Detached;
                } else if lifetime.to_lowercase() == "non-detached" {
                    self.default_actor_lifetime = ActorLifetime::NonDetached;
                } else {
                    panic!("Invalid actor lifetime: {}", lifetime);
                }
            }
            None => {}
        }
        match args.ray_runtime_env {
            Some(ref data) => {
                // deserialize json
                let runtime_env = runtime::RuntimeEnv::deserialize_from_json_string(data);
                self.runtime_env = Some(runtime_env);
            }
            None => {}
        }
    }

    pub fn init(&mut self, config: &runtime::RayConfig, args: &util::WorkerParameters) {
        // check input config and set the variables
        self.process_config(config);

        // check command line arguments and set the variables
        self.process_arguments(args);

        if self.worker_type == WorkerType::Driver && self.run_mode == RunMode::Cluster {
            if self.bootstrap_ip.is_empty() {
                // get ray address from environment variable
                let ray_address_env = std::env::var("RAY_ADDRESS");
                if ray_address_env.is_ok() {
                    debug!("Initialize Ray cluster address to \"{}\" from environment variable \"RAY_ADDRESS\"",
                        ray_address_env.clone().unwrap()
                    );
                    self.set_bootstrap_address(&ray_address_env.unwrap());
                }
            }
            // if code search path is empty, we get it from program location
            if self.code_search_path.is_empty() {
                let program_location = std::env::current_exe();
                if program_location.is_ok() {
                    let program_path = program_location.unwrap();
                    let program_dir = program_path.parent();
                    if program_dir.is_some() {
                        debug!("No code search path found yet. The program location path {} will be added for searching dynamic libraries by default. And you can add some search paths by '--ray_code_search_path'",
                            program_dir.unwrap().to_str().unwrap()
                        );
                        self.code_search_path
                            .push(program_dir.unwrap().to_str().unwrap().to_string());
                    }
                }
            } else {
                // convert all code search path to absolute path
                let mut new_code_search_path = Vec::new();
                for path in &self.code_search_path {
                    let absolute_path = std::fs::canonicalize(path);
                    if absolute_path.is_ok() {
                        new_code_search_path
                            .push(absolute_path.unwrap().to_str().unwrap().to_string());
                    }
                }
                self.code_search_path = new_code_search_path;
            }
        }

        // further update namespace
        if self.worker_type == WorkerType::Driver {
            self.ray_namespace = config.ray_namespace.clone();
            if args.ray_job_namespace.is_some() {
                self.ray_namespace = args.ray_job_namespace.clone().unwrap();
            }
            if self.ray_namespace.is_empty() {
                // generate a UUID v4 as the namespace
                self.ray_namespace = Uuid::new_v4().to_string();
            }
        }

        // check if env var RAY_JOB_CONFIG_JSON_ENV_VAR is set
        let job_config_json_str = std::env::var("RAY_JOB_CONFIG_JSON_ENV_VAR");
        if job_config_json_str.is_ok() {
            let job_config_json_str = job_config_json_str.unwrap();
            let json_value: Value = serde_json::from_str(job_config_json_str.as_str()).unwrap();

            let rt_env_obj = json_value.get("runtime_env").unwrap();
            self.runtime_env = Some(runtime::RuntimeEnv::deserialize_from_json_value(
                rt_env_obj.clone(),
            ));

            // deserialize "metadata" to job_config_metadata HashMap<String, String>
            let metadata_obj = json_value.get("metadata").unwrap().clone();
            let metadata: HashMap<String, String> = metadata_obj
                .as_object()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.to_string(), v.as_str().unwrap().to_string()))
                .collect();
            self.job_config_metadata = Some(metadata);
            // make sure json_value has only two keys
            assert_eq!(json_value.as_object().unwrap().len(), 2);
        }
    }

    fn set_bootstrap_address(&mut self, address: &str) {
        self.bootstrap_ip = String::from(address);
    }

    pub fn update_session_dir(&mut self, dir: &str) {
        if self.session_dir.is_empty() {
            self.session_dir = String::from(dir);
        }
        if self.logs_dir.is_empty() {
            self.logs_dir = format!("{}/logs", self.session_dir);
        }
        info!("Ray session directory is set to {}", self.session_dir);
        info!("Ray logs directory is set to {}", self.logs_dir);
    }
}
