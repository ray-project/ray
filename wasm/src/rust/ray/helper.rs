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
use std::process::Command;
use std::sync::Arc;
use std::sync::Mutex;
use tracing::{debug, error, info};

use crate::config::ConfigInternal;
use crate::ray::common_proto::WorkerType;
use crate::util::get_node_ip_address;

pub struct ClusterHelper {}

lazy_static! {
    /// Cluster helper implementation instance.
    static ref CLUSTER_HELPER: Arc<Mutex<ClusterHelper>> = {
        let helper = ClusterHelper {};
        Arc::new(Mutex::new(helper))
    };
}

impl ClusterHelper {
    /// Return a mutex protected instance of the cluster helper.
    pub fn instance() -> Arc<Mutex<ClusterHelper>> {
        CLUSTER_HELPER.clone()
    }

    pub fn do_init(&self) {
        // TODO: nothing to do for now
    }

    pub fn ray_start(&self) {
        let mut bootstrap_ip = ConfigInternal::instance()
            .lock()
            .unwrap()
            .bootstrap_ip
            .clone();
        let bootstrap_port = ConfigInternal::instance().lock().unwrap().bootstrap_port;
        let worker_type = ConfigInternal::instance().lock().unwrap().worker_type;

        if worker_type == WorkerType::Driver && bootstrap_ip.is_empty() {
            bootstrap_ip = get_node_ip_address("");
            debug!("bootstrap ip: {}", bootstrap_ip);

            let redis_password = ConfigInternal::instance()
                .lock()
                .unwrap()
                .redis_password
                .clone();
            let head_args = ConfigInternal::instance().lock().unwrap().head_args.clone();
            self.ray_node_start(&bootstrap_ip, bootstrap_port, &redis_password, &head_args);
        }

        let bootstrap_address = format!("{}:{}", bootstrap_ip, bootstrap_port);
        let mut node_ip = ConfigInternal::instance()
            .lock()
            .unwrap()
            .node_ip_address
            .clone();
        if node_ip.is_empty() {
            if !bootstrap_ip.is_empty() {
                node_ip = get_node_ip_address(&bootstrap_address);
            } else {
                node_ip = get_node_ip_address("");
            }
        }

        let gsa = self.create_global_state_accessor(bootstrap_address.as_str());
        // TODO: rest of the startup code
    }

    pub fn ray_stop(&self) {
        unimplemented!()
    }

    pub fn ray_node_start(
        &self,
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
    }

    pub fn ray_node_stop(&self) {
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

    pub fn create_global_state_accessor(&self, gcs_address: &str) {
        unimplemented!()
    }
}
