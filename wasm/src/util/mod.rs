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
use clap::{Parser, ValueEnum};
use std::net::UdpSocket;

pub fn get_node_ip_address(address: &str) -> String {
    // convert str to String
    let mut ip_address = address.to_string();
    if ip_address.is_empty() {
        ip_address = "8.8.8.8:53".to_string();
    }
    // use udp resolver to get local ip address
    let socket = UdpSocket::bind("0.0.0.0:0").expect("Failed to bind socket");
    socket
        .connect(ip_address)
        .expect("Failed to connect to DNS server");
    let local_addr = socket.local_addr().expect("Failed to get local address");
    local_addr.ip().to_string()
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct WorkerParameters {
    /// The address of the Ray cluster to connect to.
    #[arg(long, verbatim_doc_comment)]
    pub ray_address: Option<String>,

    /// Prevents external clients without the password from connecting to Redis
    /// if provided.
    #[arg(long, verbatim_doc_comment)]
    pub ray_redis_password: Option<String>,

    /// A list of directories or files of dynamic libraries that specify the
    /// search path for user code. Only searching the top level under a directory.
    /// ':' is used as the separator.
    #[arg(long, verbatim_doc_comment)]
    pub ray_code_search_path: Option<String>,

    /// Assigned job id
    #[arg(long, verbatim_doc_comment)]
    pub ray_job_id: Option<String>,

    /// The port to use for the node manager
    #[arg(long, verbatim_doc_comment)]
    pub ray_node_manager_port: Option<i32>,

    /// It will specify the socket name used by the raylet if provided.
    #[arg(long, verbatim_doc_comment)]
    pub ray_raylet_socket_name: Option<String>,

    /// It will specify the socket name used by the plasma store if provided.
    #[arg(long, verbatim_doc_comment)]
    pub ray_plasma_store_socket_name: Option<String>,

    /// The path of this session.
    #[arg(long, verbatim_doc_comment)]
    pub ray_session_dir: Option<String>,

    /// Logs dir for workers.
    #[arg(long, verbatim_doc_comment)]
    pub ray_logs_dir: Option<String>,

    /// The ip address for this node.
    #[arg(long, verbatim_doc_comment)]
    pub ray_node_ip_address: Option<String>,

    /// The command line args to be appended as parameters of the `ray start`
    /// command. It takes effect only if Ray head is started by a driver. Run `ray
    /// start --help` for details.
    #[arg(long, verbatim_doc_comment)]
    pub ray_head_args: Option<String>,

    /// The startup token assigned to this worker process by the raylet.
    #[arg(long, verbatim_doc_comment)]
    pub startup_token: Option<i64>,

    /// The default actor lifetime type, `detached` or `non_detached`.
    #[arg(long, verbatim_doc_comment)]
    pub ray_default_actor_lifetime: Option<String>,

    /// The serialized runtime env.
    #[arg(long, verbatim_doc_comment)]
    pub ray_runtime_env: Option<String>,

    /// The computed hash of the runtime env for this worker.
    #[arg(long, verbatim_doc_comment)]
    pub ray_runtime_env_hash: Option<i32>,

    /// The namespace of job. If not set,
    /// a unique value will be randomly generated.
    #[arg(long, verbatim_doc_comment)]
    pub ray_job_namespace: Option<String>,
}

impl WorkerParameters {
    pub fn new_empty() -> Self {
        Self {
            ray_address: None,
            ray_redis_password: None,
            ray_code_search_path: None,
            ray_job_id: None,
            ray_node_manager_port: None,
            ray_raylet_socket_name: None,
            ray_plasma_store_socket_name: None,
            ray_session_dir: None,
            ray_logs_dir: None,
            ray_node_ip_address: None,
            ray_head_args: None,
            startup_token: None,
            ray_default_actor_lifetime: None,
            ray_runtime_env: None,
            ray_runtime_env_hash: None,
            ray_job_namespace: None,
        }
    }
}

#[derive(ValueEnum, Debug, Clone)]
pub enum WasmEngineType {
    Wasmedge,
    Wasmtime,
    WAVM,
    WAMR,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct LauncherParameters {
    /// the path to the wasm file
    #[arg(short = 'f', long, verbatim_doc_comment)]
    pub wasm_file: String,

    /// type of the wasm engine to use
    #[arg(short = 'e', long, value_enum, verbatim_doc_comment, default_value_t = WasmEngineType::Wasmedge)]
    pub engine_type: WasmEngineType,

    /// the entry point function name
    /// if not set, the default value is `_start`
    #[arg(short = 's', long, verbatim_doc_comment, default_value = "_start")]
    pub entry_point: String,
}
