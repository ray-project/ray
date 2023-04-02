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

use crate::runtime::common_proto::{Language, WorkerType};

// declare extern c core worker functions
extern "C" {
    pub fn CoreWorkerProcess_Initialize() -> i32;
    pub fn CoreWorkerProcess_Shutdown();
    pub fn CoreWorkerProcess_RunTaskExecutionLoop();

    // core worker options
    pub fn CoreWorkerProcessOptions_SetWorkerType(worker_type: WorkerType);
    pub fn CoreWorkerProcessOptions_SetLanguage(language: Language);
    pub fn CoreWorkerProcessOptions_SetStoreSocket(store_socket: *const u8, len: u32);
    pub fn CoreWorkerProcessOptions_SetRayletSocket(raylet_socket: *const u8, len: u32);
    pub fn CoreWorkerProcessOptions_SetJobID_Int(job_id: u32);
    pub fn CoreWorkerProcessOptions_SetJobID_Hex(job_id: *const u8, len: u32);
    pub fn CoreWorkerProcessOptions_SetJobID_Binary(job_id: *const u8, len: u32);
    // TODO: set gcs_options
    pub fn CoreWorkerProcessOptions_SetEnableLogging(enable_logging: bool);
    // log dir
    pub fn CoreWorkerProcessOptions_SetLogDir(log_dir: *const u8, len: u32);
    pub fn CoreWorkerProcessOptions_SetInstallFailureSignalHandler(
        install_failure_signal_handler: bool,
    );
    pub fn CoreWorkerProcessOptions_SetNodeIpAddress(node_ip_address: *const u8, len: u32);
    pub fn CoreWorkerProcessOptions_SetNodeManagerPort(node_manager_port: i32);
    pub fn CoreWorkerProcessOptions_SetRayletIpAddress(raylet_ip_address: *const u8, len: u32);
    pub fn CoreWorkerProcessOptions_SetDriverName(driver_name: *const u8, len: u32);
    pub fn CoreWorkerProcessOptions_SetMetricsAgentPort(metrics_agent_port: i32);
    pub fn CoreWorkerProcessOptions_SetStartupToken(startup_token: i64);
    pub fn CoreWorkerProcessOptions_SetRuntimeEnvHash(runtime_env_hash: i32);
    pub fn CoreWorkerProcessOptions_SetSerializedJobConfig(buf: *const u8, length: u32);
    pub fn CoreWorkerProcessOptions_SetTaskExecutionCallback();

    // TODO: more stuff
    pub fn CoreWorkerProcessOptions_UpdateGcsClientOptions(gcs_address: *const u8, len: u32);
}

// sorry, if we link rust with c++, the linker will complain the atexit symbol cannot
// be found in core_worker_process.cc. We could use static-nobound, but it is only
// restricted to nightly rust. If we do this, we will see errors while compiling other
// crates. So we just declare the atexit symbol here.
#[no_mangle]
pub extern "C" fn atexit(f: fn()) -> i32 {
    0
}
