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
use anyhow::{anyhow, Result};
use std::os::raw::c_void;

// declare extern c core worker functions
extern "C" {
    pub fn CoreWorkerProcess_Initialize() -> i32;
    pub fn CoreWorkerProcess_Shutdown();
    pub fn CoreWorkerProcess_RunTaskExecutionLoop();

    // core worker options
    pub fn CoreWorkerProcessOptions_SetWorkerType(worker_type: WorkerType);
    pub fn CoreWorkerProcessOptions_SetLanguage(language: Language);
    pub fn CoreWorkerProcessOptions_SetStoreSocket(store_socket: *const u8, len: usize);
    pub fn CoreWorkerProcessOptions_SetRayletSocket(raylet_socket: *const u8, len: usize);
    pub fn CoreWorkerProcessOptions_SetJobID_Int(job_id: u32);
    pub fn CoreWorkerProcessOptions_SetJobID_Hex(job_id: *const u8, len: usize);
    pub fn CoreWorkerProcessOptions_SetJobID_Binary(job_id: *const u8, len: usize);
    // TODO: set gcs_options
    pub fn CoreWorkerProcessOptions_SetEnableLogging(enable_logging: bool);
    // log dir
    pub fn CoreWorkerProcessOptions_SetLogDir(log_dir: *const u8, len: usize);
    pub fn CoreWorkerProcessOptions_SetInstallFailureSignalHandler(
        install_failure_signal_handler: bool,
    );
    pub fn CoreWorkerProcessOptions_SetNodeIpAddress(node_ip_address: *const u8, len: usize);
    pub fn CoreWorkerProcessOptions_SetNodeManagerPort(node_manager_port: i32);
    pub fn CoreWorkerProcessOptions_SetRayletIpAddress(raylet_ip_address: *const u8, len: usize);
    pub fn CoreWorkerProcessOptions_SetDriverName(driver_name: *const u8, len: usize);
    pub fn CoreWorkerProcessOptions_SetMetricsAgentPort(metrics_agent_port: i32);
    pub fn CoreWorkerProcessOptions_SetStartupToken(startup_token: i64);
    pub fn CoreWorkerProcessOptions_SetRuntimeEnvHash(runtime_env_hash: i32);
    pub fn CoreWorkerProcessOptions_SetSerializedJobConfig(buf: *const u8, len: usize);
    pub fn CoreWorkerProcessOptions_SetTaskExecutionCallback();
    pub fn CoreWorkerProcessOptions_SetGcsOptions();

    // TODO: more stuff
    pub fn CoreWorkerProcessOptions_UpdateGcsClientOptions(gcs_address: *const u8, len: usize);

    // Object store related functions
    pub fn CoreWorker_Put(
        buf: *const u8,
        len: usize,
        object_id: *const u8,
        object_id_len: usize,
    ) -> i32;
    pub fn CoreWorker_GetMulti(
        obj_ids_buf: *const *const u8,
        obj_ids_len: *const usize,
        obj_ids_num: usize,
        data: *const *mut u8,
        data_len: *mut usize,
        data_num: *mut usize,
        timeout_ms: i32,
    ) -> i32;
    pub fn CoreWorker_Get(
        obj_id_buf: *const u8,
        obj_id_len: usize,
        data: *mut u8,
        data_len: *mut usize,
        timeout_ms: i32,
    ) -> i32;
    pub fn CoreWorker_WaitMulti(
        obj_ids_buf: *const *const u8,
        obj_ids_len: *const usize,
        obj_ids_num: usize,
        num_objects: usize,
        results: *mut bool,
        timeout_ms: i32,
    ) -> i32;

    pub fn CoreWorker_AddLocalReference(object_id: *const u8, object_id_len: usize) -> i32;
    pub fn CoreWorker_RemoveLocalReference(object_id: *const u8, object_id_len: usize) -> i32;

    // task submission related functions
    pub fn CoreWorker_SubmitActorTask(
        actor_id: *const u8,
        actor_id_len: usize,
        ray_function: *mut c_void,
        args: *mut c_void,
        task_options: *mut c_void,
        return_id_buf: *mut u8,
        return_id_len: *mut usize,
    ) -> i32;

    pub fn CoreWorker_SubmitTask(
        ray_function: *mut c_void,
        args: *mut c_void,
        task_options: *mut c_void,
        return_id_buf: *mut u8,
        return_id_len: *mut usize,
    ) -> i32;
    pub fn CoreWorker_GetActor(
        actor_name: *const u8,
        actor_name_len: usize,
        ray_namespace: *const u8,
        ray_namespace_len: usize,
        actor_id_buf: *mut u8,
        actor_id_len: *mut usize,
    ) -> i32;

    // task options related functions
    pub fn TaskOptions_Create() -> *mut c_void;
    pub fn TaskOptions_Destroy(task_options: *mut c_void);
    pub fn TaskOptions_SetNumReturns(task_options: *mut c_void, num_returns: usize) -> i32;
    pub fn TaskOptions_SetName(task_options: *mut c_void, name: *const u8, len: usize) -> i32;
    pub fn TaskOptions_SetConcurrencyGroupName(
        task_options: *mut c_void,
        name: *const u8,
        len: usize,
    ) -> i32;
    pub fn TaskOptions_SetSerializedRuntimeEnvInfo(
        task_options: *mut c_void,
        buf: *const u8,
        len: usize,
    ) -> i32;
    pub fn TaskOptions_AddResource(
        task_options: *mut c_void,
        name: *const u8,
        name_len: usize,
        value: f64,
    ) -> i32;

    // Ray function related functions
    pub fn RayFunction_Create() -> *mut c_void;
    pub fn RayFunction_Destroy(ray_function: *mut c_void);
    pub fn RayFunction_BuildCpp(
        ray_function: *mut c_void,
        function_name: *const u8,
        function_name_len: usize,
        class_name: *const u8,
        class_name_len: usize,
    ) -> i32;
    pub fn RayFunction_BuildPython(
        ray_function: *mut c_void,
        function_name: *const u8,
        function_name_len: usize,
        class_name: *const u8,
        class_name_len: usize,
        module_name: *const u8,
        module_name_len: usize,
    ) -> i32;
    pub fn RayFunction_BuildJava(
        ray_function: *mut c_void,
        function_name: *const u8,
        function_name_len: usize,
        class_name: *const u8,
        class_name_len: usize,
    ) -> i32;
    pub fn RayFunction_BuildWasm(
        ray_function: *mut c_void,
        function_name: *const u8,
        function_name_len: usize,
        module_name: *const u8,
        module_name_len: usize,
    ) -> i32;

    // TaskArg related functions
    pub fn TaskArg_Vec_Create() -> *mut c_void;
    pub fn TaskArg_Vec_Destroy(task_args: *mut c_void);
    pub fn TaskArg_Vec_PushByValue(
        task_args: *mut c_void,
        buf: *const u8,
        len: usize,
        meta: *const u8,
        meta_len: usize,
    ) -> i32;

}

pub struct RayFunction {
    pub raw: *mut c_void,
}

impl RayFunction {
    pub fn new() -> Self {
        let raw = unsafe { RayFunction_Create() };
        Self { raw }
    }

    pub fn build_cpp(&mut self, function_name: &str, class_name: &str) -> Result<()> {
        let ret = unsafe {
            RayFunction_BuildCpp(
                self.raw,
                function_name.as_ptr(),
                function_name.as_bytes().len(),
                class_name.as_ptr(),
                class_name.as_bytes().len(),
            )
        };
        if ret != 0 {
            return Err(anyhow!("build cpp function failed"));
        }
        Ok(())
    }

    pub fn build_python(
        &mut self,
        function_name: &str,
        class_name: &str,
        module_name: &str,
    ) -> Result<()> {
        let ret = unsafe {
            RayFunction_BuildPython(
                self.raw,
                function_name.as_ptr(),
                function_name.as_bytes().len(),
                class_name.as_ptr(),
                class_name.as_bytes().len(),
                module_name.as_ptr(),
                module_name.as_bytes().len(),
            )
        };
        if ret != 0 {
            return Err(anyhow!("build python function failed"));
        }
        Ok(())
    }

    pub fn build_java(&mut self, function_name: &str, class_name: &str) -> Result<()> {
        let ret = unsafe {
            RayFunction_BuildJava(
                self.raw,
                function_name.as_ptr(),
                function_name.as_bytes().len(),
                class_name.as_ptr(),
                class_name.as_bytes().len(),
            )
        };
        if ret != 0 {
            return Err(anyhow!("build java function failed"));
        }
        Ok(())
    }

    pub fn build_wasm(&mut self, function_name: &str, module_name: &str) -> Result<()> {
        let ret = unsafe {
            RayFunction_BuildWasm(
                self.raw,
                function_name.as_ptr(),
                function_name.as_bytes().len(),
                module_name.as_ptr(),
                module_name.as_bytes().len(),
            )
        };
        if ret != 0 {
            return Err(anyhow!("build wasm function failed"));
        }
        Ok(())
    }
}

impl Drop for RayFunction {
    fn drop(&mut self) {
        unsafe { RayFunction_Destroy(self.raw) };
    }
}

pub struct RayObject {
    pub raw: *mut c_void,
}

impl RayObject {
    // TODO: implement this
}

// sorry, if we link rust with c++, the linker will complain the atexit symbol cannot
// be found in core_worker_process.cc. We could use static-nobound, but it is only
// restricted to nightly rust. If we do this, we will see errors while compiling other
// crates. So we just declare the atexit symbol here.
#[no_mangle]
pub extern "C" fn atexit(f: extern "C" fn()) -> i32 {
    0
}
