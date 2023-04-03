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

// declare extern c global state accessor functions

use libc::c_void;
use tracing::{debug, info};

pub struct GlobalStateAccessor {
    pub raw: *mut c_void,
}

impl Drop for GlobalStateAccessor {
    fn drop(&mut self) {
        debug!("destroying global state accessor");
        unsafe {
            GlobalStateAccessor_Destroy(self.raw);
        }
    }
}

impl GlobalStateAccessor {
    pub fn new(gcs_address: &str) -> Self {
        debug!(
            "initializing global state accessor using gcs address: {}",
            gcs_address
        );
        unsafe {
            GcsClientOptions_Update(gcs_address.as_ptr(), gcs_address.len());
            let mut gcs = GlobalStateAccessor_Create();
            return GlobalStateAccessor { raw: gcs };
        };
    }

    pub fn get_node_to_connect_for_driver(&self, node_ip: &str) -> Vec<u8> {
        // create a buffer to hold the node to connect
        let mut node_to_connect_len = 512;
        let mut node_to_connect = vec![0u8; node_to_connect_len];

        let ret = unsafe {
            GlobalStateAccessor_GetNodeToConnectForDriver(
                self.raw,
                node_ip.as_ptr(),
                node_ip.len(),
                node_to_connect.as_mut_ptr(),
                &mut node_to_connect_len,
            )
        };
        if ret != 0 {
            panic!("failed to get node to connect for driver");
        }
        node_to_connect.truncate(node_to_connect_len);
        node_to_connect
    }

    pub fn get_internal_kv(&self, ns: &str, key: &str) -> String {
        // create a buffer to hold the value
        let mut value_len = 4096;
        let mut value = vec![0u8; value_len];

        let ret = unsafe {
            GlobalStateAccessor_GetInternalKV(
                self.raw,
                ns.as_ptr(),
                ns.len(),
                key.as_ptr(),
                key.len(),
                value.as_mut_ptr(),
                &mut value_len,
            )
        };
        if ret != 0 {
            panic!("failed to get internal kv");
        }
        value.truncate(value_len);
        String::from_utf8(value).unwrap()
    }

    pub fn get_next_job_id_binary(&self) -> Vec<u8> {
        // create a buffer to hold the job id
        let mut job_id_len = 64;
        let mut job_id = vec![0u8; job_id_len];

        let ret = unsafe {
            GlobalStateAccessor_GetNextJobID_Binary(self.raw, job_id.as_mut_ptr(), &mut job_id_len)
        };
        if ret != 0 {
            panic!("failed to get next job id");
        }
        job_id.truncate(job_id_len);
        job_id
    }

    pub fn get_next_job_id_hex(&self) -> String {
        // create a buffer to hold the job id
        let mut job_id_len = 128;
        let mut job_id = vec![0u8; job_id_len];

        let ret = unsafe {
            GlobalStateAccessor_GetNextJobID_Hex(self.raw, job_id.as_mut_ptr(), &mut job_id_len)
        };
        if ret != 0 {
            panic!("failed to get next job id");
        }
        job_id.truncate(job_id_len);
        String::from_utf8(job_id).unwrap()
    }
}

extern "C" {
    pub fn GcsClientOptions_Update(gcs_address: *const u8, len: usize);
    pub fn GlobalStateAccessor_Create() -> *mut c_void;
    pub fn GlobalStateAccessor_Destroy(ptr: *mut c_void);

    pub fn GlobalStateAccessor_GetNodeToConnectForDriver(
        gsa: *mut c_void,
        node_ip: *const u8,
        node_ip_len: usize,
        node_to_connect: *const u8,
        node_to_connect_len: *mut usize,
    ) -> i32;
    pub fn GlobalStateAccessor_GetInternalKV(
        gsa: *mut c_void,
        ns: *const u8,
        ns_len: usize,
        key: *const u8,
        key_len: usize,
        value: *const u8,
        value_len: *mut usize,
    ) -> i32;

    pub fn GlobalStateAccessor_GetNextJobID_Binary(
        gsa: *mut c_void,
        job_id: *const u8,
        job_id_len: *mut usize,
    ) -> i32;
    pub fn GlobalStateAccessor_GetNextJobID_Hex(
        gsa: *mut c_void,
        job_id: *const u8,
        job_id_len: *mut usize,
    ) -> i32;
}
