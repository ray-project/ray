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

use anyhow::{anyhow, Result};
use libc::c_void;

// default value length for data
const DEFAULT_VALUE_LEN: usize = 10 * 1024 * 1024;

extern "C" {
    pub fn GcsClient_ClientOptions_Update(address: *const u8, address_len: usize);
    pub fn GcsClient_Create() -> *mut c_void;
    pub fn GcsClient_Destroy(gcs_client: *const c_void);
    pub fn GcsClient_InternalKV_Get(
        gcs_client: *const c_void,
        ns: *const u8,
        ns_len: usize,
        key: *const u8,
        key_len: usize,
        value: *mut u8,
        value_len: *mut usize,
    ) -> i32;
    pub fn GcsClient_InternalKV_Put(
        gcs_client: *const c_void,
        ns: *const u8,
        ns_len: usize,
        key: *const u8,
        key_len: usize,
        value: *const u8,
        value_len: usize,
    ) -> bool;
    pub fn GcsClient_InternalKV_Del(
        gcs_client: *const c_void,
        ns: *const u8,
        ns_len: usize,
        key: *const u8,
        key_len: usize,
    ) -> bool;
    pub fn GcsClient_InternalKV_Exists(
        gcs_client: *const c_void,
        ns: *const u8,
        ns_len: usize,
        key: *const u8,
        key_len: usize,
    ) -> bool;
}

pub struct GcsClient {
    pub raw: *mut c_void,
    pub internal_kv: InternalKV,
}

unsafe impl Send for GcsClient {}
unsafe impl Sync for GcsClient {}

impl Drop for GcsClient {
    fn drop(&mut self) {
        unsafe {
            GcsClient_Destroy(self.raw);
        }
    }
}

impl GcsClient {
    pub fn new(gcs_address: &str) -> Self {
        unsafe {
            GcsClient_ClientOptions_Update(gcs_address.as_ptr(), gcs_address.len());
            let gcs = GcsClient_Create();
            return GcsClient {
                raw: gcs,
                internal_kv: InternalKV::new(gcs),
            };
        };
    }
}

pub struct InternalKV {
    pub gcs_client_ptr: *const c_void,
}

impl InternalKV {
    pub fn new(gcs_client_ptr: *const c_void) -> Self {
        InternalKV { gcs_client_ptr }
    }

    pub fn get(&self, ns: &str, key: &str) -> Result<Vec<u8>> {
        // create a buffer to hold the value
        let mut value_len = DEFAULT_VALUE_LEN;
        let mut value = vec![0u8; value_len];

        let ret = unsafe {
            GcsClient_InternalKV_Get(
                self.gcs_client_ptr,
                ns.as_ptr(),
                ns.len(),
                key.as_ptr(),
                key.len(),
                value.as_mut_ptr(),
                &mut value_len,
            )
        };

        if ret != 0 {
            return Err(anyhow!("InternalKV get failed"));
        }

        value.resize(value_len, 0);
        Ok(value)
    }

    pub fn put(&self, ns: &str, key: &str, value: &[u8]) -> bool {
        unsafe {
            GcsClient_InternalKV_Put(
                self.gcs_client_ptr,
                ns.as_ptr(),
                ns.len(),
                key.as_ptr(),
                key.len(),
                value.as_ptr(),
                value.len(),
            )
        }
    }

    pub fn del(&self, ns: &str, key: &str) -> bool {
        unsafe {
            GcsClient_InternalKV_Del(
                self.gcs_client_ptr,
                ns.as_ptr(),
                ns.len(),
                key.as_ptr(),
                key.len(),
            )
        }
    }

    pub fn exists(&self, ns: &str, key: &str) -> bool {
        unsafe {
            GcsClient_InternalKV_Exists(
                self.gcs_client_ptr,
                ns.as_ptr(),
                ns.len(),
                key.as_ptr(),
                key.len(),
            )
        }
    }
}
