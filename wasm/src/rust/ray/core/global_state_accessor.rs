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

extern "C" {
    pub fn GcsClientOptions_Update(gcs_address: *const u8, len: u32);
    pub fn GlobalStateAccessor_Init();
    pub fn GlobalStateAccessor_GetNodeToConnectForDriver(
        node_ip: *const u8,
        node_to_connect: *const u8,
        node_to_connect_length: *mut u32,
    );
    pub fn GlobalStateAccessor_GetInternalKV(
        ns: *const u8,
        ns_length: u32,
        key: *const u8,
        key_length: u32,
        value: *const u8,
        value_length: *mut u32,
    );

    pub fn GlobalStateAccessor_GetNextJobID_Binary(job_id: *const u8, job_id_length: *mut u32);
    pub fn GlobalStateAccessor_GetNextJobID_Hex(job_id: *const u8, job_id_length: *mut u32);
}
