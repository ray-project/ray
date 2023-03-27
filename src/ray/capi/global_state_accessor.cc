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

#include "ray/gcs/gcs_client/global_state_accessor.h"

#include <boost/algorithm/string.hpp>

#include "ray/common/asio/instrumented_io_context.h"

using namespace ray::gcs;

static GcsClientOptions client_options;
std::unique_ptr<GlobalStateAccessor> global_state_accessor(nullptr);

extern "C" {

void GcsClientOptions_Update(const uint8_t *gcs_address, uint32_t gcs_address_len) {
  client_options =
      GcsClientOptions(std::string((const char *)gcs_address, gcs_address_len));
}

void GlobalStateAccessor_Init() {
  global_state_accessor.reset(new GlobalStateAccessor(client_options));
  RAY_CHECK(global_state_accessor->Connect());
}

void GlobalStateAccessor_GetNodeToConnectForDriver(const uint8_t *node_ip,
                                                   uint8_t *node_to_connect,
                                                   uint32_t *node_to_connect_len) {
  std::string node_to_connect_str;
  auto status = global_state_accessor->GetNodeToConnectForDriver(
      std::string((const char *)node_ip), &node_to_connect_str);
  RAY_CHECK_OK(status);
  // make sure we have enough space
  RAY_CHECK(*node_to_connect_len >= node_to_connect_str.size());
  memcpy(node_to_connect, node_to_connect_str.c_str(), node_to_connect_str.size());
  *node_to_connect_len = node_to_connect_str.size();
}

void GlobalStateAccessor_GetInternalKV(const uint8_t *ns,
                                       uint32_t ns_len,
                                       const uint8_t *key,
                                       uint32_t key_len,
                                       uint8_t *value,
                                       uint32_t *value_len) {
  auto session_dir = global_state_accessor->GetInternalKV(
      std::string((const char *)ns, ns_len), std::string((const char *)key, key_len));
  // make sure we have enough space
  RAY_CHECK(*value_len >= session_dir->size());
  strncpy((char *)value, session_dir->c_str(), session_dir->size());
  *value_len = session_dir->size();
}

void GlobalStateAccessor_GetNextJobID_Binary(uint32_t *job_id_buf,
                                             uint32_t *job_id_buf_len) {
  auto job_id = global_state_accessor->GetNextJobID().Binary();
  // make sure we have enough space
  RAY_CHECK(*job_id_buf_len >= job_id.size());
  memcpy(job_id_buf, job_id.c_str(), job_id.size());
  *job_id_buf_len = job_id.size();
}

void GlobalStateAccessor_GetNextJobID_Hex(uint32_t *job_id_buf,
                                          uint32_t *job_id_buf_len) {
  auto job_id = global_state_accessor->GetNextJobID().Hex();
  // make sure we have enough space
  RAY_CHECK(*job_id_buf_len >= job_id.size());
  memcpy(job_id_buf, job_id.c_str(), job_id.size());
  *job_id_buf_len = job_id.size();
}

}  // extern "C"
