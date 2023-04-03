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
using namespace std;

static GcsClientOptions client_options;

extern "C" {

void GcsClientOptions_Update(const uint8_t *gcs_address, size_t gcs_address_len) {
  client_options =
      GcsClientOptions(std::string((const char *)gcs_address, gcs_address_len));
}

void *GlobalStateAccessor_Create() {
  auto *gcs = new GlobalStateAccessor(client_options);
  if (gcs->Connect()) {
    return gcs;
  } else {
    delete gcs;
    return nullptr;
  }
}

void GlobalStateAccessor_Destroy(void *global_state_accessor) {
  if (global_state_accessor != nullptr) {
    delete (GlobalStateAccessor *)global_state_accessor;
  }
}

int GlobalStateAccessor_GetNodeToConnectForDriver(void *global_state_accessor_raw,
                                                  const uint8_t *node_ip,
                                                  size_t node_ip_len,
                                                  uint8_t *node_to_connect,
                                                  size_t *node_to_connect_len) {
  if (global_state_accessor_raw == nullptr) {
    return -1;
  }
  auto global_state_accessor = (GlobalStateAccessor *)global_state_accessor_raw;
  std::string node_to_connect_str;
  auto status = global_state_accessor->GetNodeToConnectForDriver(
      std::string((const char *)node_ip, node_ip_len), &node_to_connect_str);
  RAY_CHECK_OK(status);
  // make sure we have enough space
  RAY_CHECK(*node_to_connect_len >= node_to_connect_str.size());
  memcpy(node_to_connect, node_to_connect_str.c_str(), node_to_connect_str.size());
  *node_to_connect_len = node_to_connect_str.size();
  return 0;
}

int GlobalStateAccessor_GetInternalKV(void *global_state_accessor_raw,
                                      const uint8_t *ns,
                                      size_t ns_len,
                                      const uint8_t *key,
                                      size_t key_len,
                                      uint8_t *value,
                                      size_t *value_len) {
  if (global_state_accessor_raw == nullptr) {
    return -1;
  }
  auto global_state_accessor = (GlobalStateAccessor *)global_state_accessor_raw;
  auto session_dir = global_state_accessor->GetInternalKV(
      std::string((const char *)ns, ns_len), std::string((const char *)key, key_len));
  // make sure we have enough space
  RAY_CHECK(*value_len >= session_dir->size());
  strncpy((char *)value, session_dir->c_str(), session_dir->size());
  *value_len = session_dir->size();
  return 0;
}

int GlobalStateAccessor_GetNextJobID_Binary(void *global_state_accessor_raw,
                                            uint32_t *job_id_buf,
                                            size_t *job_id_buf_len) {
  if (global_state_accessor_raw == nullptr) {
    return -1;
  }
  auto global_state_accessor = (GlobalStateAccessor *)global_state_accessor_raw;
  auto job_id = global_state_accessor->GetNextJobID().Binary();
  // make sure we have enough space
  RAY_CHECK(*job_id_buf_len >= job_id.size());
  memcpy(job_id_buf, job_id.c_str(), job_id.size());
  *job_id_buf_len = job_id.size();
  return 0;
}

int GlobalStateAccessor_GetNextJobID_Hex(void *global_state_accessor_raw,
                                         uint32_t *job_id_buf,
                                         size_t *job_id_buf_len) {
  if (global_state_accessor_raw == nullptr) {
    return -1;
  }
  auto global_state_accessor = (GlobalStateAccessor *)global_state_accessor_raw;
  auto job_id = global_state_accessor->GetNextJobID().Hex();
  // make sure we have enough space
  RAY_CHECK(*job_id_buf_len >= job_id.size());
  memcpy(job_id_buf, job_id.c_str(), job_id.size());
  *job_id_buf_len = job_id.size();
  return 0;
}

}  // extern "C"
