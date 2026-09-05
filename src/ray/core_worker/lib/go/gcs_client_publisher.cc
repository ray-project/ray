// Copyright 2025 The Ray Authors.
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

// src/ray/core_worker/lib/go/gcs_client_publisher.cc
// GCS Client CGO bridge - Publisher operations
#include <string>
#include <utility>

#include "ray/core_worker/lib/go/gcs_client_bridge.h"
#include "ray/core_worker/lib/go/gcs_client_internal.h"
#include "ray/core_worker/lib/go/gcs_client_utils.h"
#include "ray/gcs_rpc_client/gcs_client.h"
#include "src/ray/protobuf/logging.pb.h"

extern "C" {

int ray_gcs_client_publisher_publish_log_batch(CGcsClient *client,
                                               const char *key_id,
                                               const char *ip,
                                               const char *pid,
                                               const char *job_id,
                                               int is_error,
                                               const char **lines,
                                               int line_count,
                                               const char *actor_name,
                                               const char *task_name,
                                               int64_t timeout_ms,
                                               char **error_out) {
  if (!client || !client->gcs_client) {
    set_error(error_out, "Invalid arguments: client is null");
    return 0;
  }

  try {
    ray::rpc::LogBatch log_batch;
    if (ip) {
      log_batch.set_ip(ip);
    }
    if (pid) {
      log_batch.set_pid(pid);
    }
    if (job_id) {
      log_batch.set_job_id(job_id);
    }
    log_batch.set_is_error(is_error != 0);
    if (actor_name) {
      log_batch.set_actor_name(actor_name);
    }
    if (task_name) {
      log_batch.set_task_name(task_name);
    }
    for (int i = 0; i < line_count; i++) {
      if (lines && lines[i]) {
        log_batch.add_lines(lines[i]);
      }
    }

    auto status = client->gcs_client->Publisher().PublishLogs(
        key_id ? std::string(key_id) : std::string(), std::move(log_batch), timeout_ms);
    if (!status.ok()) {
      set_error(error_out, status.ToString().c_str());
      return 0;
    }
    return 1;
  } catch (const std::exception &e) {
    set_error(error_out, e.what());
    return 0;
  }
}

}  // extern "C"
