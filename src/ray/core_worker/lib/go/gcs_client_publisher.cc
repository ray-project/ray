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
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include "ray/core_worker/lib/go/gcs_client_bridge.h"
#include "ray/core_worker/lib/go/gcs_client_internal.h"
#include "ray/core_worker/lib/go/gcs_client_utils.h"
#include "ray/gcs_rpc_client/gcs_client.h"
#include "src/ray/protobuf/logging.pb.h"

namespace {

// Bounded wait for the asynchronous node-resource-usage publish RPC. Unlike
// publish_log_batch (whose timeout is supplied by the Go caller), this bridge
// has no caller-provided deadline, so it uses a fixed bound: if the RPC never
// completes, the call returns on time instead of blocking the Go caller (and
// the read lock it holds) forever.
constexpr auto kNodeResourceUsagePublishTimeout = std::chrono::seconds(60);

// Shared state bridging the asynchronous publish callback to the synchronous
// waiter. It is heap-allocated and captured by value in the callback, so a late
// callback only touches this shared state and never this stack frame.
struct NodeResourceUsagePublishState {
  std::mutex mtx;
  std::condition_variable cv;
  bool done = false;
  ray::Status status;
};

}  // namespace

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

int ray_gcs_client_publisher_publish_node_resource_usage(
    CGcsClient *client,
    const char *key_id,
    const char *node_resource_usage_json,
    char **error_out) {
  if (!client || !client->gcs_client) {
    set_error(error_out, "Invalid arguments: client is null");
    return 0;
  }

  try {
    // AsyncPublishNodeResourceUsage is asynchronous only, so we bridge it back to a
    // synchronous call for the Go layer. The shared state is heap-allocated and
    // captured by value in the callback, so a late callback only touches that shared
    // state and never this stack frame. The wait is bounded so a hung RPC cannot
    // block the CGO call (and the read lock the caller holds) forever.
    auto state = std::make_shared<NodeResourceUsagePublishState>();

    client->gcs_client->Publisher().AsyncPublishNodeResourceUsage(
        key_id ? std::string(key_id) : std::string(),
        node_resource_usage_json ? std::string(node_resource_usage_json) : std::string(),
        [state](const ray::Status &status) {
          std::lock_guard<std::mutex> lock(state->mtx);
          state->status = status;
          state->done = true;
          state->cv.notify_one();
        });

    std::unique_lock<std::mutex> lock(state->mtx);
    if (!state->cv.wait_for(
            lock, kNodeResourceUsagePublishTimeout, [&] { return state->done; })) {
      set_error(error_out, "publish node resource usage timed out after 60 seconds");
      return 0;
    }

    if (!state->status.ok()) {
      set_error(error_out, state->status.ToString().c_str());
      return 0;
    }
    return 1;
  } catch (const std::exception &e) {
    set_error(error_out, e.what());
    return 0;
  }
}

}  // extern "C"
