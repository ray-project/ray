// Copyright 2026 The Ray Authors.
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

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <utility>

#include "ray/common/id.h"
#include "src/ray/protobuf/common.pb.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace observability {

/// A class that contain data that will be converted to rpc::TaskStateUpdate
struct TaskStateUpdate {
  TaskStateUpdate() = default;

  explicit TaskStateUpdate(const std::optional<const rpc::RayErrorInfo> &error_info)
      : error_info_(error_info) {}

  TaskStateUpdate(const NodeID &node_id, const WorkerID &worker_id)
      : node_id_(node_id), worker_id_(worker_id) {}

  explicit TaskStateUpdate(rpc::TaskLogInfo task_log_info)
      : task_log_info_(std::move(task_log_info)) {}

  TaskStateUpdate(std::string actor_repr_name, uint32_t pid)
      : actor_repr_name_(std::move(actor_repr_name)), pid_(pid) {}

  explicit TaskStateUpdate(uint32_t pid) : pid_(pid) {}

  explicit TaskStateUpdate(bool is_debugger_paused)
      : is_debugger_paused_(is_debugger_paused) {}

  /// Node id if it's a SUBMITTED_TO_WORKER status change.
  std::optional<NodeID> node_id_ = std::nullopt;
  /// Worker id if it's a SUBMITTED_TO_WORKER status change.
  std::optional<WorkerID> worker_id_ = std::nullopt;
  /// Task error info.
  std::optional<rpc::RayErrorInfo> error_info_ = std::nullopt;
  /// Task log info.
  std::optional<rpc::TaskLogInfo> task_log_info_ = std::nullopt;
  /// Actor task repr name.
  std::string actor_repr_name_;
  /// Worker's pid if it's a RUNNING status change.
  std::optional<uint32_t> pid_ = std::nullopt;
  /// If the task is paused by the debugger.
  std::optional<bool> is_debugger_paused_ = std::nullopt;
};

}  // namespace observability
}  // namespace ray
