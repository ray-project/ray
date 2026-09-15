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

  explicit TaskStateUpdate(const std::optional<const rpc::RayErrorInfo> &error_info_arg)
      : error_info(error_info_arg) {}

  TaskStateUpdate(const NodeID &node_id_arg, const WorkerID &worker_id_arg)
      : node_id(node_id_arg), worker_id(worker_id_arg) {}

  explicit TaskStateUpdate(rpc::TaskLogInfo task_log_info_arg)
      : task_log_info(std::move(task_log_info_arg)) {}

  TaskStateUpdate(std::string actor_repr_name_arg, uint32_t pid_arg)
      : actor_repr_name(std::move(actor_repr_name_arg)), pid(pid_arg) {}

  explicit TaskStateUpdate(uint32_t pid_arg) : pid(pid_arg) {}

  explicit TaskStateUpdate(bool is_debugger_paused_arg)
      : is_debugger_paused(is_debugger_paused_arg) {}

  /// Node id if it's a SUBMITTED_TO_WORKER status change.
  std::optional<NodeID> node_id = std::nullopt;
  /// Worker id if it's a SUBMITTED_TO_WORKER status change.
  std::optional<WorkerID> worker_id = std::nullopt;
  /// Task error info.
  std::optional<rpc::RayErrorInfo> error_info = std::nullopt;
  /// Task log info.
  std::optional<rpc::TaskLogInfo> task_log_info = std::nullopt;
  /// Actor task repr name.
  std::string actor_repr_name;
  /// Worker's pid if it's a RUNNING status change.
  std::optional<uint32_t> pid = std::nullopt;
  /// If the task is paused by the debugger.
  std::optional<bool> is_debugger_paused = std::nullopt;
};

}  // namespace observability
}  // namespace ray
