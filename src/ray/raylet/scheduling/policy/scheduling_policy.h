// Copyright 2021 The Ray Authors.
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

#include <vector>

#include "ray/raylet/scheduling/cluster_resource_data.h"
#include "ray/raylet/scheduling/policy/scheduling_options.h"
#include "ray/raylet/scheduling/scheduling_ids.h"

namespace ray {
namespace raylet_scheduling_policy {

// Status of resource scheduling result.
struct SchedulingResultStatus {
  enum class SchedulingResultStatusCode {
    // Scheduling failed but retryable.
    FAILED = 0,
    // Scheduling failed and non-retryable.
    INFEASIBLE = 1,
    // Scheduling successful.
    SUCCESS = 2,
    // Only part of the requested resources succeed when batch scheduling.
    PARTIAL_SUCCESS = 3,
  };

  bool IsFailed() const { return code == SchedulingResultStatusCode::FAILED; }
  bool IsInfeasible() const { return code == SchedulingResultStatusCode::INFEASIBLE; }
  bool IsSuccess() const { return code == SchedulingResultStatusCode::SUCCESS; }
  bool IsPartialSuccess() const {
    return code == SchedulingResultStatusCode::PARTIAL_SUCCESS;
  }
  SchedulingResultStatusCode Status() const { return code; }

  SchedulingResultStatusCode code = SchedulingResultStatusCode::SUCCESS;
};

struct SchedulingResult {
  static SchedulingResult Infeasible() {
    SchedulingResult result;
    result.status.code = SchedulingResultStatus::SchedulingResultStatusCode::INFEASIBLE;
    return result;
  }

  static SchedulingResult Failed() {
    SchedulingResult result;
    result.status.code = SchedulingResultStatus::SchedulingResultStatusCode::FAILED;
    return result;
  }

  static SchedulingResult Success(std::vector<scheduling::NodeID> &&nodes) {
    SchedulingResult result;
    result.status.code = SchedulingResultStatus::SchedulingResultStatusCode::SUCCESS;
    result.selected_nodes = std::move(nodes);
    return result;
  }

  static SchedulingResult PartialSuccess(std::vector<scheduling::NodeID> &&nodes) {
    SchedulingResult result;
    result.status.code =
        SchedulingResultStatus::SchedulingResultStatusCode::PARTIAL_SUCCESS;
    result.selected_nodes = std::move(nodes);
    return result;
  }

  // The status of scheduling.
  SchedulingResultStatus status;
  // The nodes successfully scheduled.
  std::vector<scheduling::NodeID> selected_nodes;
};

/// IBundleSchedulingPolicy picks a set of nodes from the cluster, according to the
/// resource requirment list as well as the scheduling options.
class IBundleSchedulingPolicy {
 public:
  virtual ~IBundleSchedulingPolicy() = default;

  /// Schedule the specified resources to the cluster nodes.
  ///
  /// \param resource_request_list The resource request list we're attempting to schedule.
  /// \param options: scheduling options.
  /// \param context: The context of current scheduling. Each policy can
  /// correspond to a different type of context.
  /// \return `SchedulingResult`, including the
  /// selected nodes if schedule successful, otherwise, it will return an empty vector and
  /// a flag to indicate whether this request can be retry or not.
  virtual SchedulingResult Schedule(
      const std::vector<const ResourceRequest *> &resource_request_list,
      SchedulingOptions options) = 0;
};

/// ISchedulingPolicy picks a node to from the cluster, according to the resource
/// requirment as well as the scheduling options.
class ISchedulingPolicy {
 public:
  virtual ~ISchedulingPolicy() = default;

  /// \param resource_request: The resource request we're attempting to schedule.
  /// \param options: scheduling options.
  ///
  /// \return NodeID::Nil() if the task is unfeasible, otherwise the node id
  /// to schedule on.
  virtual scheduling::NodeID Schedule(const ResourceRequest &resource_request,
                                      SchedulingOptions options) = 0;
};
}  // namespace raylet_scheduling_policy
}  // namespace ray
