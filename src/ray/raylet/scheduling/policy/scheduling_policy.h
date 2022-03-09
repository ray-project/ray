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

/// ISchedulingPolicy picks a node to from the cluster, according to the resource
/// requirment as well as the scheduling options.
class ISchedulingPolicy {
 public:
  virtual ~ISchedulingPolicy() = default;
  /// \param resource_request: The resource request we're attempting to schedule.
  /// \param scheduling_options: scheduling options.
  ///
  /// \return NodeID::Nil() if the task is unfeasible, otherwise the node id
  /// to schedule on.
  virtual scheduling::NodeID Schedule(const ResourceRequest &resource_request,
                                      SchedulingOptions options) = 0;
};
}  // namespace raylet_scheduling_policy
}  // namespace ray
