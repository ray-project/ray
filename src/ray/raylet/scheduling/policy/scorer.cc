// Copyright 2017 The Ray Authors.
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

#include "ray/raylet/scheduling/policy/scorer.h"

#include <numeric>

namespace ray {
namespace raylet_scheduling_policy {

double LeastResourceScorer::Score(const ResourceRequest &required_resources,
                                  const NodeResources &node_resources) {
  // In GCS-based actor scheduling, the `NodeResources` are only acquired or released by
  // actor scheduling, instead of being updated by resource reports from raylets. So we
  // have to subtract normal task resources (if exist) from the current available
  // resources.
  const NodeResources *node_resources_ptr = &node_resources;
  NodeResources new_node_resources;
  if (!node_resources.normal_task_resources.IsEmpty()) {
    new_node_resources = node_resources;
    new_node_resources.available -= node_resources.normal_task_resources;
    new_node_resources.available.RemoveNegative();
    node_resources_ptr = &new_node_resources;
  }

  double node_score = 0.;
  for (auto &resource_id : required_resources.ResourceIds()) {
    const auto &request_resource = required_resources.Get(resource_id);
    const auto &node_available_resource = node_resources_ptr->available.Get(resource_id);
    auto score = Calculate(request_resource, node_available_resource);
    if (score < 0.) {
      return -1.;
    }
    node_score += score;
  }
  return node_score;
}

double LeastResourceScorer::Calculate(const FixedPoint &requested,
                                      const FixedPoint &available) {
  RAY_CHECK(available >= 0) << "Available resource " << available.Double()
                            << " should be nonnegative.";
  if (requested > available) {
    return -1;
  }

  if (available == 0) {
    return 0;
  }

  return (available - requested).Double() / available.Double();
}

}  // namespace raylet_scheduling_policy
}  // namespace ray
