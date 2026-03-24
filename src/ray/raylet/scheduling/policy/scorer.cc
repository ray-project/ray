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

namespace ray {
namespace raylet_scheduling_policy {

double LeastResourceScorer::Score(const ResourceRequest &required_resources,
                                  const NodeResources &node_resources) {
  if (!node_resources.IsAvailable(required_resources)) {
    return -1.;
  }

  double node_score = 0.;
  for (auto &resource_id : required_resources.ResourceIds()) {
    const auto &request_resource = required_resources.Get(resource_id);
    auto node_available_resource = node_resources.available.Sum(resource_id);
    node_score += Calculate(request_resource, node_available_resource);
  }
  return node_score;
}

// This function assumes the resource request has already passed the availability check
double LeastResourceScorer::Calculate(const FixedPoint &requested,
                                      const FixedPoint &available) {
  RAY_CHECK(available >= 0) << "Available resource " << available.Double()
                            << " should be nonnegative.";

  if (available == 0) {
    return 0;
  }

  return (available - requested).Double() / available.Double();
}

}  // namespace raylet_scheduling_policy
}  // namespace ray
