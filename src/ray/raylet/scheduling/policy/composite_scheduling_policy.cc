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

#include "ray/raylet/scheduling/policy/composite_scheduling_policy.h"

#include <functional>

#include "ray/util/container_util.h"
#include "ray/util/util.h"

namespace ray {

namespace raylet_scheduling_policy {

scheduling::NodeID CompositeSchedulingPolicy::Schedule(
    const ResourceRequest &resource_request, SchedulingOptions options) {
  switch (options.scheduling_type) {
  case SchedulingType::SPREAD:
    return spread_policy_.Schedule(resource_request, options);
  case SchedulingType::RANDOM:
    return random_policy_.Schedule(resource_request, options);
  case SchedulingType::HYBRID:
    return hybrid_policy_.Schedule(resource_request, options);
  case SchedulingType::NODE_AFFINITY:
    return node_affinity_policy_.Schedule(resource_request, options);
  case SchedulingType::AFFINITY_WITH_BUNDLE:
    return affinity_with_bundle_policy_.Schedule(resource_request, options);
  default:
    RAY_LOG(FATAL) << "Unsupported scheduling type: "
                   << static_cast<typename std::underlying_type<SchedulingType>::type>(
                          options.scheduling_type);
  }
  UNREACHABLE;
}

SchedulingResult CompositeBundleSchedulingPolicy::Schedule(
    const std::vector<const ResourceRequest *> &resource_request_list,
    SchedulingOptions options) {
  switch (options.scheduling_type) {
  case SchedulingType::BUNDLE_PACK:
    return bundle_pack_policy_.Schedule(resource_request_list, options);
  case SchedulingType::BUNDLE_SPREAD:
    return bundle_spread_policy_.Schedule(resource_request_list, options);
  case SchedulingType::BUNDLE_STRICT_PACK:
    return bundle_strict_pack_policy_.Schedule(resource_request_list, options);
  case SchedulingType::BUNDLE_STRICT_SPREAD:
    return bundle_strict_spread_policy_.Schedule(resource_request_list, options);
  default:
    RAY_LOG(FATAL) << "Unsupported scheduling type: "
                   << static_cast<typename std::underlying_type<SchedulingType>::type>(
                          options.scheduling_type);
  }
  UNREACHABLE;
}

}  // namespace raylet_scheduling_policy
}  // namespace ray
