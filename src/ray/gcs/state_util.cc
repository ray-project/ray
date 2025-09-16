// Copyright 2023 The Ray Authors.
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

#include "ray/gcs/state_util.h"

#include <string>

namespace ray {
namespace gcs {

void FillAggregateLoad(
    const rpc::ResourcesData &resources_data,
    absl::flat_hash_map<ResourceDemandKey, rpc::ResourceDemand> *aggregate_load) {
  const auto &load = resources_data.resource_load_by_shape();
  for (const auto &demand : load.resource_demands()) {
    ResourceDemandKey key;
    key.shape = demand.shape();

    key.label_selectors.reserve(demand.label_selectors().size());
    for (const auto &selector : demand.label_selectors()) {
      key.label_selectors.push_back(selector);
    }
    auto &aggregate_demand = (*aggregate_load)[key];
    aggregate_demand.set_num_ready_requests_queued(
        aggregate_demand.num_ready_requests_queued() +
        demand.num_ready_requests_queued());
    aggregate_demand.set_num_infeasible_requests_queued(
        aggregate_demand.num_infeasible_requests_queued() +
        demand.num_infeasible_requests_queued());
    aggregate_demand.set_backlog_size(aggregate_demand.backlog_size() +
                                      demand.backlog_size());
  }
}

}  // namespace gcs
}  // namespace ray
