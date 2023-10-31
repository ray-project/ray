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

#include "ray/gcs/gcs_server/state_util.h"

#include <unordered_map>

namespace ray {
namespace gcs {

void FillAggregateLoad(const rpc::ResourcesData &resources_data,
                       std::unordered_map<google::protobuf::Map<std::string, double>,
                                          rpc::ResourceDemand> *aggregate_load) {
  auto load = resources_data.resource_load_by_shape();
  for (const auto &demand : load.resource_demands()) {
    auto &aggregate_demand = (*aggregate_load)[demand.shape()];
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
