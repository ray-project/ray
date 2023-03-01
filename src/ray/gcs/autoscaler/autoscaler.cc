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

#include "ray/gcs/autoscaler/autoscaler.h"

namespace ray {
namespace autoscaler {

Autoscaler::Autoscaler(INodeProvider &node_provider,
                       gcs::GcsResourceManager &resource_manager,
                       std::unique_ptr<IAutoscalingPolicy> policy)
    : node_provider_(node_provider),
      resource_manager_(resource_manager),
      policy_(std::move(policy)) {}

void Autoscaler::RunOnce() {
  auto new_nodes_to_launch = GetNodesToLaunch();
  LaunchNewNodes(new_nodes_to_launch);
}

rpc::AvailableNodeTypesResponse Autoscaler::GetAvailableNodeTypes() {
  return node_provider_.GetAvailableNodeTypes();
}

std::vector<std::pair<rpc::NodeType, int32_t>> Autoscaler::GetLaunchingNodes() {
  return {};
}

std::vector<std::pair<rpc::NodeType, int32_t>> Autoscaler::GetNodesToLaunch() {
  ray::rpc::GetAllResourceUsageReply reply;
  resource_manager_.GetAllResourceUsage(&reply);

  // normal resources load.
  const ray::rpc::ResourceLoad &normal_resource_load =
      reply.resource_usage_data().resource_load_by_shape();

  // placement group resource load.
  const ray::rpc::PlacementGroupLoad &pg_load =
      reply.resource_usage_data().placement_group_load();

  // cluter resources ultilization.
  auto cluster_resources_view = resource_manager_.NodeResourceReportView();

  // available node types.
  auto available_node_types = GetAvailableNodeTypes();

  // nodes pending scaling up.
  auto nodes_launching = GetLaunchingNodes();

  return policy_->GetNewNodesToLaunch(normal_resource_load,
                                      pg_load,
                                      cluster_resources_view,
                                      available_node_types,
                                      nodes_launching);
}

void Autoscaler::LaunchNewNodes(
    const std::vector<std::pair<rpc::NodeType, int32_t>> &new_nodes) {
  for (auto &pair : new_nodes) {
    node_provider_.CreateNodes(pair.first.node_type_id(), pair.second);
  }
}

}  // namespace autoscaler
}  // namespace ray
