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

#include "ray/gcs/gcs_cluster_resource_storage.h"

#include "ray/common/grpc_util.h"
#include "ray/gcs/postable/postable.h"

namespace ray {
namespace gcs {

ClusterResourceStorage::ClusterResourceStorage(GcsTableStorage *gcs_table_storage,
                                               instrumented_io_context &io_context)
    : gcs_table_storage_(gcs_table_storage), io_context_(io_context) {}

ClusterResourceStorage::~ClusterResourceStorage() {}

void ClusterResourceStorage::UpdateStoredResources(const scheduling::NodeID node_id,
                                                   const NodeResources &node_resources) {
  auto resource_data = rpc::ResourcesData();
  auto binary = node_id.Binary();
  if (binary == "-1") {
    // we can see this in tests, since integer IDs don't get added to the map
    return;
  }

  auto ray_node_id = NodeID::FromBinary(binary);
  FillResourceUsage(ray_node_id, node_resources, &resource_data);
  Put(ray_node_id, resource_data);
}

void ClusterResourceStorage::Put(const ray::NodeID node_id,
                                 const rpc::ResourcesData &data) {
  RAY_LOG(DEBUG).WithField(node_id) << "RESOURCE PUT";

  auto on_done = [](const Status &status) { RAY_CHECK_OK(status); };

  gcs_table_storage_->NodeResourcesTable().Put(
      node_id, data, {std::move(on_done), io_context_});
}

void ClusterResourceStorage::DeleteStoredResources(const scheduling::NodeID node_id) {
  auto binary = node_id.Binary();
  // we can see this in tests, since integer IDs don't get added to the map
  if (binary == "-1") {
    return;
  }

  auto ray_node_id = NodeID::FromBinary(binary);
  RAY_LOG(DEBUG).WithField(ray_node_id) << "RESOURCE DELETE";
  auto on_done = [](const Status &status) { RAY_CHECK_OK(status); };

  gcs_table_storage_->NodeResourcesTable().Delete(ray_node_id,
                                                  {std::move(on_done), io_context_});
}

}  // namespace gcs
}  // namespace ray
