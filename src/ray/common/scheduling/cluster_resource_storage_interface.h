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

#include "ray/common/grpc_util.h"
#include "ray/common/id.h"
#include "ray/common/scheduling/cluster_resource_data.h"
#include "ray/common/scheduling/scheduling_ids.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
class ClusterResourceStorageInterface {
 public:
  virtual ~ClusterResourceStorageInterface() = default;

  virtual void UpdateStoredResources(const scheduling::NodeID node_id,
                                     const NodeResources &node_resources) = 0;

  virtual void DeleteStoredResources(const scheduling::NodeID node_id) = 0;

  void FillResourceUsage(const ray::NodeID node_id,
                         const NodeResources &node_resources,
                         rpc::ResourcesData *data) {
    // This populates usage information.
    data->set_node_id(node_id.Binary());

    auto total = node_resources.total.GetResourceMap();
    data->mutable_resources_total()->insert(total.begin(), total.end());

    auto available = node_resources.available.GetResourceMap();
    data->mutable_resources_available()->insert(available.begin(), available.end());

    data->mutable_labels()->insert(node_resources.labels.begin(),
                                   node_resources.labels.end());

    data->set_object_pulls_queued(node_resources.object_pulls_queued);
    data->set_idle_duration_ms(node_resources.idle_resource_duration_ms);
    data->set_is_draining(node_resources.is_draining);
    data->set_draining_deadline_timestamp_ms(
        node_resources.draining_deadline_timestamp_ms);
  }

  NodeResources NodeResourcesFromResourcesData(rpc::ResourcesData resources) {
    auto total = MapFromProtobuf(resources.resources_total());
    auto available = MapFromProtobuf(resources.resources_available());
    auto labels = MapFromProtobuf(resources.labels());
    auto node_resources = ResourceMapToNodeResources(total, available, labels);

    node_resources.is_draining = resources.is_draining();
    node_resources.draining_deadline_timestamp_ms =
        resources.draining_deadline_timestamp_ms();
    node_resources.object_pulls_queued = resources.object_pulls_queued();
    node_resources.idle_resource_duration_ms = resources.idle_duration_ms();

    return node_resources;
  }
};

}  // namespace ray
