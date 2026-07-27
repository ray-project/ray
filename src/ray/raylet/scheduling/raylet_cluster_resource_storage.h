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

#include "ray/common/scheduling/cluster_resource_storage_interface.h"

namespace ray {
namespace raylet {

class RayletClusterResourceStorage : public ClusterResourceStorageInterface {
 public:
  RayletClusterResourceStorage();

  ~RayletClusterResourceStorage();

  void UpdateStoredResources(const scheduling::NodeID node_id,
                             const NodeResources &node_resources) override {}

  void DeleteStoredResources(const scheduling::NodeID node_id) override {}
};

}  // namespace raylet
}  // namespace ray
