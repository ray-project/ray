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

#include "ray/gcs/gcs_server/gcs_resource_manager.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

const absl::flat_hash_map<NodeID, SchedulingResources>
    &GcsResourceManager::GetClusterResources() const {
  return cluster_scheduling_resources_;
}

bool GcsResourceManager::AcquireResource(const NodeID &node_id,
                                         const ResourceSet &required_resources) {
  return true;
}

bool GcsResourceManager::ReleaseResource(const NodeID &node_id,
                                         const ResourceSet &acquired_resources) {
  return true;
}

}  // namespace gcs
}  // namespace ray
