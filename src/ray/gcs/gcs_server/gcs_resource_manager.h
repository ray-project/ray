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
#pragma once

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/gcs/gcs_server/gcs_node_manager.h"

namespace ray {
namespace gcs {

class GcsResourceManagerInterface {
 public:
  virtual ~GcsResourceManagerInterface() {}

  virtual const absl::flat_hash_map<NodeID, SchedulingResources> &GetClusterResources()
      const = 0;

  virtual bool AcquireResource(const NodeID &node_id,
                               const ResourceSet &required_resources) = 0;

  virtual bool ReleaseResource(const NodeID &node_id,
                               const ResourceSet &acquired_resources) = 0;
};

class GcsResourceManager : public GcsResourceManagerInterface {
 public:
  GcsResourceManager(const GcsNodeManager &gcs_node_manager);

  virtual ~GcsResourceManager() = default;

  const absl::flat_hash_map<NodeID, SchedulingResources> &GetClusterResources() const;

  bool AcquireResource(const NodeID &node_id, const ResourceSet &required_resources);

  bool ReleaseResource(const NodeID &node_id, const ResourceSet &acquired_resources);

 private:
  /// Map from node id to the scheduling resources of the node.
  absl::flat_hash_map<NodeID, SchedulingResources> cluster_scheduling_resources_;
};

}  // namespace gcs
}  // namespace ray
