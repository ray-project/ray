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

/// Gcs resource manager interface.
/// It is used for actor and placement group scheduling.
/// Non-thread safe.
class GcsResourceManagerInterface {
 public:
  virtual ~GcsResourceManagerInterface() {}

  /// Get the resources of all nodes in the cluster.
  ///
  /// \return The resources of all nodes in the cluster.
  virtual const absl::flat_hash_map<NodeID, ResourceSet> &GetClusterResources() const = 0;

  /// Acquire resources from the specified node. It will deduct directly from the node
  /// resource.
  ///
  /// \param node_id Id of a node.
  /// \param required_resources Resources to apply for.
  /// \return True if acquire resources successfully. False otherwise.
  virtual bool AcquireResource(const NodeID &node_id,
                               const ResourceSet &required_resources) = 0;

  /// Release the resource of the specified node. It will be added directly to the node
  /// resource.
  ///
  /// \param node_id Id of a node.
  /// \param acquired_resources Resources to release.
  /// \return True if release resources successfully. False otherwise.
  virtual bool ReleaseResource(const NodeID &node_id,
                               const ResourceSet &acquired_resources) = 0;
};

/// Gcs resource manager implementation. It obtains the available resources of nodes
/// through heartbeat reporting. Non-thread safe.
class GcsResourceManager : public GcsResourceManagerInterface {
 public:
  GcsResourceManager(GcsNodeManager &gcs_node_manager);

  virtual ~GcsResourceManager() = default;

  const absl::flat_hash_map<NodeID, ResourceSet> &GetClusterResources() const;

  bool AcquireResource(const NodeID &node_id, const ResourceSet &required_resources);

  bool ReleaseResource(const NodeID &node_id, const ResourceSet &acquired_resources);

 private:
  /// Map from node id to the resources of the node.
  absl::flat_hash_map<NodeID, ResourceSet> cluster_resources_;
};

}  // namespace gcs
}  // namespace ray
