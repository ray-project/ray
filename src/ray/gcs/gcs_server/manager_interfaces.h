// Copyright 2025 The Ray Authors.
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

#include "ray/common/id.h"
#include "src/ray/protobuf/autoscaler.pb.h"
#include "src/ray/protobuf/ray_syncer.pb.h"

namespace ray {
namespace gcs {

class NodeManagerInterface {
 public:
  /// Update node state from a resource view sync message if the node is alive.
  ///
  /// \param node_id The ID of the node to update.
  /// \param resource_view_sync_message The sync message containing the new state.
  virtual void UpdateAliveNode(
      const NodeID &node_id,
      const rpc::syncer::ResourceViewSyncMessage &resource_view_sync_message) = 0;

  /// Get alive node by ID.
  ///
  /// \param node_id The id of the node.
  /// \return the node if it is alive. Optional empty value if it is not alive.
  virtual std::optional<std::shared_ptr<rpc::GcsNodeInfo>> GetAliveNode(
      const NodeID &node_id) const = 0;

  /// Get all alive nodes.
  ///
  /// \return all alive nodes.
  virtual const absl::flat_hash_map<NodeID, std::shared_ptr<rpc::GcsNodeInfo>>
      &GetAllAliveNodes() const = 0;

  /// Get all dead nodes.
  virtual const absl::flat_hash_map<NodeID, std::shared_ptr<rpc::GcsNodeInfo>>
      &GetAllDeadNodes() const = 0;

  /// Set the node to be draining.
  ///
  /// \param node_id The ID of the draining node. This node must already
  /// be in the alive nodes.
  /// \param request The drain node request.
  virtual void SetNodeDraining(
      const NodeID &node_id,
      std::shared_ptr<rpc::autoscaler::DrainNodeRequest> request) = 0;
};

}  // namespace gcs
}  // namespace ray
