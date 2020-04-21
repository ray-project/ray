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

#ifndef RAY_GCS_NODE_RESOURCE_MANAGER_H
#define RAY_GCS_NODE_RESOURCE_MANAGER_H

#include <ray/common/id.h>
#include "ray/gcs/accessor.h"

namespace ray {
namespace gcs {

/// GcsNodeResourceManager is responsible for managing node resource.
/// This class is thread-safe.
class GcsNodeResourceManager {
 public:
  /// Update the resource of the specified node.
  ///
  /// \param node_id ID of the node to update the resource.
  /// \param resources Resource of the node to be updated.
  void UpdateNodeResources(const ClientID &node_id,
                           const NodeInfoAccessor::ResourceMap &resources);

  /// Delete the specified resource of the specified node.
  ///
  /// \param node_id ID of the node to delete the specified resource.
  /// \param resource_names The names of the resource to be deleted.
  void DeleteNodeResources(const ClientID &node_id,
                           const std::vector<std::string> &resource_names);

  /// Get the specified resource of the specified node.
  ///
  /// \param node_id ID of the node to get the specified resource.
  /// \param resource_names The names of the resource to get.
  /// \return Node resource found.
  boost::optional<NodeInfoAccessor::ResourceMap> GetNodeResources(
      const ClientID &node_id, const std::vector<std::string> &resource_names);

 private:
  /// Mutex to protect the nodes_resource_cache_ field.
  absl::Mutex mutex_;
  /// A mapping from node id to node resource.
  std::unordered_map<ClientID, gcs::NodeInfoAccessor::ResourceMap> nodes_resource_cache_
      GUARDED_BY(mutex_);
};

}  // namespace gcs
}  // namespace ray

#endif  // RAY_GCS_NODE_RESOURCE_MANAGER_H
