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

  virtual void StartTransaction() = 0;

  virtual void CommitTransaction() = 0;

  virtual void RollbackTransaction() = 0;

  virtual const absl::flat_hash_map<NodeID, ResourceSet> &GetClusterResources() const = 0;

  virtual void AcquireResource(const NodeID &node_id,
                               const ResourceSet &required_resources) = 0;

  virtual void ReleaseResource(const NodeID &node_id,
                               const ResourceSet &acquired_resources) = 0;
};

class GcsResourceManager : public GcsResourceManagerInterface {
 public:
  GcsResourceManager(GcsNodeManager &gcs_node_manager);

  virtual ~GcsResourceManager() = default;

  void StartTransaction();

  void CommitTransaction();

  void RollbackTransaction();

  const absl::flat_hash_map<NodeID, ResourceSet> &GetClusterResources() const;

  void AcquireResource(const NodeID &node_id, const ResourceSet &required_resources);

  void ReleaseResource(const NodeID &node_id, const ResourceSet &acquired_resources);

 private:
  /// Check if there's any transaction going on.
  bool IsTransactionInProgress() const { return is_transaction_in_progress_; }

  bool is_transaction_in_progress_;

  /// Map from node id to the resources of the node.
  absl::flat_hash_map<NodeID, ResourceSet> cluster_resources_;

  absl::flat_hash_map<NodeID, std::list<ResourceSet>>
      resource_changes_during_transaction_;
};

}  // namespace gcs
}  // namespace ray
