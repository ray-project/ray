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

#include "ray/common/id.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

/// `GcsInitData` is used to initialize all modules which need to recovery status when GCS
/// server restarts.
/// It loads all required metadata from the store into memory at once, so that the next
/// initialization process can be synchronized.
class GcsInitData {
 public:
  /// Create a GcsInitData.
  ///
  /// \param gcs_table_storage The storage from which the metadata will be loaded.
  explicit GcsInitData(std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage)
      : gcs_table_storage_(std::move(gcs_table_storage)) {}

  /// Load all required metadata from the store into memory at once asynchronously.
  ///
  /// \param on_done The callback when all metadatas are loaded successfully.
  void AsyncLoad(const EmptyCallback &on_done);

  /// Get job metadata.
  const absl::flat_hash_map<JobID, rpc::JobTableData> &Jobs() const {
    return job_table_data_;
  }

  /// Get node metadata.
  const absl::flat_hash_map<NodeID, rpc::GcsNodeInfo> &Nodes() const {
    return node_table_data_;
  }

  /// Get resource metadata.
  const absl::flat_hash_map<NodeID, rpc::ResourceMap> &ClusterResources() const {
    return resource_table_data_;
  }

  /// Get actor metadata.
  const absl::flat_hash_map<ActorID, rpc::ActorTableData> &Actors() const {
    return actor_table_data_;
  }

  /// Get placement group metadata.
  const absl::flat_hash_map<PlacementGroupID, rpc::PlacementGroupTableData>
      &PlacementGroups() const {
    return placement_group_table_data_;
  }

 private:
  /// Load job metadata from the store into memory asynchronously.
  ///
  /// \param on_done The callback when job metadata is loaded successfully.
  void AsyncLoadJobTableData(const EmptyCallback &on_done);

  /// Load node metadata from the store into memory asynchronously.
  ///
  /// \param on_done The callback when node metadata is loaded successfully.
  void AsyncLoadNodeTableData(const EmptyCallback &on_done);

  /// Load resource metadata from the store into memory asynchronously.
  ///
  /// \param on_done The callback when resource metadata is loaded successfully.
  void AsyncLoadResourceTableData(const EmptyCallback &on_done);

  /// Load placement group metadata from the store into memory asynchronously.
  ///
  /// \param on_done The callback when placement group metadata is loaded successfully.
  void AsyncLoadPlacementGroupTableData(const EmptyCallback &on_done);

  /// Load actor metadata from the store into memory asynchronously.
  ///
  /// \param on_done The callback when actor metadata is loaded successfully.
  void AsyncLoadActorTableData(const EmptyCallback &on_done);

 protected:
  /// The gcs table storage.
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;

  /// Job metadata.
  absl::flat_hash_map<JobID, rpc::JobTableData> job_table_data_;

  /// Node metadata.
  absl::flat_hash_map<NodeID, rpc::GcsNodeInfo> node_table_data_;

  /// Resource metadata.
  absl::flat_hash_map<NodeID, rpc::ResourceMap> resource_table_data_;

  /// Placement group metadata.
  absl::flat_hash_map<PlacementGroupID, rpc::PlacementGroupTableData>
      placement_group_table_data_;

  /// Actor metadata.
  absl::flat_hash_map<ActorID, rpc::ActorTableData> actor_table_data_;
};

}  // namespace gcs
}  // namespace ray
