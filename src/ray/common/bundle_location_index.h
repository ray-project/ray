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
#include "ray/common/id.h"
#include "ray/common/placement_group.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {

/// A data structure that helps fast bundle location lookup.
class BundleLocationIndex {
 public:
  BundleLocationIndex() {}
  ~BundleLocationIndex() = default;

  /// Add bundle locations to index.
  ///
  /// \param placement_group_id
  /// \param bundle_locations Bundle locations that will be associated with the placement
  /// group id.
  void AddBundleLocations(const PlacementGroupID &placement_group_id,
                          std::shared_ptr<BundleLocations> bundle_locations);

  /// Add or update bundles location to index. May be bundles of different PGs
  ///
  /// \param bundle_locations Bundles location
  void AddOrUpdateBundleLocations(
      const std::shared_ptr<BundleLocations> &bundle_locations);

  /// Add or update a bundle location to index.
  ///
  /// \param bundle_id
  /// \param node_id
  /// \param bundle_specialication
  void AddOrUpdateBundleLocation(
      const BundleID &bundle_id,
      const NodeID &node_id,
      std::shared_ptr<const BundleSpecification> bundle_specialication = nullptr);

  /// Erase bundle locations associated with a given node id.
  ///
  /// \param node_id The id of node.
  /// \return True if succeed. False otherwise.
  bool Erase(const NodeID &node_id);

  /// Erase bundle locations associated with a given placement group id.
  ///
  /// \param placement_group_id Placement group id
  /// \return True if succeed. False otherwise.
  bool Erase(const PlacementGroupID &placement_group_id);

  /// Get BundleLocation of placement group id.
  ///
  /// \param placement_group_id Placement group id of this bundle locations.
  /// \return Bundle locations that are associated with a given placement group id.
  const absl::optional<std::shared_ptr<BundleLocations> const> GetBundleLocations(
      const PlacementGroupID &placement_group_id) const;

  /// Get BundleLocation of node id.
  ///
  /// \param node_id Node id of this bundle locations.
  /// \return Bundle locations that are associated with a given node id.
  const absl::optional<std::shared_ptr<BundleLocations> const> GetBundleLocationsOnNode(
      const NodeID &node_id) const;

  std::optional<NodeID> GetBundleLocation(const BundleID &bundle_id) const;
  /// Update the index to contain new node information. Should be used only when new node
  /// is added to the cluster.
  ///
  /// \param alive_nodes map of alive nodes.
  void AddNodes(
      const absl::flat_hash_map<NodeID, std::shared_ptr<ray::rpc::GcsNodeInfo>> &nodes);

  /// get bundle_locations debug string info
  std::string GetBundleLocationDebugString(const BundleLocations &bundle_locations) const;

  /// get all debug string info
  std::string DebugString() const;

 private:
  /// only erase bundle in node_to_leased_bundles_
  void EraseBundleInNodeMap(const NodeID &node_id, const BundleID &bundle_id);
  /// Map from node ID to the set of bundles. This is used to lookup bundles at each node
  /// when a node is dead.
  absl::flat_hash_map<NodeID, std::shared_ptr<BundleLocations>> node_to_leased_bundles_;

  /// A map from placement group id to bundle locations.
  /// It is used to destroy bundles for the placement group.
  /// NOTE: It is a reverse index of `node_to_leased_bundles`.
  absl::flat_hash_map<PlacementGroupID, std::shared_ptr<BundleLocations>>
      placement_group_to_bundle_locations_;
};

}  // namespace ray
