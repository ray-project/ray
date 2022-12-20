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

#include "ray/common/bundle_location_index.h"

namespace ray {

void BundleLocationIndex::AddBundleLocations(
    const PlacementGroupID &placement_group_id,
    std::shared_ptr<BundleLocations> bundle_locations) {
  // Update `placement_group_to_bundle_locations_`.
  // The placement group may be scheduled several times to succeed, so we need to merge
  // `bundle_locations` instead of covering it directly.
  auto iter = placement_group_to_bundle_locations_.find(placement_group_id);
  if (iter == placement_group_to_bundle_locations_.end()) {
    placement_group_to_bundle_locations_.emplace(placement_group_id, bundle_locations);
  } else {
    iter->second->insert(bundle_locations->begin(), bundle_locations->end());
  }

  // Update `node_to_leased_bundles_`.
  for (auto iter : *bundle_locations) {
    const auto &node_id = iter.second.first;
    if (!node_to_leased_bundles_.contains(node_id)) {
      node_to_leased_bundles_[node_id] = std::make_shared<BundleLocations>();
    }
    node_to_leased_bundles_[node_id]->emplace(iter.first, iter.second);
  }
}

void BundleLocationIndex::AddOrUpdateBundleLocation(
    const BundleID &bundle_id,
    const NodeID &node_id,
    std::shared_ptr<const BundleSpecification> bundle_specification) {
  const auto &placement_group_id = bundle_id.first;
  if (!placement_group_to_bundle_locations_.contains(placement_group_id)) {
    placement_group_to_bundle_locations_[placement_group_id] =
        std::make_shared<BundleLocations>();
  }
  const auto &bundle_locations = placement_group_to_bundle_locations_[placement_group_id];
  if (bundle_locations->contains(bundle_id)) {
    // If the bundle is on other nodes before, need to clean up node_to_bundles first.
    const auto &old_pair = (*bundle_locations)[bundle_id];
    if (old_pair.first != node_id) {
      EraseBundleInNodeMap(old_pair.first, bundle_id);
    }
  }
  (*bundle_locations)[bundle_id] = std::make_pair(node_id, bundle_specification);

  if (!node_to_leased_bundles_.contains(node_id)) {
    node_to_leased_bundles_[node_id] = std::make_shared<BundleLocations>();
  }
  (*node_to_leased_bundles_[node_id])[bundle_id] =
      std::make_pair(node_id, bundle_specification);
}

void BundleLocationIndex::AddOrUpdateBundleLocations(
    const std::shared_ptr<BundleLocations> &bundle_locations) {
  for (const auto &iter : *bundle_locations) {
    AddOrUpdateBundleLocation(iter.first, iter.second.first, iter.second.second);
  }
}

bool BundleLocationIndex::Erase(const NodeID &node_id) {
  const auto leased_bundles_it = node_to_leased_bundles_.find(node_id);
  if (leased_bundles_it == node_to_leased_bundles_.end()) {
    return false;
  }

  const auto &bundle_locations = leased_bundles_it->second;
  for (const auto &bundle_location : *bundle_locations) {
    // Remove corresponding placement group id.
    const auto &bundle_id = bundle_location.first;
    const auto placement_group_id = bundle_id.first;
    auto placement_group_it =
        placement_group_to_bundle_locations_.find(placement_group_id);
    if (placement_group_it != placement_group_to_bundle_locations_.end()) {
      auto &pg_bundle_locations = placement_group_it->second;
      auto pg_bundle_it = pg_bundle_locations->find(bundle_id);
      if (pg_bundle_it != pg_bundle_locations->end()) {
        pg_bundle_locations->erase(pg_bundle_it);
      }
    }
  }
  node_to_leased_bundles_.erase(leased_bundles_it);
  return true;
}

bool BundleLocationIndex::Erase(const PlacementGroupID &placement_group_id) {
  auto it = placement_group_to_bundle_locations_.find(placement_group_id);
  if (it == placement_group_to_bundle_locations_.end()) {
    return false;
  }

  const auto &bundle_locations = it->second;
  // Remove bundles from node_to_leased_bundles_ because bundles are removed now.
  for (const auto &bundle_location : *bundle_locations) {
    const auto &bundle_id = bundle_location.first;
    const auto &node_id = bundle_location.second.first;
    EraseBundleInNodeMap(node_id, bundle_id);
  }
  placement_group_to_bundle_locations_.erase(it);

  return true;
}

const absl::optional<std::shared_ptr<BundleLocations> const>
BundleLocationIndex::GetBundleLocations(
    const PlacementGroupID &placement_group_id) const {
  auto it = placement_group_to_bundle_locations_.find(placement_group_id);
  if (it == placement_group_to_bundle_locations_.end()) {
    return {};
  }
  return it->second;
}

const absl::optional<std::shared_ptr<BundleLocations> const>
BundleLocationIndex::GetBundleLocationsOnNode(const NodeID &node_id) const {
  auto it = node_to_leased_bundles_.find(node_id);
  if (it == node_to_leased_bundles_.end()) {
    return {};
  }
  return it->second;
}

std::optional<NodeID> BundleLocationIndex::GetBundleLocation(
    const BundleID &bundle_id) const {
  auto all_bundle_locations_opt = GetBundleLocations(bundle_id.first);
  if (all_bundle_locations_opt) {
    const auto &iter = (*all_bundle_locations_opt)->find(bundle_id);
    if (iter != (*all_bundle_locations_opt)->end()) {
      return std::make_optional(iter->second.first);
    }
  }
  return std::nullopt;
}

void BundleLocationIndex::AddNodes(
    const absl::flat_hash_map<NodeID, std::shared_ptr<ray::rpc::GcsNodeInfo>> &nodes) {
  for (const auto &iter : nodes) {
    if (!node_to_leased_bundles_.contains(iter.first)) {
      node_to_leased_bundles_[iter.first] = std::make_shared<BundleLocations>();
    }
  }
}

std::string BundleLocationIndex::GetBundleLocationDebugString(
    const BundleLocations &bundle_locations) const {
  std::ostringstream ostr;
  ostr << "[";
  for (const auto &iter : bundle_locations) {
    ostr << "{bundle_index:" << iter.first.second
         << ", node_id:" << scheduling::NodeID(iter.second.first.Binary()).ToInt()
         << "},\n";
  }
  ostr << "]";
  return ostr.str();
}

std::string BundleLocationIndex::DebugString() const {
  std::ostringstream ostr;
  ostr << "{ \"placment group locations\": [";
  for (const auto &iter : placement_group_to_bundle_locations_) {
    ostr << "{placement group id: " << iter.first << ", ";
    ostr << "bundle locations:" << GetBundleLocationDebugString(*iter.second);
    ostr << "},";
  }
  ostr << "], \"node to bundles\": [";
  for (const auto &iter : node_to_leased_bundles_) {
    ostr << "{node id: " << scheduling::NodeID(iter.first.Binary()).ToInt() << ", ";
    ostr << "bundles:";
    ostr << "[";
    for (const auto &bundle_iter : (*iter.second)) {
      ostr << "{pg_id:" << bundle_iter.first.first
           << ", bundle_index:" << bundle_iter.first.second << "},";
    }
    ostr << "]";
    ostr << "},";
  }
  ostr << "]}";
  return ostr.str();
}

void BundleLocationIndex::EraseBundleInNodeMap(const NodeID &node_id,
                                               const BundleID &bundle_id) {
  const auto &node_bundles_it = node_to_leased_bundles_.find(node_id);
  if (node_bundles_it != node_to_leased_bundles_.end()) {
    node_bundles_it->second->erase(bundle_id);
    if (node_bundles_it->second->empty()) {
      node_to_leased_bundles_.erase(node_bundles_it);
    }
  }
}

}  // namespace ray
