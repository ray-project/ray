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

#include "ray/common/placement_group.h"

namespace ray {
void PlacementGroupSpecification::ConstructBundles() {
  for (int i = 0; i < message_->bundles_size(); i++) {
    bundles_.push_back(BundleSpecification(message_->bundles(i)));
  }
}

PlacementGroupID PlacementGroupSpecification::PlacementGroupId() const {
  if (message_->placement_group_id().empty()) {
    return PlacementGroupID::Nil();
  } else {
    return PlacementGroupID::FromBinary(message_->placement_group_id());
  }
}

std::vector<BundleSpecification> PlacementGroupSpecification::GetBundles() const {
  return bundles_;
}

rpc::PlacementStrategy PlacementGroupSpecification::GetStrategy() const {
  return message_->strategy();
}

BundleSpecification PlacementGroupSpecification::GetBundle(int position) const {
  return bundles_[position];
}

std::string PlacementGroupSpecification::GetName() const {
  return std::string(message_->name());
}

rpc::Bundle BuildBundle(const std::unordered_map<std::string, double> &resources,
                        const size_t &bundle_index,
                        const PlacementGroupID &placement_group_id) {
  rpc::Bundle message_bundle;
  auto mutable_bundle_id = message_bundle.mutable_bundle_id();
  mutable_bundle_id->set_bundle_index(bundle_index);
  mutable_bundle_id->set_placement_group_id(placement_group_id.Binary());
  message_bundle.set_is_valid(true);
  auto mutable_unit_resources = message_bundle.mutable_unit_resources();
  for (const auto &resource : resources) {
    if (resource.second > 0) {
      mutable_unit_resources->insert({resource.first, resource.second});
    }
  }
  return message_bundle;
}

void LogBundlesChangedEventDebugInfo(
    const rpc::PlacementGroupBundlesChangedNotification &notification) {
  if (RAY_LOG_ENABLED(DEBUG)) {
    const PlacementGroupID &placement_group_id =
        PlacementGroupID::FromBinary(notification.placement_group_id());
    std::ostringstream debug_info;
    debug_info << "Receive a bundles update event from placement group: "
               << placement_group_id << " which is valid bundle index: [ ";
    for (int index = 0; index < notification.bundles_size(); index++) {
      if (notification.bundles(index)) {
        debug_info << index << ", ";
      }
    }
    debug_info << "] and invalid bundle index: [ ";
    for (int index = 0; index < notification.bundles_size(); index++) {
      if (!notification.bundles(index)) {
        debug_info << index << ", ";
      }
    }
    debug_info << "]";
    RAY_LOG(DEBUG) << debug_info.str();
  }
}

}  // namespace ray
