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

#include "ray/common/bundle_spec.h"

namespace ray {

void BundleSpecification::ComputeResources() {
  auto unit_resource = MapFromProtobuf(message_->unit_resources());

  if (unit_resource.empty()) {
    // A static nil object is used here to avoid allocating the empty object every time.
    unit_resource_ = ResourceSet::Nil();
  } else {
    unit_resource_.reset(new ResourceSet(unit_resource));
  }
}

const ResourceSet& BundleSpecification::GetRequiredResources() const {
  return *unit_resource_;
}

std::pair<PlacementGroupID, int64_t> BundleSpecification::BundleId() const {
  if (message_->bundle_id()
          .placement_group_id()
          .empty() /* e.g., empty proto default */) {
    int64_t index = message_->bundle_id().bundle_index();
    return std::make_pair(PlacementGroupID::Nil(), index);
  }
  int64_t index = message_->bundle_id().bundle_index();
  return std::make_pair(
      PlacementGroupID::FromBinary(message_->bundle_id().placement_group_id()), index);
}

PlacementGroupID BundleSpecification::PlacementGroupId() const {
  return PlacementGroupID::FromBinary(message_->bundle_id().placement_group_id());
}

int64_t BundleSpecification::Index() const {
  return message_->bundle_id().bundle_index();
}

std::string FormatPlacementGroupResource(const std::string& original_resource_name,
                                         PlacementGroupID group_id,
                                         int64_t bundle_index) {
  auto str = original_resource_name + "_group_" + group_id.Hex() + "_" +
             std::to_string(bundle_index);
  RAY_CHECK(GetOriginalResourceName(str) == original_resource_name) << str;
  return str;
}

std::string FormatPlacementGroupResource(const std::string& original_resource_name,
                                         const BundleSpecification& bundle_spec) {
  return FormatPlacementGroupResource(
      original_resource_name, bundle_spec.PlacementGroupId(), bundle_spec.Index());
}

std::string GetOriginalResourceName(const std::string& resource) {
  auto idx = resource.find("_group_");
  RAY_CHECK(idx >= 0) << "This isn't a placement group resource " << resource;
  return resource.substr(0, idx);
}

}  // namespace ray
