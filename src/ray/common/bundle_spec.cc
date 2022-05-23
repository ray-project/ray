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
    static std::shared_ptr<ResourceRequest> nil_unit_resource =
        std::make_shared<ResourceRequest>();
    unit_resource_ = nil_unit_resource;
  } else {
    unit_resource_ = std::make_shared<ResourceRequest>(ResourceMapToResourceRequest(
        unit_resource, /*requires_object_store_memory=*/false));
  }

  // Generate placement group bundle labels.
  ComputeBundleResourceLabels();
}

void BundleSpecification::ComputeBundleResourceLabels() {
  RAY_CHECK(unit_resource_);

  for (auto &resource_id : unit_resource_->ResourceIds()) {
    auto resource_name = resource_id.Binary();
    auto resource_value = unit_resource_->Get(resource_id);

    /// With bundle index (e.g., CPU_group_i_zzz).
    const std::string &resource_label =
        FormatPlacementGroupResource(resource_name, PlacementGroupId(), Index());
    bundle_resource_labels_[resource_label] = resource_value.Double();

    /// Without bundle index (e.g., CPU_group_zzz).
    const std::string &wildcard_label =
        FormatPlacementGroupResource(resource_name, PlacementGroupId(), -1);
    bundle_resource_labels_[wildcard_label] = resource_value.Double();
  }
  auto bundle_label =
      FormatPlacementGroupResource(kBundle_ResourceLabel, PlacementGroupId(), -1);
  auto index_bundle_label =
      FormatPlacementGroupResource(kBundle_ResourceLabel, PlacementGroupId(), Index());
  bundle_resource_labels_[index_bundle_label] = bundle_resource_labels_[bundle_label] =
      1000;
}

const ResourceRequest &BundleSpecification::GetRequiredResources() const {
  return *unit_resource_;
}

absl::flat_hash_map<std::string, double> BundleSpecification::GetWildcardResources()
    const {
  absl::flat_hash_map<std::string, double> wildcard_resources;
  std::string pattern("_group_");
  for (const auto &[name, capacity] : bundle_resource_labels_) {
    auto idx = name.find(pattern);
    if (idx != std::string::npos &&
        name.find("_", idx + pattern.size()) == std::string::npos) {
      wildcard_resources[name] = capacity;
    }
  }
  return wildcard_resources;
}

BundleID BundleSpecification::BundleId() const {
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

NodeID BundleSpecification::NodeId() const {
  return NodeID::FromBinary(message_->node_id());
}

int64_t BundleSpecification::Index() const {
  return message_->bundle_id().bundle_index();
}

std::string BundleSpecification::DebugString() const {
  std::ostringstream stream;
  auto bundle_id = BundleId();
  stream << "placement group id={" << bundle_id.first << "}, bundle index={"
         << bundle_id.second << "}";
  return stream.str();
}

std::string FormatPlacementGroupResource(const std::string &original_resource_name,
                                         const PlacementGroupID &group_id,
                                         int64_t bundle_index) {
  std::stringstream os;
  if (bundle_index >= 0) {
    os << original_resource_name << kGroupKeyword << std::to_string(bundle_index) << "_"
       << group_id.Hex();
  } else {
    RAY_CHECK(bundle_index == -1) << "Invalid index " << bundle_index;
    os << original_resource_name << kGroupKeyword << group_id.Hex();
  }
  std::string result = os.str();
  RAY_DCHECK(GetOriginalResourceName(result) == original_resource_name)
      << "Generated: " << GetOriginalResourceName(result)
      << " Original: " << original_resource_name;
  return result;
}

std::string FormatPlacementGroupResource(const std::string &original_resource_name,
                                         const BundleSpecification &bundle_spec) {
  return FormatPlacementGroupResource(
      original_resource_name, bundle_spec.PlacementGroupId(), bundle_spec.Index());
}

bool IsBundleIndex(const std::string &resource,
                   const PlacementGroupID &group_id,
                   const int bundle_index) {
  return resource.find(kGroupKeyword + std::to_string(bundle_index) + "_" +
                       group_id.Hex()) != std::string::npos;
}

std::string GetOriginalResourceName(const std::string &resource) {
  auto idx = resource.find(kGroupKeyword);
  RAY_CHECK(idx >= 0) << "This isn't a placement group resource " << resource;
  return resource.substr(0, idx);
}

std::string GetDebugStringForBundles(
    const std::vector<std::shared_ptr<const BundleSpecification>> &bundles) {
  std::ostringstream debug_info;
  for (const auto &bundle : bundles) {
    debug_info << "{" << bundle->DebugString() << "},";
  }
  return debug_info.str();
};

}  // namespace ray
