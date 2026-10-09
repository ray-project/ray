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

#include "ray/common/scheduling/label_selector.h"
#include "ray/common/scheduling/placement_group_util.h"
#include "ray/common/scheduling/scheduling_ids.h"

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

    // Set LabelSelector required for scheduling this bundle if specified.
    // Parses string map from proto to LabelSelector data type.
    unit_resource_->SetLabelSelector(LabelSelector(message_->label_selector()));
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
                                         const std::string &group_id_hex,
                                         int64_t bundle_index) {
  std::stringstream os;
  if (bundle_index >= 0) {
    os << original_resource_name << kGroupKeyword << std::to_string(bundle_index) << "_"
       << group_id_hex;
  } else {
    RAY_CHECK(bundle_index == -1) << "Invalid index " << bundle_index;
    os << original_resource_name << kGroupKeyword << group_id_hex;
  }
  std::string result = os.str();
  RAY_DCHECK(GetOriginalResourceName(result) == original_resource_name)
      << "Generated: " << GetOriginalResourceName(result)
      << " Original: " << original_resource_name;
  return result;
}

std::string FormatPlacementGroupResource(const std::string &original_resource_name,
                                         const PlacementGroupID &group_id,
                                         int64_t bundle_index) {
  return FormatPlacementGroupResource(
      original_resource_name, group_id.Hex(), bundle_index);
}

std::string GetOriginalResourceName(const std::string &resource) {
  auto data = ParsePgFormattedResource(
      resource, /*for_wildcard_resource*/ true, /*for_indexed_resource*/ true);
  RAY_CHECK(data) << "This isn't a placement group resource " << resource;
  return data->original_resource;
}

std::string GetOriginalResourceNameFromWildcardResource(const std::string &resource) {
  auto data = ParsePgFormattedResource(
      resource, /*for_wildcard_resource*/ true, /*for_indexed_resource*/ false);
  if (!data) {
    return "";
  } else {
    RAY_CHECK(data->original_resource != "");
    RAY_CHECK(data->bundle_index == -1);
    return data->original_resource;
  }
}

std::string GetDebugStringForBundles(
    const std::vector<std::shared_ptr<const BundleSpecification>> &bundles) {
  std::ostringstream debug_info;
  for (const auto &bundle : bundles) {
    debug_info << "{" << bundle->DebugString() << "},";
  }
  return debug_info.str();
};

std::unordered_map<std::string, double> AddPlacementGroupConstraint(
    const std::unordered_map<std::string, double> &resources,
    const PlacementGroupID &placement_group_id,
    int64_t bundle_index) {
  if (placement_group_id.IsNil()) {
    return resources;
  }

  std::unordered_map<std::string, double> new_resources;
  RAY_CHECK((bundle_index == -1 || bundle_index >= 0))
      << "Invalid bundle index " << bundle_index;
  for (auto iter = resources.begin(); iter != resources.end(); iter++) {
    auto wildcard_name =
        FormatPlacementGroupResource(iter->first, placement_group_id, -1);
    new_resources[wildcard_name] = iter->second;
    if (bundle_index >= 0) {
      auto index_name =
          FormatPlacementGroupResource(iter->first, placement_group_id, bundle_index);
      new_resources[index_name] = iter->second;
    }
  }

  // Note that all nodes that have placement group have a special
  // keyword bundle_group_[pg_id].
  // Always include bundle resource wildcard resources, so that
  // even when the task doesn't require any resource, it can use the placement group.
  auto bundle_key =
      FormatPlacementGroupResource(kBundle_ResourceLabel, placement_group_id, -1);
  new_resources[bundle_key] = 0.001;
  if (bundle_index >= 0) {
    auto bundle_key_with_index = FormatPlacementGroupResource(
        kBundle_ResourceLabel, placement_group_id, bundle_index);
    new_resources[bundle_key_with_index] = 0.001;
  }
  return new_resources;
}

std::unordered_map<std::string, double> AddPlacementGroupConstraint(
    const std::unordered_map<std::string, double> &resources,
    const rpc::SchedulingStrategy &scheduling_strategy) {
  auto placement_group_id = PlacementGroupID::Nil();
  auto bundle_index = -1;
  if (scheduling_strategy.scheduling_strategy_case() ==
      rpc::SchedulingStrategy::SchedulingStrategyCase::
          kPlacementGroupSchedulingStrategy) {
    placement_group_id = PlacementGroupID::FromBinary(
        scheduling_strategy.placement_group_scheduling_strategy().placement_group_id());
    bundle_index = scheduling_strategy.placement_group_scheduling_strategy()
                       .placement_group_bundle_index();
  }
  return AddPlacementGroupConstraint(resources, placement_group_id, bundle_index);
}

std::string GetGroupIDFromResource(const std::string &resource) {
  size_t pg_suffix_len = 2 * PlacementGroupID::Size();
  RAY_CHECK(resource.size() > pg_suffix_len);
  return resource.substr(resource.size() - pg_suffix_len, pg_suffix_len);
}

}  // namespace ray
