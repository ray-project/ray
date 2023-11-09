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

#include "ray/common/virtual_cluster_bundle_spec.h"

#include <regex>

namespace ray {

std::optional<VirtualClusterBundleResourceLabel>
VirtualClusterBundleResourceLabel::ParseFromIndexed(const std::string &resource) {
  // Check if it is a wildcard pg resource.
  VirtualClusterBundleResourceLabel data;
  std::smatch match_groups;

  static const std::regex vc_resource_pattern("^(.+)_vc_(\\d+)_([0-9a-zA-Z]+)$");
  if (std::regex_match(resource, match_groups, vc_resource_pattern) &&
      match_groups.size() == 4) {
    data.original_resource = match_groups[1].str();
    data.vc_id = VirtualClusterID::FromHex(match_groups[3].str());
    data.bundle_index = stoi(match_groups[2].str());
    return data;
  }
  return {};
}

std::optional<VirtualClusterBundleResourceLabel>
VirtualClusterBundleResourceLabel::ParseFromWildcard(const std::string &resource) {
  // Check if it is a wildcard pg resource.
  VirtualClusterBundleResourceLabel data;
  std::smatch match_groups;

  static const std::regex wildcard_resource_pattern("^(.*)_vc_([0-9a-f]+)$");
  if (std::regex_match(resource, match_groups, wildcard_resource_pattern) &&
      match_groups.size() == 3) {
    data.original_resource = match_groups[1].str();
    data.vc_id = VirtualClusterID::FromHex(match_groups[2].str());
    return data;
  }
  return {};
}

std::optional<VirtualClusterBundleResourceLabel>
VirtualClusterBundleResourceLabel::ParseFromEither(const std::string &resource) {
  auto maybe_indexed = ParseFromIndexed(resource);
  if (maybe_indexed.has_value()) {
    return maybe_indexed;
  }
  return ParseFromWildcard(resource);
}

ResourceRequest VirtualClusterBundleSpec::ComputeResources(
    const rpc::VirtualClusterBundle &message) {
  auto unit_resource = MapFromProtobuf(message.resources());
  return ResourceMapToResourceRequest(unit_resource,
                                      /*requires_object_store_memory=*/false);
}

absl::flat_hash_map<std::string, double>
VirtualClusterBundleSpec::ComputeFormattedBundleResourceLabels(
    const ray::ResourceRequest &request, VirtualClusterBundleID vc_bundle_id) {
  absl::flat_hash_map<std::string, double> labels;

  for (auto &resource_id : request.ResourceIds()) {
    auto resource_name = resource_id.Binary();
    auto resource_value = request.Get(resource_id);

    /// With bundle index (e.g., CPU_vc_i_vchex).
    const std::string &resource_label =
        FormatVirtualClusterResource(resource_name, vc_bundle_id);
    labels[resource_label] = resource_value.Double();

    /// Without bundle index (e.g., CPU_group_zzz).
    const std::string &wildcard_label =
        FormatVirtualClusterResource(resource_name, vc_bundle_id.first);
    labels[wildcard_label] = resource_value.Double();
  }
  auto bundle_label =
      FormatVirtualClusterResource(kVirtualClusterBundle_ResourceLabel, vc_bundle_id);
  labels[bundle_label] = 1000;

  auto bundle_wildcard_label = FormatVirtualClusterResource(
      kVirtualClusterBundle_ResourceLabel, vc_bundle_id.first);
  labels[bundle_wildcard_label] = 1000;

  return labels;
}

std::string VirtualClusterBundleSpec::DebugString() const {
  std::ostringstream stream;
  auto bundle_id = GetBundleId();
  stream << "virtual cluster id={" << bundle_id.first << "}, bundle index={"
         << bundle_id.second << "}";
  return stream.str();
}

std::string FormatVirtualClusterResource(const std::string &original_resource_name,
                                         const VirtualClusterBundleID &vc_bundle_id) {
  std::stringstream os;

  const auto &[vc_id, bundle_index] = vc_bundle_id;
  os << original_resource_name << kVirtualClusterKeyword << std::to_string(bundle_index)
     << "_" << vc_id.Hex();
  std::string result = os.str();
  RAY_DCHECK(
      VirtualClusterBundleResourceLabel::ParseFromIndexed(result)->original_resource ==
      original_resource_name)
      << "Generated: " << result << " Original: " << original_resource_name;
  return result;
}

std::string FormatVirtualClusterResource(const std::string &original_resource_name,
                                         const VirtualClusterID &vc_id) {
  std::stringstream os;
  os << original_resource_name << kVirtualClusterKeyword << vc_id.Hex();

  std::string result = os.str();
  RAY_DCHECK(
      VirtualClusterBundleResourceLabel::ParseFromWildcard(result)->original_resource ==
      original_resource_name)
      << "Generated: " << result << " Original: " << original_resource_name;
  return result;
}

}  // namespace ray
