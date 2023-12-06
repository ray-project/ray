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

std::optional<VirtualClusterBundleResourceLabel> VirtualClusterBundleResourceLabel::Parse(
    const std::string &resource) {
  VirtualClusterBundleResourceLabel data;
  std::smatch match_groups;

  // Greedy match.
  // CPU_vc_123_vc_456 is parsed as
  // {.original_resource="CPU_vc_123", .vc_id="456"}
  static const std::regex wildcard_resource_pattern("^(.*?)_vc_([0-9a-f]+)$");
  if (std::regex_match(resource, match_groups, wildcard_resource_pattern) &&
      match_groups.size() == 3) {
    data.original_resource = match_groups[1].str();
    data.vc_id = VirtualClusterID::FromHex(match_groups[2].str());
    return data;
  }
  return {};
}

ResourceRequest VirtualClusterBundleSpec::ComputeResources(
    const rpc::VirtualClusterBundle &message) {
  auto unit_resource = MapFromProtobuf(message.resources());
  return ResourceMapToResourceRequest(unit_resource,
                                      /*requires_object_store_memory=*/false);
}

absl::flat_hash_map<std::string, double>
VirtualClusterBundleSpec::ComputeFormattedBundleResourceLabels(
    const ray::ResourceRequest &request, VirtualClusterID vc_id) {
  absl::flat_hash_map<std::string, double> labels;

  for (auto &resource_id : request.ResourceIds()) {
    auto resource_name = resource_id.Binary();
    auto resource_value = request.Get(resource_id);

    const std::string &wildcard_label =
        FormatVirtualClusterResource(resource_name, vc_id);
    labels[wildcard_label] = resource_value.Double();
  }

  auto bundle_wildcard_label =
      FormatVirtualClusterResource(kVirtualClusterBundle_ResourceLabel, vc_id);
  labels[bundle_wildcard_label] = 1000;

  return labels;
}

std::string VirtualClusterBundleSpec::DebugString() const {
  std::ostringstream stream;
  auto bundle_id = GetVirtualClusterId();
  stream << "bundle with virtual cluster id={" << bundle_id << "}";
  return stream.str();
}

std::string FormatVirtualClusterResource(const std::string &original_resource_name,
                                         const VirtualClusterID &vc_id) {
  std::stringstream os;
  os << original_resource_name << kVirtualClusterKeyword << vc_id.Hex();

  std::string result = os.str();
  RAY_DCHECK(VirtualClusterBundleResourceLabel::Parse(result)->original_resource ==
             original_resource_name)
      << "Generated: " << result << " Original: " << original_resource_name;
  return result;
}

}  // namespace ray
