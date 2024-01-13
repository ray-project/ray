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

#include "ray/common/virtual_cluster_node_spec.h"

#include <regex>

namespace ray {

ResourceRequest VirtualClusterNodeSpec::ComputeResources(
    const rpc::VirtualClusterNode &message) {
  auto unit_resource = MapFromProtobuf(message.resources());
  return ResourceMapToResourceRequest(unit_resource,
                                      /*requires_object_store_memory=*/false);
}

absl::flat_hash_map<std::string, double>
VirtualClusterNodeSpec::ComputeFormattedResourceLabels(
    const ray::ResourceRequest &request, VirtualClusterID vc_id) {
  absl::flat_hash_map<std::string, double> labels;

  for (auto &resource_id : request.ResourceIds()) {
    auto resource_name = resource_id.Binary();
    auto resource_value = request.Get(resource_id);

    VirtualClusterResourceLabel wildcard_label{.original_resource = resource_name,
                                               .vc_id = vc_id};
    labels[wildcard_label.Format()] = resource_value.Double();
  }

  VirtualClusterResourceLabel bundle_wildcard_label{
      .original_resource = kVirtualClusterBundle_ResourceLabel, .vc_id = vc_id};
  labels[bundle_wildcard_label.Format()] = 1000;

  return labels;
}

std::string VirtualClusterNodeSpec::DebugString() const {
  std::ostringstream stream;
  auto bundle_id = GetVirtualClusterId();
  stream << "bundle with virtual cluster id={" << bundle_id << "}";
  return stream.str();
}
}  // namespace ray
