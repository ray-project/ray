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

#include "ray/common/virtual_cluster_resource_label.h"

#include <regex>

namespace ray {

namespace {
const static std::string kVirtualClusterKeyword = "_vc_";
}

std::optional<VirtualClusterResourceLabel> VirtualClusterResourceLabel::Parse(
    const std::string &resource) {
  VirtualClusterResourceLabel data;
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

std::string VirtualClusterResourceLabel::Format() {
  return original_resource + kVirtualClusterKeyword + vc_id.Hex();
}

}  // namespace ray
