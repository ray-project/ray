// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/common/scheduling/placement_group_util.h"

#include <regex>
#include <string>

#include "ray/util/logging.h"

namespace ray {

bool IsCPUOrPlacementGroupCPUResource(ResourceID resource_id) {
  // Check whether the resource is CPU resource or CPU resource inside PG.
  if (resource_id == ResourceID::CPU()) {
    return true;
  }

  auto possible_pg_resource = ParsePgFormattedResource(resource_id.Binary(),
                                                       /*for_wildcard_resource*/ true,
                                                       /*for_indexed_resource*/ true);
  if (possible_pg_resource.has_value() &&
      possible_pg_resource->original_resource == ResourceID::CPU().Binary()) {
    return true;
  }

  return false;
}

std::optional<PgFormattedResourceData> ParsePgFormattedResource(
    const std::string &resource, bool for_wildcard_resource, bool for_indexed_resource) {
  // Check if it is a wildcard pg resource.
  PgFormattedResourceData data;
  std::smatch match_groups;
  RAY_CHECK(for_wildcard_resource || for_indexed_resource)
      << "Either one of for_wildcard_resource or for_indexed_resource must be true";

  if (for_wildcard_resource) {
    static const std::regex wild_card_resource_pattern("^(.*)_group_([0-9a-f]+)$");

    if (std::regex_match(resource, match_groups, wild_card_resource_pattern) &&
        match_groups.size() == 3) {
      data.original_resource = match_groups[1].str();
      data.bundle_index = -1;
      data.group_id = match_groups[2].str();
      return data;
    }
  }

  // Check if it is a regular pg resource.
  if (for_indexed_resource) {
    static const std::regex pg_resource_pattern("^(.+)_group_(\\d+)_([0-9a-zA-Z]+)");
    if (std::regex_match(resource, match_groups, pg_resource_pattern) &&
        match_groups.size() == 4) {
      data.original_resource = match_groups[1].str();
      data.bundle_index = stoi(match_groups[2].str());
      data.group_id = match_groups[3].str();
      return data;
    }
  }

  // If it is not a wildcard or pg formatted resource, return nullopt.
  return std::nullopt;
}

}  // namespace ray
