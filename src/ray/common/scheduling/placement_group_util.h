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

#pragma once

#include <optional>
#include <string>

#include "ray/common/scheduling/scheduling_ids.h"

namespace ray {

using scheduling::ResourceID;

struct PgFormattedResourceData {
  std::string original_resource;
  /// -1 if it is a wildcard resource.
  int64_t bundle_index;
  std::string group_id;
};

/// Return whether the resource specified by the resource_id is a CPU resource
/// or CPU resource inside a placement group.
bool IsCPUOrPlacementGroupCPUResource(ResourceID resource_id);

/// Parse the given resource and get the pg related information.
///
/// \param resource name of the resource.
/// \param for_wildcard_resource if true, it parses wildcard pg resources.
/// E.g., [resource]_group_[pg_id]
/// \param for_indexed_resource if true, it parses indexed pg resources.
/// E.g., [resource]_group_[index]_[pg_id]
/// \return nullopt if it is not a pg resource. Otherwise, it returns the
/// struct with pg information parsed from the resource.
/// If a returned bundle index is -1, it means the resource is the wildcard resource.
std::optional<PgFormattedResourceData> ParsePgFormattedResource(
    const std::string &resource, bool for_wildcard_resource, bool for_indexed_resource);

}  // namespace ray
