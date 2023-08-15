// Copyright 2019-2021 The Ray Authors.
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

#include "ray/common/scheduling/resource_set.h"

#include <cmath>
#include <sstream>

#include "absl/container/flat_hash_map.h"
#include "ray/util/logging.h"

namespace ray {

ResourceSet::ResourceSet() {}

ResourceSet::ResourceSet(
    const absl::flat_hash_map<std::string, FixedPoint> &resource_map) {
  for (auto const &[name, quantity] : resource_map) {
    Set(ResourceID(name), quantity);
  }
}

ResourceSet::ResourceSet(const absl::flat_hash_map<std::string, double> &resource_map) {
  for (auto const &[name, quantity] : resource_map) {
    Set(ResourceID(name), FixedPoint(quantity));
  }
}

bool ResourceSet::operator==(const ResourceSet &other) const {
  return this->resources_ == other.resources_;
}

bool ResourceSet::IsEmpty() const { return resources_.empty(); }

FixedPoint ResourceSet::Get(ResourceID resource_id) const {
  auto it = resources_.find(resource_id);
  if (it == resources_.end()) {
    return FixedPoint(0);
  } else {
    return it->second;
  }
}

void ResourceSet::Set(ResourceID resource_id, FixedPoint value) {
  if (value == 0) {
    resources_.erase(resource_id);
  } else {
    resources_[resource_id] = value;
  }
}

const std::string ResourceSet::DebugString() const {
  std::stringstream buffer;
  buffer << "{";
  bool first = true;
  for (const auto &[id, quantity] : resources_) {
    if (!first) {
      buffer << ", ";
    }
    first = false;
    buffer << id.Binary() << ": " << quantity;
  }
  buffer << "}";
  return buffer.str();
}

std::unordered_map<std::string, double> ResourceSet::GetResourceUnorderedMap() const {
  std::unordered_map<std::string, double> result;
  for (const auto &[id, quantity] : resources_) {
    result[id.Binary()] = quantity.Double();
  }
  return result;
};

absl::flat_hash_map<std::string, double> ResourceSet::GetResourceMap() const {
  absl::flat_hash_map<std::string, double> result;
  for (const auto &[id, quantity] : resources_) {
    result[id.Binary()] = quantity.Double();
  }
  return result;
};

}  // namespace ray
