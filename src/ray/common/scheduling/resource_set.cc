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

#include "ray/util/logging.h"

namespace ray {

ResourceSet::ResourceSet(
    const absl::flat_hash_map<std::string, FixedPoint> &resource_map) {
  for (auto const &[name, quantity] : resource_map) {
    Set(ResourceID(name), quantity);
  }
}

ResourceSet::ResourceSet(
    const absl::flat_hash_map<ResourceID, FixedPoint> &resource_map) {
  for (auto const &[id, quantity] : resource_map) {
    Set(id, quantity);
  }
}

ResourceSet::ResourceSet(const absl::flat_hash_map<ResourceID, double> &resource_map) {
  for (auto const &[id, quantity] : resource_map) {
    Set(id, FixedPoint(quantity));
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

ResourceSet ResourceSet::operator+(const ResourceSet &other) const {
  ResourceSet res = *this;
  res += other;
  return res;
}

ResourceSet ResourceSet::operator-(const ResourceSet &other) const {
  ResourceSet res = *this;
  res -= other;
  return res;
}

ResourceSet &ResourceSet::operator+=(const ResourceSet &other) {
  for (auto &entry : other.resources_) {
    auto it = resources_.find(entry.first);
    if (it != resources_.end()) {
      it->second += entry.second;
      if (it->second == 0) {
        resources_.erase(it);
      }
    } else {
      resources_.emplace(entry.first, entry.second);
    }
  }
  return *this;
}

ResourceSet &ResourceSet::operator-=(const ResourceSet &other) {
  for (auto &entry : other.resources_) {
    auto it = resources_.find(entry.first);
    if (it != resources_.end()) {
      it->second -= entry.second;
      if (it->second == 0) {
        resources_.erase(it);
      }
    } else {
      resources_.emplace(entry.first, -entry.second);
    }
  }
  return *this;
}

bool ResourceSet::operator<=(const ResourceSet &other) const {
  // Check all resources that exist in this.
  for (auto &entry : resources_) {
    auto &this_value = entry.second;
    auto other_value = FixedPoint(0);
    auto it = other.resources_.find(entry.first);
    if (it != other.resources_.end()) {
      other_value = it->second;
    }
    if (this_value > other_value) {
      return false;
    }
  }
  // Check all resources that exist in other, but not in this.
  for (auto &entry : other.resources_) {
    if (!resources_.contains(entry.first)) {
      if (entry.second < 0) {
        return false;
      }
    }
  }
  return true;
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

ResourceSet &ResourceSet::Set(ResourceID resource_id, FixedPoint value) {
  if (value == 0) {
    resources_.erase(resource_id);
  } else {
    resources_[resource_id] = value;
  }
  return *this;
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
    buffer << id.Binary() << ": " << quantity.Double();
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

NodeResourceSet::NodeResourceSet(
    const absl::flat_hash_map<std::string, double> &resource_map) {
  for (auto const &[name, quantity] : resource_map) {
    Set(ResourceID(name), FixedPoint(quantity));
  }
}

NodeResourceSet::NodeResourceSet(
    const absl::flat_hash_map<ResourceID, double> &resource_map) {
  for (auto const &[id, quantity] : resource_map) {
    Set(id, FixedPoint(quantity));
  }
}

NodeResourceSet::NodeResourceSet(
    const absl::flat_hash_map<ResourceID, FixedPoint> &resource_map) {
  for (auto const &[id, quantity] : resource_map) {
    Set(id, quantity);
  }
}

NodeResourceSet &NodeResourceSet::Set(ResourceID resource_id, FixedPoint value) {
  if (value == ResourceDefaultValue(resource_id)) {
    resources_.erase(resource_id);
  } else {
    resources_[resource_id] = value;
  }
  return *this;
}

FixedPoint NodeResourceSet::Get(ResourceID resource_id) const {
  auto it = resources_.find(resource_id);
  if (it == resources_.end()) {
    return ResourceDefaultValue(resource_id);
  } else {
    return it->second;
  }
}

bool NodeResourceSet::Has(ResourceID resource_id) const { return Get(resource_id) != 0; }

NodeResourceSet &NodeResourceSet::operator-=(const ResourceSet &other) {
  for (auto &entry : other.Resources()) {
    Set(entry.first, Get(entry.first) - entry.second);
  }
  return *this;
}

bool NodeResourceSet::operator>=(const ResourceSet &other) const {
  for (auto &entry : other.Resources()) {
    if (Get(entry.first) < entry.second) {
      return false;
    }
  }
  return true;
}

bool NodeResourceSet::operator==(const NodeResourceSet &other) const {
  return this->resources_ == other.resources_;
}

FixedPoint NodeResourceSet::ResourceDefaultValue(ResourceID resource_id) const {
  if (resource_id.IsImplicitResource()) {
    return FixedPoint(1);
  } else {
    return FixedPoint(0);
  }
}

absl::flat_hash_map<std::string, double> NodeResourceSet::GetResourceMap() const {
  absl::flat_hash_map<std::string, double> result;
  for (const auto &[id, quantity] : resources_) {
    result[id.Binary()] = quantity.Double();
  }
  return result;
};

void NodeResourceSet::RemoveNegative() {
  for (auto it = resources_.begin(); it != resources_.end();) {
    if (it->second < 0) {
      resources_.erase(it++);
    } else {
      it++;
    }
  }
}

std::set<ResourceID> NodeResourceSet::ExplicitResourceIds() const {
  std::set<ResourceID> result;
  for (const auto &[id, quantity] : resources_) {
    if (!id.IsImplicitResource()) {
      result.emplace(id);
    }
  }
  return result;
}

std::string NodeResourceSet::DebugString() const {
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

}  // namespace ray
