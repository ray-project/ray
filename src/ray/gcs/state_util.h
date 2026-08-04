// Copyright 2023 The Ray Authors.
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
#pragma once

#include <functional>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "absl/hash/hash.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

struct ResourceDemandKey {
  google::protobuf::Map<std::string, double> shape;
  std::vector<rpc::LabelSelector> label_selectors;
};

inline bool operator==(const ResourceDemandKey &lhs, const ResourceDemandKey &rhs) {
  if (lhs.shape.size() != rhs.shape.size()) {
    return false;
  }
  for (const auto &entry : lhs.shape) {
    auto it = rhs.shape.find(entry.first);
    if (it == rhs.shape.end() || it->second != entry.second) {
      return false;
    }
  }

  if (lhs.label_selectors.size() != rhs.label_selectors.size()) {
    return false;
  }
  for (size_t i = 0; i < lhs.label_selectors.size(); ++i) {
    if (lhs.label_selectors[i].SerializeAsString() !=
        rhs.label_selectors[i].SerializeAsString()) {
      return false;
    }
  }
  return true;
}

template <typename H>
H AbslHashValue(H h, const ResourceDemandKey &key);

/// Aggregate nodes' pending task info.
///
/// \param resources_data A node's pending task info (by shape).
/// \param aggregate_load[out] The aggregate pending task info (across the cluster).
void FillAggregateLoad(
    const rpc::ResourcesData &resources_data,
    absl::flat_hash_map<ResourceDemandKey, rpc::ResourceDemand> *aggregate_load);

}  // namespace gcs
}  // namespace ray

template <typename H>
H ray::gcs::AbslHashValue(H h, const ray::gcs::ResourceDemandKey &key) {
  h = H::combine(std::move(h), key.shape);
  for (const auto &selector : key.label_selectors) {
    h = H::combine(std::move(h), selector.SerializeAsString());
  }
  return h;
}

namespace std {
template <>
struct hash<google::protobuf::Map<std::string, double>> {
  size_t operator()(google::protobuf::Map<std::string, double> const &k) const {
    size_t seed = k.size();
    for (auto &elem : k) {
      seed ^= std::hash<std::string>()(elem.first);
      seed ^= std::hash<double>()(elem.second);
    }
    return seed;
  }
};

template <>
struct equal_to<google::protobuf::Map<std::string, double>> {
  bool operator()(const google::protobuf::Map<std::string, double> &left,
                  const google::protobuf::Map<std::string, double> &right) const {
    if (left.size() != right.size()) {
      return false;
    }
    for (const auto &entry : left) {
      auto iter = right.find(entry.first);
      if (iter == right.end() || iter->second != entry.second) {
        return false;
      }
    }
    return true;
  }
};

template <>
struct hash<ray::gcs::ResourceDemandKey> {
  size_t operator()(const ray::gcs::ResourceDemandKey &k) const {
    return absl::Hash<ray::gcs::ResourceDemandKey>{}(k);
  }
};
}  // namespace std
