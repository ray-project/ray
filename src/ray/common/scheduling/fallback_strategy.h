// Copyright 2025 The Ray Authors.
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

#include <string>
#include <unordered_map>
#include <utility>

#include "absl/hash/hash.h"
#include "ray/common/scheduling/label_selector.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {

/// This struct holds all options for a single fallback scheduling strategy.
/// It is designed to be extensible.
struct FallbackStrategyOptions {
  FallbackStrategyOptions() = default;

  LabelSelector label_selector;
  // To add a new option, add a new field here.

  explicit FallbackStrategyOptions(const rpc::LabelSelector &proto_selector)
      : label_selector(proto_selector) {}

  explicit FallbackStrategyOptions(LabelSelector selector)
      : label_selector(std::move(selector)) {}
};

inline bool operator==(const FallbackStrategyOptions &lhs,
                       const FallbackStrategyOptions &rhs) {
  return lhs.label_selector == rhs.label_selector;
}

template <typename H>
H AbslHashValue(H h, const FallbackStrategyOptions &opts) {
  return H::combine(std::move(h), opts.label_selector);
}

}  // namespace ray
