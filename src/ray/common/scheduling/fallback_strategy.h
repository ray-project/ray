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

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/hash/hash.h"
#include "ray/common/scheduling/label_selector.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {

/// This struct holds all the information for a single fallback option in the fallback
/// strategy list. It is designed to be extensible.
struct FallbackOption {
  FallbackOption() = default;

  LabelSelector label_selector;
  // To add a new option, add a new field here.

  explicit FallbackOption(const rpc::LabelSelector &proto_selector)
      : label_selector(proto_selector) {}

  explicit FallbackOption(LabelSelector selector) : label_selector(std::move(selector)) {}

  // Return a FallbackOption proto message.
  void ToProto(rpc::FallbackOption *proto) const;
};

inline bool operator==(const FallbackOption &lhs, const FallbackOption &rhs) {
  return lhs.label_selector == rhs.label_selector;
}

template <typename H>
H AbslHashValue(H h, const FallbackOption &opts) {
  return H::combine(std::move(h), opts.label_selector);
}

// Parse FallbackStrategy from FallbackOption vector.
std::shared_ptr<std::vector<FallbackOption>> ParseFallbackStrategy(
    const google::protobuf::RepeatedPtrField<rpc::FallbackOption> &strategy_proto_list);

// Return a FallbackStrategy message, which is a repeated FallbackOption proto.
rpc::FallbackStrategy SerializeFallbackStrategy(
    const std::vector<FallbackOption> &strategy_list);

}  // namespace ray
