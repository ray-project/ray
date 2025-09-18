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

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "google/protobuf/map.h"
#include "ray/common/constants.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {

enum class LabelSelectorOperator {
  LABEL_OPERATOR_UNSPECIFIED = 0,
  // This is to support equality or in semantics.
  LABEL_IN = 1,
  // This is to support not equal or not in semantics.
  LABEL_NOT_IN = 2
};

// Defines requirements for a label key and value.
class LabelConstraint {
 public:
  LabelConstraint() = default;

  LabelConstraint(std::string key,
                  LabelSelectorOperator op,
                  absl::flat_hash_set<std::string> values)
      : key_(std::move(key)), op_(op), values_(std::move(values)) {}

  const std::string &GetLabelKey() const { return key_; }

  LabelSelectorOperator GetOperator() const { return op_; }

  const absl::flat_hash_set<std::string> &GetLabelValues() const { return values_; }

 private:
  std::string key_;
  LabelSelectorOperator op_;
  absl::flat_hash_set<std::string> values_;
};

// Label Selector data type. Defines a list of label constraints
// required for scheduling on a node.
class LabelSelector {
 public:
  LabelSelector() = default;

  explicit LabelSelector(
      const google::protobuf::Map<std::string, std::string> &label_selector);

  rpc::LabelSelector ToProto() const;

  void AddConstraint(const std::string &key, const std::string &value);

  void AddConstraint(LabelConstraint constraint) {
    constraints_.push_back(std::move(constraint));
  }

  const std::vector<LabelConstraint> &GetConstraints() const { return constraints_; }

  std::pair<LabelSelectorOperator, absl::flat_hash_set<std::string>>
  ParseLabelSelectorValue(const std::string &key, const std::string &value);

 private:
  std::vector<LabelConstraint> constraints_;
};

inline bool operator==(const LabelConstraint &lhs, const LabelConstraint &rhs) {
  return lhs.GetLabelKey() == rhs.GetLabelKey() &&
         lhs.GetOperator() == rhs.GetOperator() &&
         lhs.GetLabelValues() == rhs.GetLabelValues();
}

inline bool operator==(const LabelSelector &lhs, const LabelSelector &rhs) {
  return lhs.GetConstraints() == rhs.GetConstraints();
}

template <typename H>
H AbslHashValue(H h, const LabelSelector &label_selector) {
  h = H::combine(std::move(h), label_selector.GetConstraints().size());
  for (const auto &constraint : label_selector.GetConstraints()) {
    h = H::combine(std::move(h),
                   constraint.GetLabelKey(),
                   static_cast<int>(constraint.GetOperator()));
    for (const auto &value : constraint.GetLabelValues()) {
      h = H::combine(std::move(h), value);
    }
  }
  return h;
}

inline std::optional<absl::flat_hash_set<std::string>> GetHardNodeAffinityValues(
    const LabelSelector &label_selector) {
  const std::string hard_affinity_key(kLabelKeyNodeID);

  for (const auto &constraint : label_selector.GetConstraints()) {
    if (constraint.GetLabelKey() == hard_affinity_key) {
      if (constraint.GetOperator() == LabelSelectorOperator::LABEL_IN) {
        return constraint.GetLabelValues();
      }
    }
  }
  return std::nullopt;
}

}  // namespace ray
