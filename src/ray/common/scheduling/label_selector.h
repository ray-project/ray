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
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "google/protobuf/map.h"

namespace ray {

enum class LabelSelectorOperator {
  // This is to support equality or in semantics.
  LABEL_IN = 0,
  // This is to support not equal or not in semantics.
  LABEL_NOT_IN = 1
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

}  // namespace ray
