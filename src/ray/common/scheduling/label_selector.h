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

#include <algorithm>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/strings/str_cat.h"
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

  // Constructor to parse LabelConstraint data type from proto message.
  explicit LabelConstraint(const rpc::LabelSelectorConstraint &proto)
      : key_(proto.label_key()),
        op_(static_cast<LabelSelectorOperator>(proto.operator_())) {
    for (const auto &value : proto.label_values()) {
      values_.insert(value);
    }
  }

  const std::string &GetLabelKey() const { return key_; }

  LabelSelectorOperator GetOperator() const { return op_; }

  const absl::flat_hash_set<std::string> &GetLabelValues() const { return values_; }

  /// Return the string representation of the label constraint.
  /// For example, "ray.io/node-id:!in(123,456)".
  std::string ToString() const {
    std::string result = absl::StrCat(key_, ":");
    std::vector<std::string> values(values_.begin(), values_.end());
    std::sort(values.begin(), values.end());
    if (op_ == LabelSelectorOperator::LABEL_IN) {
      if (values.size() == 1) {
        absl::StrAppend(&result, values[0]);
      } else {
        absl::StrAppend(&result, "in(");
        bool first = true;
        for (const std::string &value : values) {
          if (first) {
            first = false;
          } else {
            absl::StrAppend(&result, ",");
          }
          absl::StrAppend(&result, value);
        }
        absl::StrAppend(&result, ")");
      }
    } else if (op_ == LabelSelectorOperator::LABEL_NOT_IN) {
      if (values.size() == 1) {
        absl::StrAppend(&result, "!", values[0]);
      } else {
        absl::StrAppend(&result, "!in(");
        bool first = false;
        for (const std::string &value : values) {
          if (first) {
            first = false;
          } else {
            absl::StrAppend(&result, ",");
          }
          absl::StrAppend(&result, value);
        }
        absl::StrAppend(&result, ")");
      }
    }
    return result;
  }

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

  // Constructor for parsing user-input label selector string maps to LabelSelector class.
  template <typename MapType>
  explicit LabelSelector(const MapType &label_selector) {
    // Label selector keys and values are validated before construction in
    // `prepare_label_selector`.
    // https://github.com/ray-project/ray/blob/feb1c6180655b69fc64c5e0c25cc56cbe96e0b26/python/ray/_raylet.pyx#L782C1-L784C70
    for (const auto &[key, value] : label_selector) {
      AddConstraint(key, value);
    }
  }

  // Constructor to parse LabelSelector data type from proto message.
  explicit LabelSelector(const rpc::LabelSelector &proto) {
    constraints_.reserve(proto.label_constraints_size());
    for (const auto &proto_constraint : proto.label_constraints()) {
      constraints_.emplace_back(proto_constraint);
    }
  }

  // Convert LabelSelector object to rpc::LabelSelector proto message.
  void ToProto(rpc::LabelSelector *proto) const;

  // Convert the LabelSelector object back into a string map.
  google::protobuf::Map<std::string, std::string> ToStringMap() const;

  void AddConstraint(const std::string &key, const std::string &value);

  void AddConstraint(LabelConstraint constraint) {
    constraints_.push_back(std::move(constraint));
  }

  const std::vector<LabelConstraint> &GetConstraints() const { return constraints_; }

  std::string DebugString() const;

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
H AbslHashValue(H h, const LabelConstraint &label_constraint) {
  h = H::combine(std::move(h),
                 label_constraint.GetLabelKey(),
                 static_cast<int>(label_constraint.GetOperator()));
  const auto &values = label_constraint.GetLabelValues();
  h = H::combine(std::move(h), values.size());
  h = H::combine_unordered(std::move(h), values.begin(), values.end());
  return h;
}

template <typename H>
H AbslHashValue(H h, const LabelSelector &label_selector) {
  h = H::combine(std::move(h), label_selector.GetConstraints().size());
  for (const auto &constraint : label_selector.GetConstraints()) {
    h = H::combine(std::move(h), constraint);
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
