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
#include "ray/common/grpc_util.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {

// Defines requirements for a label key and value.
class LabelConstraint : public MessageWrapper<rpc::LabelConstraint> {
 public:
  LabelConstraint() : MessageWrapper(std::make_shared<rpc::LabelConstraint>()) {}

  explicit LabelConstraint(const rpc::LabelConstraint &proto)
        : MessageWrapper(std::make_shared<rpc::LabelConstraint>()) {
    GetMutableMessage().CopyFrom(proto);
  }

  // Copy constructor
  LabelConstraint(const LabelConstraint &other)
    : MessageWrapper(other.message_ ? std::make_shared<rpc::LabelConstraint>(*other.message_)
                                    : std::make_shared<rpc::LabelConstraint>()) {}

  LabelConstraint(const std::string &key,
                  rpc::LabelSelectorOperator op,
                  const absl::flat_hash_set<std::string> &values)
            : MessageWrapper(std::make_shared<rpc::LabelConstraint>()) {
    SetLabelKey(key);
    SetOperator(op);
    SetLabelValues(values);
  }

  const std::string &GetLabelKey() const { return GetMessage().label_key(); }

  rpc::LabelSelectorOperator GetOperator() const { return GetMessage().operator_(); }

  absl::flat_hash_set<std::string> GetLabelValues() const {
    absl::flat_hash_set<std::string> values;
    for (const auto &val : GetMessage().label_values()) {
      values.insert(val);
    }
    return values;
  }

  void SetLabelKey(const std::string &key) { GetMutableMessage().set_label_key(key); }

  void SetOperator(rpc::LabelSelectorOperator op) {
    GetMutableMessage().set_operator_(op);
  }

  void SetLabelValues(const absl::flat_hash_set<std::string> &values) {
    auto *mutable_values = GetMutableMessage().mutable_label_values();
    mutable_values->Clear();
    for (const auto &v : values) {
      *mutable_values->Add() = v;
    }
  }

  // Copy assignment
  LabelConstraint &operator=(const LabelConstraint &other) {
    if (this != &other) {
        message_ = other.message_ ? std::make_shared<rpc::LabelConstraint>(*other.message_)
                                : std::make_shared<rpc::LabelConstraint>();
    }
    return *this;
  }
};

// Label Selector data type. Defines a list of label constraints
// required for scheduling on a node.
class LabelSelector : public MessageWrapper<rpc::LabelSelector> {
 public:
  LabelSelector() : MessageWrapper(std::make_shared<rpc::LabelSelector>()) {}

  LabelSelector(const LabelSelector &other)
    : MessageWrapper(other.message_ ? std::make_shared<rpc::LabelSelector>(*other.message_)
                                    : std::make_shared<rpc::LabelSelector>()) {}

  // Constructor that initializes the LabelSelector from a map of label key-value pairs
  LabelSelector(const std::unordered_map<std::string, std::string> &label_selector)
    : MessageWrapper(std::make_shared<rpc::LabelSelector>()) {
    for (const auto &pair : label_selector) {
      const std::string &key = pair.first;
      const std::string &value = pair.second;

      if (key.empty()) {
        throw std::invalid_argument("Label key must be a non-empty string.");
      }

      if (value.empty()) {
        throw std::invalid_argument("Label value must be a non-empty string.");
      }

      // Directly add to the protobuf!
      AddConstraint(key, value);
    }
  }

  bool IsInitialized() const {
    return message_ != nullptr;
  }

  // Creates a LabelConstraint from a key-value string and adds it to the LabelSelector
  void AddConstraint(const std::string &key, const std::string &value) {
    auto [op, values] = ParseLabelSelectorValue(key, value);
    LabelConstraint constraint(key, op, values);
    AddConstraint(constraint);
  }

  void AddConstraint(const LabelConstraint &constraint) {
    // Add to the underlying protobuf message
    auto *new_constraint = GetMutableMessage().add_label_constraints();
    *new_constraint = constraint.GetMessage();
  }

  std::vector<LabelConstraint> GetConstraints() const {
    std::vector<LabelConstraint> out;
    for (const auto &c : GetMessage().label_constraints()) {
      out.emplace_back(c);
    }
    return out;
  }

  std::pair<rpc::LabelSelectorOperator, absl::flat_hash_set<std::string>>
  ParseLabelSelectorValue(const std::string &key, const std::string &value) {
    bool is_negated = false;
    std::string val = value;
    if (!val.empty() && val[0] == '!') {
        is_negated = true;
        val = val.substr(1);
    }

    absl::flat_hash_set<std::string> values;
    rpc::LabelSelectorOperator op;

    if (val.rfind("in(", 0) == 0 && val.back() == ')') {
        val = val.substr(3, val.size() - 4);
        std::vector<std::string> tokens;
        size_t pos;
        while ((pos = val.find(',')) != std::string::npos) {
            tokens.push_back(val.substr(0, pos));
            val.erase(0, pos + 1);
        }
        if (!val.empty()) tokens.push_back(val);

        if (tokens.empty()) {
            throw std::invalid_argument("No values provided for key '" + key + "'");
        }

        values.insert(tokens.begin(), tokens.end());
        op = is_negated ? rpc::LabelSelectorOperator::NOT_IN
                        : rpc::LabelSelectorOperator::IN;
    } else {
        values.insert(val);
        op = is_negated ? rpc::LabelSelectorOperator::NOT_IN
                        : rpc::LabelSelectorOperator::IN;
    }

    return {op, values};
  }

  LabelSelector &operator=(const LabelSelector &other) {
    if (this != &other) {
        message_ = other.message_ ? std::make_shared<rpc::LabelSelector>(*other.message_)
                                : std::make_shared<rpc::LabelSelector>();
    }
    return *this;
  }
};

}  // namespace ray
