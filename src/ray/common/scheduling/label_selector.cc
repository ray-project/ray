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

#include "ray/common/scheduling/label_selector.h"

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/match.h"
#include "absl/strings/str_join.h"
#include "ray/util/logging.h"

namespace ray {

void LabelSelector::ToProto(rpc::LabelSelector *proto) const {
  RAY_CHECK(proto != nullptr);
  proto->clear_label_constraints();

  for (const auto &constraint : constraints_) {
    auto *proto_constraint = proto->add_label_constraints();
    proto_constraint->set_label_key(constraint.GetLabelKey());
    proto_constraint->set_operator_(
        static_cast<rpc::LabelSelectorOperator>(constraint.GetOperator()));
    for (const auto &val : constraint.GetLabelValues()) {
      proto_constraint->add_label_values(val);
    }
  }
}

namespace {

// Convert one constraint back into the string form the parser accepts.
std::string ConstraintToString(const LabelConstraint &constraint) {
  const auto &values = constraint.GetLabelValues();

  // Sort the values for deterministic output.
  std::vector<std::string> sorted_values(values.begin(), values.end());
  std::sort(sorted_values.begin(), sorted_values.end());

  switch (constraint.GetOperator()) {
  case LabelSelectorOperator::LABEL_IN:
    if (values.size() == 1) {
      return sorted_values[0];
    }
    return "in(" + absl::StrJoin(sorted_values, ",") + ")";
  case LabelSelectorOperator::LABEL_NOT_IN:
    if (values.size() == 1) {
      return "!" + sorted_values[0];
    }
    return "!in(" + absl::StrJoin(sorted_values, ",") + ")";
  case LabelSelectorOperator::LABEL_EXISTS:
    return "exists()";
  case LabelSelectorOperator::LABEL_DOES_NOT_EXIST:
    return "!exists()";
  default:
    return "";
  }
}

}  // namespace

google::protobuf::Map<std::string, std::string> LabelSelector::ToStringMap() const {
  google::protobuf::Map<std::string, std::string> string_map;

  for (const auto &constraint : constraints_) {
    std::string value_str = ConstraintToString(constraint);
    if (value_str.empty()) {
      continue;
    }
    // Join several constraints on the same key with "," so none are dropped.
    auto it = string_map.find(constraint.GetLabelKey());
    if (it == string_map.end()) {
      string_map[constraint.GetLabelKey()] = std::move(value_str);
    } else {
      it->second += "," + value_str;
    }
  }
  return string_map;
}

void LabelSelector::AddConstraint(const std::string &key, const std::string &value) {
  for (const auto &expression : SplitLabelSelectorValue(value)) {
    auto [op, values] = ParseLabelSelectorValue(key, expression);
    AddConstraint(LabelConstraint(key, op, std::move(values)));
  }
}

std::vector<std::string> LabelSelector::SplitLabelSelectorValue(
    const std::string &value) {
  std::vector<std::string> expressions;
  int depth = 0;
  size_t start = 0;
  for (size_t i = 0; i < value.size(); ++i) {
    if (value[i] == '(') {
      ++depth;
    } else if (value[i] == ')') {
      --depth;
    } else if (value[i] == ',' && depth == 0) {
      expressions.push_back(value.substr(start, i - start));
      start = i + 1;
    }
  }
  expressions.push_back(value.substr(start));
  return expressions;
}

void LabelSelector::AddConstraint(LabelConstraint constraint) {
  // Avoid adding duplicate constraints.
  if (std::find(constraints_.begin(), constraints_.end(), constraint) !=
      constraints_.end()) {
    return;
  }

  constraints_.push_back(std::move(constraint));
}

std::pair<LabelSelectorOperator, absl::flat_hash_set<std::string>>
LabelSelector::ParseLabelSelectorValue(const std::string &key, const std::string &value) {
  bool is_negated = false;
  std::string_view val = value;

  if (!val.empty() && val[0] == '!') {
    is_negated = true;
    val.remove_prefix(1);
  }

  absl::flat_hash_set<std::string> values;
  LabelSelectorOperator op;

  if (val == "exists()") {
    op = is_negated ? LabelSelectorOperator::LABEL_DOES_NOT_EXIST
                    : LabelSelectorOperator::LABEL_EXISTS;
  } else if (absl::StartsWith(val, "in(") && val.back() == ')') {
    val.remove_prefix(3);  // Remove "in("
    val.remove_suffix(1);  // Remove ')'

    while (!val.empty()) {
      // Parse each token in the LabelSelector value.
      size_t pos = val.find(',');
      std::string_view token = (pos == std::string_view::npos) ? val : val.substr(0, pos);
      values.insert(std::string(token));
      if (pos == std::string_view::npos) break;
      val.remove_prefix(pos + 1);
    }
    op = is_negated ? LabelSelectorOperator::LABEL_NOT_IN
                    : LabelSelectorOperator::LABEL_IN;
  } else {
    values.insert(std::string(val));
    op = is_negated ? LabelSelectorOperator::LABEL_NOT_IN
                    : LabelSelectorOperator::LABEL_IN;
  }

  return {op, values};
}

std::string LabelSelector::DebugString() const {
  std::stringstream ss;
  ss << "{";
  for (size_t i = 0; i < constraints_.size(); ++i) {
    const auto &constraint = constraints_[i];
    ss << "'" << constraint.GetLabelKey() << "': ";

    ss << LabelSelectorOperatorToString(constraint.GetOperator());

    ss << " (";
    bool first = true;
    for (const auto &val : constraint.GetLabelValues()) {
      if (!first) {
        ss << ", ";
      }
      ss << "'" << val << "'";
      first = false;
    }
    ss << ")";

    if (i < constraints_.size() - 1) {
      ss << ", ";
    }
  }
  ss << "}";
  return ss.str();
}

}  // namespace ray
