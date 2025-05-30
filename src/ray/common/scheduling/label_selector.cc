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

#include "absl/strings/match.h"
#include "ray/util/logging.h"

namespace ray {

// Constructor to parse LabelSelector data type from proto.
StatusOr<LabelSelector> LabelSelector::FromProto(
    const google::protobuf::Map<std::string, std::string> &label_selector) {
  LabelSelector selector;
  for (const auto &[key, value] : label_selector) {
    if (key.empty()) {
      return Status::InvalidArgument("Empty label selector key is not supported.");
    }
    RAY_RETURN_NOT_OK(selector.AddConstraint(key, value));
  }
  return selector;
}

Status LabelSelector::AddConstraint(const std::string &key, const std::string &value) {
  RAY_ASSIGN_OR_RETURN(auto parsed, ParseLabelSelectorValue(key, value));
  auto &[op, values] = parsed;

  LabelConstraint constraint(key, op, values);
  AddConstraint(std::move(constraint));
  return Status::OK();
}

StatusOr<std::pair<LabelSelectorOperator, absl::flat_hash_set<std::string>>>
LabelSelector::ParseLabelSelectorValue(const std::string &key, const std::string &value) {
  bool is_negated = false;
  std::string_view val = value;

  if (!val.empty() && val[0] == '!') {
    is_negated = true;
    val.remove_prefix(1);
  }

  absl::flat_hash_set<std::string> values;
  LabelSelectorOperator op;

  if (absl::StartsWith(val, "in(") && val.back() == ')') {
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

    if (values.empty()) {
      return Status::InvalidArgument(
          "No values provided for Label Selector 'in' operator.");
    }

    op = is_negated ? LabelSelectorOperator::LABEL_NOT_IN
                    : LabelSelectorOperator::LABEL_IN;
  } else {
    values.insert(std::string(val));
    op = is_negated ? LabelSelectorOperator::LABEL_NOT_IN
                    : LabelSelectorOperator::LABEL_IN;
  }

  return std::make_pair(op, values);
}

}  // namespace ray
