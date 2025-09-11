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

#include <string>
#include <utility>

#include "absl/strings/match.h"

namespace ray {

// Constructor to parse LabelSelector data type from proto.
LabelSelector::LabelSelector(
    const google::protobuf::Map<std::string, std::string> &label_selector) {
  // Label selector keys and values are validated before construction in
  // `prepare_label_selector`.
  // https://github.com/ray-project/ray/blob/feb1c6180655b69fc64c5e0c25cc56cbe96e0b26/python/ray/_raylet.pyx#L782C1-L784C70
  for (const auto &[key, value] : label_selector) {
    AddConstraint(key, value);
  }
}

rpc::LabelSelector LabelSelector::ToProto() const {
  rpc::LabelSelector result;
  for (const auto &constraint : constraints_) {
    auto *proto_constraint = result.add_label_constraints();
    proto_constraint->set_label_key(constraint.GetLabelKey());
    proto_constraint->set_operator_(
        static_cast<rpc::LabelSelectorOperator>(constraint.GetOperator()));
    for (const auto &val : constraint.GetLabelValues()) {
      proto_constraint->add_label_values(val);
    }
  }
  return result;
}

void LabelSelector::AddConstraint(const std::string &key, const std::string &value) {
  auto [op, values] = ParseLabelSelectorValue(key, value);
  LabelConstraint constraint(key, op, values);
  AddConstraint(std::move(constraint));
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
    op = is_negated ? LabelSelectorOperator::LABEL_NOT_IN
                    : LabelSelectorOperator::LABEL_IN;
  } else {
    values.insert(std::string(val));
    op = is_negated ? LabelSelectorOperator::LABEL_NOT_IN
                    : LabelSelectorOperator::LABEL_IN;
  }

  return {op, values};
}

}  // namespace ray
