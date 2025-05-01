#include "ray/common/scheduling/label_selector.h"

namespace ray {

// Constructor to parse LabelSelector data type from proto.
LabelSelector::LabelSelector(const google::protobuf::Map<std::string, std::string> &label_selector) {
  for (const auto &pair : label_selector) {
    const std::string &key = pair.first;
    const std::string &value = pair.second;

    if (key.empty()) {
      throw std::invalid_argument("Label selector key must be a non-empty string.");
    }

    if (value.empty()) {
      throw std::invalid_argument("Label selector value must be a non-empty string.");
    }

    AddConstraint(key, value);
  }
}

void LabelSelector::AddConstraint(const std::string &key, const std::string &value) {
  auto [op, values] = ParseLabelSelectorValue(key, value);
  LabelConstraint constraint(key, op, values);
  AddConstraint(constraint);
}

std::pair<LabelSelectorOperator, absl::flat_hash_set<std::string>>
LabelSelector::ParseLabelSelectorValue(const std::string &key, const std::string &value) {
  bool is_negated = false;
  std::string val = value;
  if (!val.empty() && val[0] == '!') {
    is_negated = true;
    val = val.substr(1);
  }

  absl::flat_hash_set<std::string> values;
  LabelSelectorOperator op;

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
    op = is_negated ? LabelSelectorOperator::NOT_IN : LabelSelectorOperator::IN;
  } else {
    values.insert(val);
    op = is_negated ? LabelSelectorOperator::NOT_IN : LabelSelectorOperator::IN;
  }

  return {op, values};
}

}  // namespace ray
