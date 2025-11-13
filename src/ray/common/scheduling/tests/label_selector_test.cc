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
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace ray {

TEST(LabelSelectorTest, BasicConstruction) {
  google::protobuf::Map<std::string, std::string> label_selector_dict;
  label_selector_dict["market-type"] = "spot";
  label_selector_dict["region"] = "us-east";

  LabelSelector selector(label_selector_dict);
  auto constraints = selector.GetConstraints();

  ASSERT_EQ(constraints.size(), 2);

  for (const auto &constraint : constraints) {
    EXPECT_TRUE(label_selector_dict.count(constraint.GetLabelKey()));
    EXPECT_EQ(constraint.GetOperator(), LabelSelectorOperator::LABEL_IN);
    auto values = constraint.GetLabelValues();
    EXPECT_EQ(values.size(), 1);
    EXPECT_EQ(*values.begin(), label_selector_dict[constraint.GetLabelKey()]);
  }
}

TEST(LabelSelectorTest, InOperatorParsing) {
  LabelSelector selector;
  selector.AddConstraint("region", "in(us-west,us-east,me-central)");

  auto constraints = selector.GetConstraints();
  ASSERT_EQ(constraints.size(), 1);
  const auto &constraint = constraints[0];

  EXPECT_EQ(constraint.GetOperator(), LabelSelectorOperator::LABEL_IN);
  auto values = constraint.GetLabelValues();
  EXPECT_EQ(values.size(), 3);
  EXPECT_TRUE(values.contains("us-west"));
  EXPECT_TRUE(values.contains("us-east"));
  EXPECT_TRUE(values.contains("me-central"));
}

TEST(LabelSelectorTest, NotInOperatorParsing) {
  LabelSelector selector;
  selector.AddConstraint("tier", "!in(premium,free)");

  auto constraints = selector.GetConstraints();
  ASSERT_EQ(constraints.size(), 1);
  const auto &constraint = constraints[0];

  EXPECT_EQ(constraint.GetOperator(), LabelSelectorOperator::LABEL_NOT_IN);
  auto values = constraint.GetLabelValues();
  EXPECT_EQ(values.size(), 2);
  EXPECT_TRUE(values.contains("premium"));
  EXPECT_TRUE(values.contains("free"));
}

TEST(LabelSelectorTest, SingleValueNotInParsing) {
  LabelSelector selector;
  selector.AddConstraint("env", "!dev");

  auto constraints = selector.GetConstraints();
  ASSERT_EQ(constraints.size(), 1);
  const auto &constraint = constraints[0];

  EXPECT_EQ(constraint.GetOperator(), LabelSelectorOperator::LABEL_NOT_IN);
  auto values = constraint.GetLabelValues();
  EXPECT_EQ(values.size(), 1);
  EXPECT_TRUE(values.contains("dev"));
}

TEST(LabelSelectorTest, ToStringMap) {
  using ::testing::ElementsAre;
  using ::testing::IsEmpty;
  using ::testing::Pair;

  // Unpopulated label selector.
  LabelSelector empty_selector;
  auto empty_map = empty_selector.ToStringMap();
  EXPECT_TRUE(empty_map.empty());

  // Test label selector with all supported constraints.
  LabelSelector selector;

  selector.AddConstraint(
      LabelConstraint("region", LabelSelectorOperator::LABEL_IN, {"us-west"}));

  selector.AddConstraint(LabelConstraint(
      "tier", LabelSelectorOperator::LABEL_IN, {"prod", "dev", "staging"}));

  selector.AddConstraint(
      LabelConstraint("env", LabelSelectorOperator::LABEL_NOT_IN, {"dev"}));

  selector.AddConstraint(
      LabelConstraint("team", LabelSelectorOperator::LABEL_NOT_IN, {"A100", "B200"}));

  // Validate LabelSelector is correctly converted back to a string map.
  // We explicitly sort the values, which are stored in an unordered set,
  // to ensure the string output is deterministic.
  auto string_map = selector.ToStringMap();

  ASSERT_EQ(string_map.size(), 4);
  EXPECT_EQ(string_map.at("region"), "us-west");
  EXPECT_EQ(string_map.at("env"), "!dev");
  EXPECT_EQ(string_map.at("tier"), "in(dev,prod,staging)");
  EXPECT_EQ(string_map.at("team"), "!in(A100,B200)");
}

TEST(LabelSelectorTest, ToProto) {
  LabelSelector selector;
  selector.AddConstraint("region", "us-west");
  selector.AddConstraint("tier", "in(prod,dev)");
  selector.AddConstraint("env", "!dev");
  selector.AddConstraint("team", "!in(A100,B200)");

  rpc::LabelSelector proto_selector;
  selector.ToProto(&proto_selector);

  // Validate constraints are added to proto as expected.
  std::map<std::string, std::pair<rpc::LabelSelectorOperator, std::vector<std::string>>>
      expected_constraints;
  expected_constraints["region"] = {rpc::LabelSelectorOperator::LABEL_OPERATOR_IN,
                                    {"us-west"}};
  expected_constraints["tier"] = {rpc::LabelSelectorOperator::LABEL_OPERATOR_IN,
                                  {"dev", "prod"}};
  expected_constraints["env"] = {rpc::LabelSelectorOperator::LABEL_OPERATOR_NOT_IN,
                                 {"dev"}};
  expected_constraints["team"] = {rpc::LabelSelectorOperator::LABEL_OPERATOR_NOT_IN,
                                  {"A100", "B200"}};

  // Verify each constraint in the proto
  for (const auto &proto_constraint : proto_selector.label_constraints()) {
    const std::string &key = proto_constraint.label_key();

    // Check label key
    ASSERT_TRUE(expected_constraints.count(key))
        << "Unexpected key found in proto: " << key;
    const auto &expected = expected_constraints[key];
    rpc::LabelSelectorOperator expected_op = expected.first;
    const std::vector<std::string> &expected_values = expected.second;

    // Check operator
    EXPECT_EQ(proto_constraint.operator_(), expected_op)
        << "Operator mismatch for key: " << key;

    // Check label values
    std::vector<std::string> actual_values;
    for (const auto &val : proto_constraint.label_values()) {
      actual_values.push_back(val);
    }
    std::sort(actual_values.begin(), actual_values.end());

    EXPECT_EQ(actual_values.size(), expected_values.size())
        << "Value count mismatch for key: " << key;
    EXPECT_EQ(actual_values, expected_values) << "Values mismatch for key: " << key;
    expected_constraints.erase(key);
  }
  EXPECT_TRUE(expected_constraints.empty())
      << "Not all expected constraints were found in the proto.";
}

}  // namespace ray
