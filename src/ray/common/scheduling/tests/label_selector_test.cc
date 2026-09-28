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
#include "ray/common/scheduling/cluster_resource_data.h"

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

TEST(LabelSelectorTest, Deduplication) {
  LabelSelector selector;

  selector.AddConstraint("region", "us-west");
  ASSERT_EQ(selector.GetConstraints().size(), 1);

  // Add the exact same constraint again.
  selector.AddConstraint("region", "us-west");
  ASSERT_EQ(selector.GetConstraints().size(), 1);

  // Add a constraint with the same key but different value.
  selector.AddConstraint("region", "us-east");
  ASSERT_EQ(selector.GetConstraints().size(), 2);

  // Add a constraint with a different key but same value.
  selector.AddConstraint("location", "us-east");
  ASSERT_EQ(selector.GetConstraints().size(), 3);

  // Add a constraint with a different key and value.
  selector.AddConstraint("instance", "spot");
  ASSERT_EQ(selector.GetConstraints().size(), 4);

  // Add a duplicate using the LabelConstraint object directly.
  LabelConstraint duplicate_constraint(
      "instance", LabelSelectorOperator::LABEL_IN, {"spot"});
  selector.AddConstraint(duplicate_constraint);
  ASSERT_EQ(selector.GetConstraints().size(), 4);
}

TEST(LabelSelectorTest, ExistsOperatorParsing) {
  LabelSelector selector;
  selector.AddConstraint("ray.io/tpu-slice-name", "exists()");
  selector.AddConstraint("spot", "!exists()");

  auto constraints = selector.GetConstraints();
  ASSERT_EQ(constraints.size(), 2);

  EXPECT_EQ(constraints[0].GetLabelKey(), "ray.io/tpu-slice-name");
  EXPECT_EQ(constraints[0].GetOperator(), LabelSelectorOperator::LABEL_EXISTS);
  EXPECT_TRUE(constraints[0].GetLabelValues().empty());

  EXPECT_EQ(constraints[1].GetLabelKey(), "spot");
  EXPECT_EQ(constraints[1].GetOperator(), LabelSelectorOperator::LABEL_DOES_NOT_EXIST);
  EXPECT_TRUE(constraints[1].GetLabelValues().empty());
}

TEST(LabelSelectorTest, MultipleExpressionsParsing) {
  LabelSelector selector;
  selector.AddConstraint("ray.io/tpu-slice-name", "exists(),!in(slice-a,slice-b)");

  auto constraints = selector.GetConstraints();
  ASSERT_EQ(constraints.size(), 2);

  EXPECT_EQ(constraints[0].GetOperator(), LabelSelectorOperator::LABEL_EXISTS);
  EXPECT_TRUE(constraints[0].GetLabelValues().empty());

  EXPECT_EQ(constraints[1].GetOperator(), LabelSelectorOperator::LABEL_NOT_IN);
  EXPECT_EQ(constraints[1].GetLabelValues(),
            (absl::flat_hash_set<std::string>{"slice-a", "slice-b"}));

  for (const auto &constraint : constraints) {
    EXPECT_EQ(constraint.GetLabelKey(), "ray.io/tpu-slice-name");
  }
}

TEST(LabelSelectorTest, SplitLabelSelectorValue) {
  using ::testing::ElementsAre;

  // Existing single expression forms are not split.
  EXPECT_THAT(LabelSelector::SplitLabelSelectorValue(""), ElementsAre(""));
  EXPECT_THAT(LabelSelector::SplitLabelSelectorValue("spot"), ElementsAre("spot"));
  EXPECT_THAT(LabelSelector::SplitLabelSelectorValue("!spot"), ElementsAre("!spot"));
  EXPECT_THAT(LabelSelector::SplitLabelSelectorValue("in(a,b)"), ElementsAre("in(a,b)"));
  EXPECT_THAT(LabelSelector::SplitLabelSelectorValue("!in(a,b)"),
              ElementsAre("!in(a,b)"));

  // Commas outside parentheses separate expressions.
  EXPECT_THAT(LabelSelector::SplitLabelSelectorValue("exists(),!in(a,b)"),
              ElementsAre("exists()", "!in(a,b)"));
  EXPECT_THAT(LabelSelector::SplitLabelSelectorValue("in(a,b),!c,!exists()"),
              ElementsAre("in(a,b)", "!c", "!exists()"));
}

TEST(LabelSelectorTest, ToStringMapNewOperators) {
  LabelSelector selector;
  selector.AddConstraint(
      LabelConstraint("has-key", LabelSelectorOperator::LABEL_EXISTS, {}));
  selector.AddConstraint(
      LabelConstraint("no-key", LabelSelectorOperator::LABEL_DOES_NOT_EXIST, {}));

  auto string_map = selector.ToStringMap();
  ASSERT_EQ(string_map.size(), 2);
  EXPECT_EQ(string_map.at("has-key"), "exists()");
  EXPECT_EQ(string_map.at("no-key"), "!exists()");
}

TEST(LabelSelectorTest, ToStringMapKeepsEveryConstraintOnAKey) {
  LabelSelector selector;
  selector.AddConstraint("region", "us-west");
  selector.AddConstraint("region", "us-east");
  selector.AddConstraint("slice", "exists()");
  selector.AddConstraint("slice", "!in(b,a)");

  auto string_map = selector.ToStringMap();
  ASSERT_EQ(string_map.size(), 2);
  EXPECT_EQ(string_map.at("region"), "us-west,us-east");
  EXPECT_EQ(string_map.at("slice"), "exists(),!in(a,b)");
}

TEST(LabelSelectorTest, ToStringMapRoundTrip) {
  google::protobuf::Map<std::string, std::string> input;
  input["ray.io/tpu-slice-name"] = "exists(),!in(slice-a,slice-b)";
  input["region"] = "in(us-east,us-west)";
  input["env"] = "!dev";
  input["spot"] = "!exists()";
  input["tier"] = "prod";

  LabelSelector selector(input);
  ASSERT_EQ(selector.GetConstraints().size(), 6);

  auto string_map = selector.ToStringMap();
  ASSERT_EQ(string_map.size(), input.size());
  for (const auto &[key, value] : input) {
    EXPECT_EQ(string_map.at(key), value) << "Mismatch for key: " << key;
  }

  // Parsing the output again gives the same constraints for each key.
  LabelSelector reparsed(string_map);
  EXPECT_EQ(reparsed.ToStringMap().at("ray.io/tpu-slice-name"),
            "exists(),!in(slice-a,slice-b)");
  EXPECT_EQ(reparsed.GetConstraints().size(), selector.GetConstraints().size());
  for (const auto &constraint : selector.GetConstraints()) {
    const auto &reparsed_constraints = reparsed.GetConstraints();
    EXPECT_NE(
        std::find(reparsed_constraints.begin(), reparsed_constraints.end(), constraint),
        reparsed_constraints.end());
  }
}

TEST(LabelSelectorTest, ProtoRoundTripNewOperators) {
  LabelSelector selector;
  selector.AddConstraint("slice", "exists(),!in(a,b)");
  selector.AddConstraint("spot", "!exists()");

  rpc::LabelSelector proto_selector;
  selector.ToProto(&proto_selector);

  ASSERT_EQ(proto_selector.label_constraints_size(), 3);
  EXPECT_EQ(proto_selector.label_constraints(0).operator_(),
            rpc::LabelSelectorOperator::LABEL_OPERATOR_EXISTS);
  EXPECT_EQ(proto_selector.label_constraints(0).label_values_size(), 0);
  EXPECT_EQ(proto_selector.label_constraints(1).operator_(),
            rpc::LabelSelectorOperator::LABEL_OPERATOR_NOT_IN);
  EXPECT_EQ(proto_selector.label_constraints(2).operator_(),
            rpc::LabelSelectorOperator::LABEL_OPERATOR_DOES_NOT_EXIST);

  EXPECT_EQ(LabelSelector(proto_selector), selector);
}

TEST(LabelSelectorTest, DebugStringNewOperators) {
  LabelSelector selector;
  selector.AddConstraint("slice", "exists(),!exists()");
  EXPECT_EQ(selector.DebugString(), "{'slice': exists (), 'slice': !exists ()}");
}

// Returns whether a node with the given labels satisfies the selector.
bool NodeMatches(const absl::flat_hash_map<std::string, std::string> &node_labels,
                 const absl::flat_hash_map<std::string, std::string> &selector) {
  NodeResources node;
  node.labels = node_labels;
  return node.HasRequiredLabels(LabelSelector(selector));
}

TEST(LabelSelectorTest, MatchExistingOperators) {
  // Existing forms keep their meaning.
  EXPECT_TRUE(NodeMatches({{"k", "a"}}, {{"k", "a"}}));
  EXPECT_FALSE(NodeMatches({{"k", "b"}}, {{"k", "a"}}));
  EXPECT_FALSE(NodeMatches({}, {{"k", "a"}}));
  EXPECT_TRUE(NodeMatches({{"k", "a"}}, {{"k", "in(a,b)"}}));
  EXPECT_FALSE(NodeMatches({{"k", "c"}}, {{"k", "in(a,b)"}}));
  EXPECT_FALSE(NodeMatches({{"k", "a"}}, {{"k", "!a"}}));
  EXPECT_TRUE(NodeMatches({{"k", "b"}}, {{"k", "!a"}}));
  EXPECT_FALSE(NodeMatches({{"k", "a"}}, {{"k", "!in(a,b)"}}));
  EXPECT_TRUE(NodeMatches({{"k", "c"}}, {{"k", "!in(a,b)"}}));

  // A negated selector alone still matches a node without the key.
  EXPECT_TRUE(NodeMatches({}, {{"k", "!a"}}));
  EXPECT_TRUE(NodeMatches({}, {{"k", "!in(a,b)"}}));
}

TEST(LabelSelectorTest, MatchExistsOperators) {
  EXPECT_TRUE(NodeMatches({{"k", "a"}}, {{"k", "exists()"}}));
  EXPECT_TRUE(NodeMatches({{"k", ""}}, {{"k", "exists()"}}));
  EXPECT_FALSE(NodeMatches({{"other", "a"}}, {{"k", "exists()"}}));

  EXPECT_FALSE(NodeMatches({{"k", "a"}}, {{"k", "!exists()"}}));
  EXPECT_TRUE(NodeMatches({{"other", "a"}}, {{"k", "!exists()"}}));
  EXPECT_TRUE(NodeMatches({}, {{"k", "!exists()"}}));
}

TEST(LabelSelectorTest, MatchMultipleExpressions) {
  const absl::flat_hash_map<std::string, std::string> selector = {
      {"slice", "exists(),!in(slice-a,slice-b)"}};

  // The node must have the key and its value must not be in the list.
  EXPECT_TRUE(NodeMatches({{"slice", "slice-c"}}, selector));
  EXPECT_FALSE(NodeMatches({{"slice", "slice-a"}}, selector));
  EXPECT_FALSE(NodeMatches({{"slice", "slice-b"}}, selector));
  EXPECT_FALSE(NodeMatches({}, selector));

  // Expressions on one key combine with other keys using AND.
  EXPECT_TRUE(NodeMatches({{"slice", "slice-c"}, {"zone", "z1"}},
                          {{"slice", "exists(),!slice-a"}, {"zone", "z1"}}));
  EXPECT_FALSE(NodeMatches({{"slice", "slice-c"}, {"zone", "z2"}},
                           {{"slice", "exists(),!slice-a"}, {"zone", "z1"}}));
}

}  // namespace ray
