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
#include <cstddef>
#include <map>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/hash/hash.h"
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

TEST(LabelSelectorTest, SplitLabelSelectorValue) {
  using Expressions = std::vector<std::string>;
  EXPECT_EQ(LabelSelector::SplitLabelSelectorValue(""), Expressions({""}));
  EXPECT_EQ(LabelSelector::SplitLabelSelectorValue("a"), Expressions({"a"}));
  EXPECT_EQ(LabelSelector::SplitLabelSelectorValue("!in(a,b)"),
            Expressions({"!in(a,b)"}));
  // Commas inside parentheses separate values, and commas outside separate expressions.
  EXPECT_EQ(LabelSelector::SplitLabelSelectorValue("in(a,b),!c,!in(d,e)"),
            Expressions({"in(a,b)", "!c", "!in(d,e)"}));
}

TEST(LabelSelectorTest, SeveralExpressionsParsing) {
  LabelSelector selector;
  selector.AddConstraint("zone", "in(a,b,c),!in(b),!c");

  const auto &constraints = selector.GetConstraints();
  ASSERT_EQ(constraints.size(), 3);
  EXPECT_EQ(constraints[0],
            LabelConstraint("zone", LabelSelectorOperator::LABEL_IN, {"a", "b", "c"}));
  EXPECT_EQ(constraints[1],
            LabelConstraint("zone", LabelSelectorOperator::LABEL_NOT_IN, {"b"}));
  EXPECT_EQ(constraints[2],
            LabelConstraint("zone", LabelSelectorOperator::LABEL_NOT_IN, {"c"}));
}

TEST(LabelSelectorTest, ToStringMapJoinsConstraintsOnOneKey) {
  LabelSelector selector;
  selector.AddConstraint("zone", "in(b,a),!b");
  selector.AddConstraint("zone", "!c");

  auto string_map = selector.ToStringMap();
  ASSERT_EQ(string_map.size(), 1);
  EXPECT_EQ(string_map.at("zone"), "in(a,b),!b,!c");
  // The joined value parses back into the same constraints.
  EXPECT_EQ(LabelSelector(string_map), selector);
}

TEST(LabelSelectorTest, ProtoRoundTripKeepsEveryExpression) {
  LabelSelector selector;
  selector.AddConstraint("zone", "in(a,b),!b");

  rpc::LabelSelector proto_selector;
  selector.ToProto(&proto_selector);
  ASSERT_EQ(proto_selector.label_constraints_size(), 2);
  EXPECT_EQ(LabelSelector(proto_selector), selector);
}

// Returns whether a node with the given labels satisfies `value` on `key`.
bool NodeMatches(absl::flat_hash_map<std::string, std::string> node_labels,
                 const std::string &key,
                 const std::string &value) {
  NodeResources node;
  node.labels = std::move(node_labels);
  LabelSelector selector;
  selector.AddConstraint(key, value);
  return node.HasRequiredLabels(selector);
}

TEST(LabelSelectorTest, NodeMustMatchEveryExpressionOnOneKey) {
  EXPECT_TRUE(NodeMatches({{"zone", "a"}}, "zone", "in(a,b),!b"));
  EXPECT_FALSE(NodeMatches({{"zone", "b"}}, "zone", "in(a,b),!b"));
  EXPECT_FALSE(NodeMatches({{"zone", "c"}}, "zone", "in(a,b),!b"));
  EXPECT_FALSE(NodeMatches({}, "zone", "in(a,b),!b"));

  // Negated expressions alone still match a node without the key.
  EXPECT_TRUE(NodeMatches({}, "zone", "!a,!in(b,c)"));
  EXPECT_TRUE(NodeMatches({{"zone", "d"}}, "zone", "!a,!in(b,c)"));
  EXPECT_FALSE(NodeMatches({{"zone", "c"}}, "zone", "!a,!in(b,c)"));
}

TEST(LabelSelectorTest, ExistsOperatorParsing) {
  LabelSelector selector;
  selector.AddConstraint("zone", "exists()");
  selector.AddConstraint("gpu", "!exists()");

  const auto &constraints = selector.GetConstraints();
  ASSERT_EQ(constraints.size(), 2);
  EXPECT_EQ(constraints[0],
            LabelConstraint("zone", LabelSelectorOperator::LABEL_EXISTS, {}));
  EXPECT_EQ(constraints[1],
            LabelConstraint("gpu", LabelSelectorOperator::LABEL_DOES_NOT_EXIST, {}));
  EXPECT_EQ(selector.DebugString(), "{'zone': exists (), 'gpu': !exists ()}");
}

TEST(LabelSelectorTest, ExistsOperatorRoundTrips) {
  LabelSelector selector;
  selector.AddConstraint("zone", "exists(),!in(a,b)");
  selector.AddConstraint("gpu", "!exists()");

  auto string_map = selector.ToStringMap();
  ASSERT_EQ(string_map.size(), 2);
  EXPECT_EQ(string_map.at("zone"), "exists(),!in(a,b)");
  EXPECT_EQ(string_map.at("gpu"), "!exists()");

  rpc::LabelSelector proto_selector;
  selector.ToProto(&proto_selector);
  ASSERT_EQ(proto_selector.label_constraints_size(), 3);
  EXPECT_EQ(proto_selector.label_constraints(0).operator_(),
            rpc::LabelSelectorOperator::LABEL_OPERATOR_EXISTS);
  EXPECT_EQ(proto_selector.label_constraints(0).label_values_size(), 0);
  EXPECT_EQ(proto_selector.label_constraints(2).operator_(),
            rpc::LabelSelectorOperator::LABEL_OPERATOR_DOES_NOT_EXIST);
  EXPECT_EQ(LabelSelector(proto_selector), selector);
}

TEST(LabelSelectorTest, NodeMatchesExistsOperators) {
  EXPECT_TRUE(NodeMatches({{"zone", "a"}}, "zone", "exists()"));
  EXPECT_TRUE(NodeMatches({{"zone", ""}}, "zone", "exists()"));
  EXPECT_FALSE(NodeMatches({{"region", "a"}}, "zone", "exists()"));

  EXPECT_FALSE(NodeMatches({{"zone", "a"}}, "zone", "!exists()"));
  EXPECT_TRUE(NodeMatches({{"region", "a"}}, "zone", "!exists()"));

  // exists() turns a negated expression into one that needs the key.
  EXPECT_TRUE(NodeMatches({{"zone", "c"}}, "zone", "exists(),!in(a,b)"));
  EXPECT_FALSE(NodeMatches({{"zone", "a"}}, "zone", "exists(),!in(a,b)"));
  EXPECT_FALSE(NodeMatches({}, "zone", "exists(),!in(a,b)"));

  // Contradictory expressions match no node.
  EXPECT_FALSE(NodeMatches({{"zone", "a"}}, "zone", "exists(),!exists()"));
  EXPECT_FALSE(NodeMatches({}, "zone", "exists(),!exists()"));
}

namespace {

LabelSelector OneConstraint(std::string key,
                            LabelSelectorOperator op,
                            absl::flat_hash_set<std::string> values) {
  LabelSelector selector;
  selector.AddConstraint(LabelConstraint(std::move(key), op, std::move(values)));
  return selector;
}

LabelSelector RegionSelector(absl::flat_hash_set<std::string> values) {
  return OneConstraint("region", LabelSelectorOperator::LABEL_IN, std::move(values));
}

}  // namespace

// LabelConstraint values form an unordered set, so their hash must be order-independent
// to match operator==. We shuffle inputs to force diverse internal memory layouts
// (caused by insertion order and internal salting) and verify hash stability.
TEST(LabelSelectorTest, EqualSelectorsHashEquallyWhateverTheValueOrder) {
  std::vector<std::string> values;
  values.reserve(64);
  for (int i = 0; i < 64; i++) {
    values.push_back("region-" + std::to_string(i));
  }
  const LabelSelector reference =
      RegionSelector(absl::flat_hash_set<std::string>(values.begin(), values.end()));

  absl::flat_hash_set<size_t> hashes;
  absl::flat_hash_set<std::vector<std::string>> layouts;
  std::mt19937 rng(20260826);
  for (int round = 0; round < 16; round++) {
    std::shuffle(values.begin(), values.end(), rng);
    absl::flat_hash_set<std::string> set(values.begin(), values.end());
    const LabelSelector selector = RegionSelector(std::move(set));
    // operator== compares the constraint vectors, so it also passes when both sides are
    // empty; the count needs its own assertion, which also guards the [0] below.
    ASSERT_EQ(selector.GetConstraints().size(), 1u);
    ASSERT_EQ(selector, reference);
    const auto &stored = selector.GetConstraints()[0].GetLabelValues();
    layouts.insert(std::vector<std::string>(stored.begin(), stored.end()));
    hashes.insert(absl::HashOf(selector));
  }

  // Without more than one layout the hash assertion below holds for an order-dependent
  // hash too, so the test would pass while guarding nothing.
  ASSERT_GT(layouts.size(), 1u);
  EXPECT_EQ(hashes.size(), 1u);
}

// Verify that the values are mixed into the hash.
TEST(LabelSelectorTest, SelectorsWithDifferentValuesHashDifferently) {
  EXPECT_NE(absl::HashOf(RegionSelector({"us-east", "us-west"})),
            absl::HashOf(RegionSelector({"eu-central", "ap-south"})));
  EXPECT_NE(absl::HashOf(RegionSelector({"us-east", "us-west"})),
            absl::HashOf(RegionSelector({"us-east", "us-west", "eu-central"})));
  EXPECT_NE(absl::HashOf(RegionSelector({})), absl::HashOf(RegionSelector({"us-east"})));
}

// Verify that both the key and the operator are mixed into the hash.
TEST(LabelSelectorTest, SelectorsDifferingOnlyInKeyOrOperatorHashDifferently) {
  const absl::flat_hash_set<std::string> values = {"us-east"};
  EXPECT_NE(
      absl::HashOf(OneConstraint("region", LabelSelectorOperator::LABEL_IN, values)),
      absl::HashOf(OneConstraint("zone", LabelSelectorOperator::LABEL_IN, values)));
  EXPECT_NE(
      absl::HashOf(OneConstraint("region", LabelSelectorOperator::LABEL_IN, values)),
      absl::HashOf(OneConstraint("region", LabelSelectorOperator::LABEL_NOT_IN, values)));
  EXPECT_NE(
      absl::HashOf(OneConstraint("region", LabelSelectorOperator::LABEL_EXISTS, {})),
      absl::HashOf(
          OneConstraint("region", LabelSelectorOperator::LABEL_DOES_NOT_EXIST, {})));
}

// Each expression becomes its own constraint, so an extra expression on a key changes
// both equality and the hash, while reordering an in() list changes neither.
TEST(LabelSelectorTest, ExpressionsOnOneKeyCompareAndHashTogether) {
  LabelSelector in_only;
  in_only.AddConstraint("zone", "in(a,b)");
  LabelSelector with_not_in;
  with_not_in.AddConstraint("zone", "in(a,b),!b");
  LabelSelector reordered;
  reordered.AddConstraint("zone", "in(b,a),!b");

  EXPECT_FALSE(in_only == with_not_in);
  EXPECT_NE(absl::HashOf(in_only), absl::HashOf(with_not_in));
  EXPECT_EQ(with_not_in, reordered);
  EXPECT_EQ(absl::HashOf(with_not_in), absl::HashOf(reordered));
}

}  // namespace ray
