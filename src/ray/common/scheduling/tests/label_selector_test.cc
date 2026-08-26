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

#include "absl/container/flat_hash_set.h"
#include "absl/hash/hash.h"
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

namespace {

LabelSelector OneConstraint(const std::string &key,
                            LabelSelectorOperator op,
                            const absl::flat_hash_set<std::string> &values) {
  LabelSelector selector;
  selector.AddConstraint(LabelConstraint(key, op, values));
  return selector;
}

LabelSelector RegionSelector(const absl::flat_hash_set<std::string> &values) {
  return OneConstraint("region", LabelSelectorOperator::LABEL_IN, values);
}

}  // namespace

// A constraint holds its values in a flat_hash_set and operator== compares them as a
// set, so selectors built from the same labels have to hash alike however the set was
// filled. Which slot an element takes depends on the order elements were inserted, when
// two contend for one, and on a salt absl derives from the address of the table's control
// bytes, so sweep shuffled orders rather than trusting one layout.
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
    const LabelSelector selector = RegionSelector(set);
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

// The order sweep above would also pass if the values stopped reaching the hash at all,
// so pin that they contribute.
TEST(LabelSelectorTest, SelectorsWithDifferentValuesHashDifferently) {
  EXPECT_NE(absl::HashOf(RegionSelector({"us-east", "us-west"})),
            absl::HashOf(RegionSelector({"eu-central", "ap-south"})));
  EXPECT_NE(absl::HashOf(RegionSelector({"us-east", "us-west"})),
            absl::HashOf(RegionSelector({"us-east", "us-west", "eu-central"})));
  EXPECT_NE(absl::HashOf(RegionSelector({})), absl::HashOf(RegionSelector({"us-east"})));
}

// The key and the operator need their own case: with only the cases above, dropping
// either one from the hash leaves this file green.
TEST(LabelSelectorTest, SelectorsDifferingOnlyInKeyOrOperatorHashDifferently) {
  const absl::flat_hash_set<std::string> values = {"us-east"};
  EXPECT_NE(
      absl::HashOf(OneConstraint("region", LabelSelectorOperator::LABEL_IN, values)),
      absl::HashOf(OneConstraint("zone", LabelSelectorOperator::LABEL_IN, values)));
  EXPECT_NE(
      absl::HashOf(OneConstraint("region", LabelSelectorOperator::LABEL_IN, values)),
      absl::HashOf(OneConstraint("region", LabelSelectorOperator::LABEL_NOT_IN, values)));
}

}  // namespace ray
