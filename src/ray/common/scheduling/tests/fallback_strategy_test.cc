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

#include "ray/common/scheduling/fallback_strategy.h"

#include <map>
#include <memory>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/scheduling/label_selector.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {

TEST(FallbackStrategyTest, OptionsConstructionAndEquality) {
  auto selector_a =
      LabelSelector(std::map<std::string, std::string>{{"region", "us-east-1"}});
  auto selector_b =
      LabelSelector(std::map<std::string, std::string>{{"region", "us-east-1"}});
  auto selector_c =
      LabelSelector(std::map<std::string, std::string>{{"region", "us-west-2"}});

  FallbackOption options_a(selector_a);
  FallbackOption options_b(selector_b);
  FallbackOption options_c(selector_c);

  // Test FallbackOption equality
  EXPECT_EQ(options_a, options_b);
  EXPECT_FALSE(options_a == options_c);

  // Test FallbackOption from proto constructor
  rpc::LabelSelector selector_a_proto;
  selector_a.ToProto(&selector_a_proto);
  FallbackOption options_from_proto(selector_a_proto);

  EXPECT_EQ(options_a, options_from_proto);
}

TEST(FallbackStrategyTest, OptionsToProto) {
  auto selector =
      LabelSelector(std::map<std::string, std::string>{{"accelerator-type", "A100"}});
  FallbackOption options(selector);

  rpc::FallbackOption proto;
  options.ToProto(&proto);

  ASSERT_TRUE(proto.has_label_selector());
  FallbackOption options_from_proto(proto.label_selector());
  EXPECT_EQ(options, options_from_proto);
  EXPECT_EQ(options_from_proto.label_selector.ToStringMap().at("accelerator-type"),
            "A100");
}

TEST(FallbackStrategyTest, OptionsHashing) {
  auto selector_a = LabelSelector(std::map<std::string, std::string>{{"key1", "val1"}});
  auto selector_b = LabelSelector(std::map<std::string, std::string>{{"key1", "val1"}});
  auto selector_c = LabelSelector(std::map<std::string, std::string>{{"key2", "val2"}});

  FallbackOption options_a(selector_a);
  FallbackOption options_b(selector_b);
  FallbackOption options_c(selector_c);

  absl::Hash<FallbackOption> hasher;
  EXPECT_EQ(hasher(options_a), hasher(options_b));
  EXPECT_FALSE(hasher(options_a) == hasher(options_c));
}

TEST(FallbackStrategyTest, ParseAndSerializeStrategy) {
  auto selector1 = LabelSelector(std::map<std::string, std::string>{
      {"region", "us-east-1"}, {"market-type", "spot"}});
  auto selector2 =
      LabelSelector(std::map<std::string, std::string>{{"cpu-family", "intel"}});

  auto original_list = std::make_shared<std::vector<FallbackOption>>();
  original_list->emplace_back(selector1);
  original_list->emplace_back(selector2);

  // Serialize to FallbackStrategy proto
  auto serialized_proto = SerializeFallbackStrategy(*original_list);
  ASSERT_EQ(serialized_proto.options_size(), 2);

  // Parse the proto back into the FallbackStrategy C++ struct vector
  auto parsed_list = ParseFallbackStrategy(serialized_proto.options());

  // Validate options are parsed successfully
  ASSERT_NE(parsed_list, nullptr);
  ASSERT_EQ(parsed_list->size(), 2);

  EXPECT_EQ(*original_list, *parsed_list);

  auto map1 = (*parsed_list)[0].label_selector.ToStringMap();
  EXPECT_EQ(map1.at("region"), "us-east-1");
  EXPECT_EQ(map1.at("market-type"), "spot");
  auto map2 = (*parsed_list)[1].label_selector.ToStringMap();
  EXPECT_EQ(map2.at("cpu-family"), "intel");
}

TEST(FallbackStrategyTest, EmptyFallbackStrategy) {
  rpc::FallbackStrategy empty_proto;
  auto parsed_list = ParseFallbackStrategy(empty_proto.options());

  // Validate empty fallback list is handled correctly.
  ASSERT_NE(parsed_list, nullptr);
  EXPECT_TRUE(parsed_list->empty());

  auto serialized_proto = SerializeFallbackStrategy(*parsed_list);
  EXPECT_EQ(serialized_proto.options_size(), 0);
}

}  // namespace ray
