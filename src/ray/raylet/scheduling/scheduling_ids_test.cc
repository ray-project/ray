// Copyright 2021 The Ray Authors.
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

#include "ray/raylet/scheduling/scheduling_ids.h"

#include "gtest/gtest.h"

namespace ray {
using namespace ray::scheduling;

struct SchedulingIDsTest : public ::testing::Test {};

TEST_F(SchedulingIDsTest, BasicTest) {
  std::vector<std::string> string_ids = {"hello", "whaaat", "yes"};
  std::vector<NodeID> node_ids;
  for (auto &string_id : string_ids) {
    node_ids.emplace_back(NodeID(string_id));
    ASSERT_EQ(node_ids.back().ToBinary(), string_id);
  }
  ASSERT_EQ(node_ids[0], NodeID(string_ids[0]));
  ASSERT_EQ(node_ids[0], NodeID(node_ids[0].ToInt64()));

  ASSERT_TRUE(NodeID::NullID().IsNull());
  ASSERT_EQ(NodeID::NullID().ToInt64(), -1);
  ASSERT_EQ(NodeID::NullID().ToBinary(), "-1");
}

TEST_F(SchedulingIDsTest, PrepopulateResourceIDTest) {
  ASSERT_EQ(kCPU_ResourceLabel, ResourceID(CPU).ToBinary());
  ASSERT_EQ(kGPU_ResourceLabel, ResourceID(GPU).ToBinary());
  ASSERT_EQ(kObjectStoreMemory_ResourceLabel, ResourceID(OBJECT_STORE_MEM).ToBinary());
  ASSERT_EQ(kMemory_ResourceLabel, ResourceID(MEM).ToBinary());

  // mean while NodeID is not populated.
  ASSERT_NE(kCPU_ResourceLabel, NodeID(CPU).ToBinary());
}
}  // namespace ray
