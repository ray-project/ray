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

#include "ray/common/scheduling/scheduling_ids.h"

#include <string>
#include <vector>

#include "gtest/gtest.h"

namespace ray {

struct SchedulingIDsTest : public ::testing::Test {};

TEST_F(SchedulingIDsTest, BasicTest) {
  std::vector<std::string> string_ids = {"hello", "whaaat", "yes"};
  std::vector<scheduling::NodeID> node_ids;
  for (auto &string_id : string_ids) {
    node_ids.emplace_back(scheduling::NodeID(string_id));
    ASSERT_EQ(node_ids.back().Binary(), string_id);
  }
  ASSERT_EQ(node_ids[0], scheduling::NodeID(string_ids[0]));
  ASSERT_EQ(node_ids[0], scheduling::NodeID(node_ids[0].ToInt()));

  ASSERT_TRUE(scheduling::NodeID::Nil().IsNil());
  ASSERT_EQ(scheduling::NodeID::Nil().ToInt(), -1);
  ASSERT_EQ(scheduling::NodeID::Nil().Binary(), "-1");

  ASSERT_EQ(scheduling::NodeID(13), scheduling::NodeID(13));
  ASSERT_NE(scheduling::NodeID(1), scheduling::NodeID(2));
  ASSERT_TRUE(scheduling::NodeID(1) < scheduling::NodeID(2));
}

TEST_F(SchedulingIDsTest, PrepopulateResourceIDTest) {
  ASSERT_EQ(kCPU_ResourceLabel, scheduling::ResourceID(CPU).Binary());
  ASSERT_EQ(kGPU_ResourceLabel, scheduling::ResourceID(GPU).Binary());
  ASSERT_EQ(kObjectStoreMemory_ResourceLabel,
            scheduling::ResourceID(OBJECT_STORE_MEM).Binary());
  ASSERT_EQ(kMemory_ResourceLabel, scheduling::ResourceID(MEM).Binary());

  // mean while NodeID is not populated.
  ASSERT_NE(kCPU_ResourceLabel, scheduling::NodeID(CPU).Binary());
}

TEST_F(SchedulingIDsTest, UnitInstanceResourceTest) {
  RayConfig::instance().initialize(
      R"(
{
  "predefined_unit_instance_resources": "CPU,GPU",
  "custom_unit_instance_resources": "neuron_cores,TPU,custom1"
}
  )");
  ASSERT_TRUE(scheduling::ResourceID::CPU().IsUnitInstanceResource());
  ASSERT_TRUE(scheduling::ResourceID::GPU().IsUnitInstanceResource());
  ASSERT_TRUE(scheduling::ResourceID("custom1").IsUnitInstanceResource());
  ASSERT_TRUE(scheduling::ResourceID("neuron_cores").IsUnitInstanceResource());
  ASSERT_TRUE(scheduling::ResourceID("TPU").IsUnitInstanceResource());

  ASSERT_FALSE(scheduling::ResourceID::Memory().IsUnitInstanceResource());
  ASSERT_FALSE(scheduling::ResourceID("custom2").IsUnitInstanceResource());
}
}  // namespace ray
