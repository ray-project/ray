// Copyright 2023 The Ray Authors.
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

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "gtest/gtest.h"
#include "ray/common/scheduling/cluster_resource_data.h"

namespace ray {
class ResourceSetTest : public ::testing::Test {};

TEST_F(ResourceSetTest, TestRemoveNegative) {
  ResourceSet r1 = ResourceSet(
      {{"CPU", FixedPoint(-1)}, {"custom1", FixedPoint(2)}, {"custom2", FixedPoint(-2)}});
  r1.RemoveNegative();
  absl::flat_hash_map<std::string, double> expected = {{"custom1", 2}};
  ASSERT_EQ(r1.GetResourceMap(), expected);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
