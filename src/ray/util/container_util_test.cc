// Copyright 2022 The Ray Authors.
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

#include "ray/util/container_util.h"

#include "gtest/gtest.h"

namespace ray {

TEST(ContainerUtilTest, TestDebugString) {
  ASSERT_EQ(debug_string(std::vector<int>{1, 2}), "[1, 2]");
  ASSERT_EQ(debug_string(std::set<int>{1, 2}), "[1, 2]");
  ASSERT_EQ(debug_string(std::unordered_set<int>{2}), "[2]");
  ASSERT_EQ(debug_string(absl::flat_hash_set<int>{1}), "[1]");
  ASSERT_EQ(debug_string(std::map<int, int>{{1, 2}, {3, 4}}), "[(1, 2), (3, 4)]");
  ASSERT_EQ(debug_string(absl::flat_hash_map<int, int>{{3, 4}}), "[(3, 4)]");
  ASSERT_EQ(debug_string(absl::flat_hash_map<int, int>{{1, 2}}), "[(1, 2)]");
}

TEST(ContainerUtilTest, TestMapFindOrDie) {
  {
    std::map<int, int> m{{1, 2}, {3, 4}};
    ASSERT_EQ(map_find_or_die(m, 1), 2);
    ASSERT_DEATH(map_find_or_die(m, 5), "");
  }
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
