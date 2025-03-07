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

#include "ray/util/type_traits.h"

#include <gtest/gtest.h>

#include <list>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace ray {

TEST(TypeTraitsTest, Container) {
  EXPECT_FALSE(is_container_v<int>);
  EXPECT_FALSE(is_container_v<double>);
  EXPECT_TRUE(is_container_v<std::vector<int>>);
  EXPECT_TRUE(is_container_v<std::list<int>>);
}

TEST(TypeTraitsTest, Map) {
  using T1 = std::map<int, int>;
  EXPECT_TRUE(is_map_v<T1>);
  using T2 = std::unordered_map<int, int>;
  EXPECT_TRUE(is_map_v<T2>);
  EXPECT_FALSE(is_map_v<int>);
  EXPECT_FALSE(is_map_v<double>);
}

}  // namespace ray
