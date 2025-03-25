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

#include <gtest/gtest.h>

#include <deque>
#include <list>
#include <map>
#include <optional>
#include <sstream>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace ray {

template <typename T>
std::string debug_string_to_string(const T &t) {
  std::ostringstream ss;
  ss << debug_string(t);
  return std::move(ss).str();
}

TEST(ContainerUtilTest, TestDebugString) {
  // Numerical values.
  ASSERT_EQ(debug_string_to_string(static_cast<int>(2)), "2");

  // String values.
  ASSERT_EQ(debug_string_to_string(std::string_view{"hello"}), "hello");
  ASSERT_EQ(debug_string_to_string(std::string{"hello"}), "hello");

  // Non-associative containers.
  ASSERT_EQ(debug_string_to_string(std::vector<int>{1, 2}), "[1, 2]");
  ASSERT_EQ(debug_string_to_string(std::array<int, 3>{1, 2, 3}), "[1, 2, 3]");
  ASSERT_EQ(debug_string_to_string(absl::InlinedVector<int, 3>{1, 2}), "[1, 2]");

  // Associative containers.
  ASSERT_EQ(debug_string_to_string(std::set<int>{1, 2}), "[1, 2]");
  ASSERT_EQ(debug_string_to_string(std::unordered_set<int>{2}), "[2]");
  ASSERT_EQ(debug_string_to_string(absl::flat_hash_set<int>{1}), "[1]");
  ASSERT_EQ(debug_string_to_string(std::map<int, int>{{1, 2}, {3, 4}}),
            "[(1, 2), (3, 4)]");
  ASSERT_EQ(debug_string_to_string(absl::flat_hash_map<int, int>{{3, 4}}), "[(3, 4)]");
  ASSERT_EQ(debug_string_to_string(absl::flat_hash_map<int, int>{{1, 2}}), "[(1, 2)]");

  // Tuples
  ASSERT_EQ(debug_string_to_string(std::tuple<>()), "()");
  ASSERT_EQ(debug_string_to_string(std::tuple<int>(2)), "(2)");
  ASSERT_EQ(debug_string_to_string(std::tuple<int, std::string>({2, "hello world"})),
            "(2, hello world)");
  ASSERT_EQ(debug_string_to_string(
                std::tuple<int, std::string, bool>({2, "hello world", true})),
            "(2, hello world, 1)");

  // Pairs
  ASSERT_EQ(debug_string_to_string(std::pair<int, int>{1, 2}), "(1, 2)");
  ASSERT_EQ(debug_string_to_string(std::pair<std::string, int>{"key", 42}), "(key, 42)");
  ASSERT_EQ(debug_string_to_string(std::pair<int, std::string>{3, "value"}),
            "(3, value)");

  // Optional.
  ASSERT_EQ(debug_string_to_string(std::nullopt), "(nullopt)");
  ASSERT_EQ(debug_string_to_string(std::optional<std::string>{}), "(nullopt)");
  ASSERT_EQ(debug_string_to_string(std::optional<std::string>{"hello"}), "hello");

  // Composable: tuples of pairs of maps and vectors.
  ASSERT_EQ(debug_string_to_string(
                std::tuple<std::pair<int, std::vector<int>>, std::map<int, int>>{
                    {1, {2, 3}}, {{4, 5}, {6, 7}}}),
            "((1, [2, 3]), [(4, 5), (6, 7)])");

  ASSERT_EQ(
      debug_string_to_string(std::tuple<std::pair<std::string, std::vector<std::string>>,
                                        std::map<std::string, std::string>>{
          {"key", {"value1", "value2"}}, {{"key1", "value1"}, {"key2", "value2"}}}),
      "((key, [value1, value2]), [(key1, value1), (key2, value2)])");

  ASSERT_EQ(
      debug_string_to_string(
          std::tuple<std::pair<int, std::vector<int>>, std::map<int, std::vector<int>>>{
              {1, {2, 3}}, {{4, {5, 6}}, {7, {8, 9}}}}),
      "((1, [2, 3]), [(4, [5, 6]), (7, [8, 9])])");

  ASSERT_EQ(
      debug_string_to_string(std::tuple<std::pair<int, std::vector<int>>,
                                        std::map<int, std::vector<std::pair<int, int>>>>{
          {1, {2, 3}}, {{4, {{5, 6}, {7, 8}}}, {9, {{10, 11}, {12, 13}}}}}),
      "((1, [2, 3]), [(4, [(5, 6), (7, 8)]), (9, [(10, 11), (12, 13)])])");
}

TEST(ContainerUtilTest, TestMapFindOrDie) {
  {
    std::map<int, int> m{{1, 2}, {3, 4}};
    ASSERT_EQ(map_find_or_die(m, 1), 2);
    ASSERT_DEATH(map_find_or_die(m, 5), "");
  }
}

TEST(ContainerUtilTest, TestEraseIf) {
  {
    std::list<int> list{1, 2, 3, 4};
    ray::erase_if<int>(list, [](const int &value) { return value % 2 == 0; });
    ASSERT_EQ(list, (std::list<int>{1, 3}));
  }

  {
    std::list<int> list{1, 2, 3};
    ray::erase_if<int>(list, [](const int &value) { return value % 2 == 0; });
    ASSERT_EQ(list, (std::list<int>{1, 3}));
  }

  {
    std::list<int> list{};
    ray::erase_if<int>(list, [](const int &value) { return value % 2 == 0; });
    ASSERT_EQ(list, (std::list<int>{}));
  }

  {
    absl::flat_hash_map<int, std::deque<int>> map;
    map[1] = std::deque<int>{1, 3};
    map[2] = std::deque<int>{2, 4};
    map[3] = std::deque<int>{5, 6};
    ray::erase_if<int, int>(map, [](const int &value) { return value % 2 == 0; });

    ASSERT_EQ(map.size(), 2);
    ASSERT_EQ(map[1], (std::deque<int>{1, 3}));
    ASSERT_EQ(map[3], (std::deque<int>{5}));
  }
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
