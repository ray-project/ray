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

#include "ray/util/strings/debug_printer.h"

#include <gtest/gtest.h>

#include <iostream>
#include <map>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>

namespace ray {

namespace {
enum class MyEnum { kEnum };
struct Point {
  template <typename Sink>
  friend void AbslStringify(Sink &sink, const Point &p) {
    absl::Format(&sink, "(%d, %d)", p.x, p.y);
  }
  int x;
  int y;
};
struct DebugStringStruct {
  std::string DebugString() const { return "hello"; }
};
}  // namespace

TEST(DebugStringTest, LiteralTest) {
  // Literal.
  EXPECT_EQ(DebugString(1), "1");

  // Debug string.
  EXPECT_EQ(DebugString(DebugStringStruct{}), "hello");

  // Container.
  const std::vector<int> vec{1, 2, 3};
  EXPECT_EQ(DebugString(vec), "[1, 2, 3]");

  // std::byte.
  const std::byte b{10};
  EXPECT_EQ(DebugString(b), "b[0x0a]");

  // Boolean.
  EXPECT_EQ(DebugString(true), "true");

  // nullptr
  EXPECT_EQ(DebugString(nullptr), "nullptr");

  // Tuple.
  const std::tuple<int, std::string, double> tpl{4, "hello", 4.5};
  EXPECT_EQ(DebugString(tpl), "[4, hello, 4.5]");

  // Map.
  std::map<std::string, std::string> m;
  m.emplace("a", "b");
  m.emplace("hello", "world");
  EXPECT_EQ(DebugString(m), "[{a, b}, {hello, world}]");

  // Pair.
  std::pair<int, double> p;
  p.first = 5;
  p.second = 10.6;
  EXPECT_EQ(DebugString(p), "{5, 10.6}");

  // Container inside of container.
  const std::vector<std::vector<int>> cont{
      {1, 2, 3},
      {4, 5, 6},
  };
  EXPECT_EQ(DebugString(cont), "[[1, 2, 3], [4, 5, 6]]");

  // Enum.
  EXPECT_EQ(DebugString(MyEnum::kEnum), "0");

  // Abseil stringify.
  EXPECT_EQ(DebugString(Point{.x = 10, .y = 20}), "(10, 20)");

  // std::optional
  const std::optional<int> has_value{10};
  const std::optional<int> no_value = std::nullopt;
  EXPECT_EQ(DebugString(has_value), "10");
  EXPECT_EQ(DebugString(no_value), "(nullopt)");

  // std::optional with std::vector and std::map inside.
  std::map<std::string, int> uno_m = {
      {"hello", 6},
  };
  std::vector<std::map<std::string, int>> map_vec = {uno_m};
  std::optional<std::vector<std::map<std::string, int>>> opt_vec_map = map_vec;
  EXPECT_EQ(DebugString(opt_vec_map), "[[{hello, 6}]]");

  // std::type_info
  EXPECT_EQ(DebugString(typeid(Point)), "ray::(anonymous namespace)::Point");

  // std::variant
  std::variant<std::monostate, std::string, double, std::vector<std::vector<int>>> v;
  v = std::monostate{};
  EXPECT_EQ(DebugString(v), "(monostate)");

  v = std::string{"hello world"};
  EXPECT_EQ(DebugString(v), "hello world");

  v = std::vector<std::vector<int>>{
      std::vector<int>{1},
      std::vector<int>{2},
  };
  EXPECT_EQ(DebugString(v), "[[1], [2]]");

  // Complex type.
  std::vector<int> vec1 = {1};
  std::vector<std::vector<int>> vec2 = {vec1, vec1};
  std::vector<std::vector<std::vector<int>>> vec3 = {vec2, vec2};
  std::vector<std::vector<std::vector<std::vector<int>>>> vec4 = {vec3, vec3};
  EXPECT_EQ(DebugString(vec4), "[[[[1], [1]], [[1], [1]]], [[[1], [1]], [[1], [1]]]]");

  // Complex with self-defined type.
  std::vector<Point> objs{Point{.x = 5, .y = 6}};
  EXPECT_EQ(DebugString(objs), "[(5, 6)]");
}

}  // namespace ray
