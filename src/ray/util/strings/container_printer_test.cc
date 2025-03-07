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

#include "ray/util/strings/container_printer.h"

#include <gtest/gtest.h>

#include <map>
#include <optional>
#include <sstream>
#include <variant>
#include <vector>

namespace ray {

namespace {

TEST(ContainerPrinterTest, BasicTest) {
  ContainerPrinter printer{};

  // vector type.
  {
    std::stringstream ss;
    printer(ss, std::vector<int>{1, 2});
    EXPECT_EQ(ss.str(), "[1, 2]");
  }

  // map type.
  {
    std::stringstream ss;
    printer(ss, std::map<int, int>{{1, 2}, {3, 4}});
    EXPECT_EQ(ss.str(), "[{1, 2}, {3, 4}]");
  }

  // variant type with no value.
  {
    std::variant<std::monostate, int> var = std::monostate{};
    std::stringstream ss;
    printer(ss, var);
    EXPECT_EQ(ss.str(), "(monostate)");
  }

  // variant type with value.
  {
    std::variant<std::monostate, int> var = 10;
    std::stringstream ss;
    printer(ss, var);
    EXPECT_EQ(ss.str(), "10");
  }

  // optional type with no value.
  {
    std::optional<int> opt = std::nullopt;
    std::stringstream ss;
    printer(ss, opt);
    EXPECT_EQ(ss.str(), "(nullopt)");
  }

  // optional type with no value.
  {
    std::optional<int> opt = 10;
    std::stringstream ss;
    printer(ss, opt);
    EXPECT_EQ(ss.str(), "10");
  }
}

}  // namespace

}  // namespace ray
