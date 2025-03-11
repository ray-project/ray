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

#include "ray/util/strings/absl_stringify_printer.h"

#include <gtest/gtest.h>

#include "absl/strings/str_format.h"

namespace ray {

namespace {

struct Point {
  template <typename Sink>
  friend void AbslStringify(Sink &sink, const Point &p) {
    absl::Format(&sink, "(%d, %d)", p.x, p.y);
  }

  int x;
  int y;
};

TEST(AbslStringifyTest, BasicTest) {
  Point point;
  point.x = 10;
  point.y = 20;

  std::stringstream ss;
  AbslStringifyPrinter{}(ss, point);
  EXPECT_EQ(ss.str(), "(10, 20)");
}

}  // namespace

}  // namespace ray
