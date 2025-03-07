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

#include "ray/util/strings/enum_printer.h"

#include <gtest/gtest.h>

namespace ray {

namespace {
enum PlainEnum {
  ZERO = 0,
  TEN = 10,
};
enum class EnumClass {
  kZero = 0,
  kTen = 10,
};
}  // namespace

TEST(EnumPrinterTest, BasicTest) {
  // C-style enum.
  {
    std::stringstream ss;
    EnumPrinter{}(ss, TEN);
    EXPECT_EQ(ss.str(), "10");
  }
  // C-style enum.
  {
    std::stringstream ss;
    EnumPrinter{}(ss, EnumClass::kTen);
    EXPECT_EQ(ss.str(), "10");
  }
}

}  // namespace ray
