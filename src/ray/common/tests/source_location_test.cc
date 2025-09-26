// Copyright 2024 The Ray Authors.
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

#include "ray/common/source_location.h"

#include <gtest/gtest.h>

#include <sstream>

namespace ray {

namespace {

TEST(SourceLocationTest, StringifyTest) {
  // Default source location.
  {
    std::stringstream ss{};
    ss << SourceLocation();
    EXPECT_EQ(ss.str(), "");
  }

  // Initialized source location.
  {
    auto loc = RAY_LOC();
    std::stringstream ss{};
    ss << loc;
    EXPECT_EQ(ss.str(), "src/ray/common/tests/source_location_test.cc:35");
  }
}

}  // namespace

}  // namespace ray
