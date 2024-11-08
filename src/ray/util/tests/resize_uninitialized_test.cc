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

#include "src/ray/util/resize_uninitialized.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

namespace ray::utils::strings {

namespace {

TEST(ResizeUninitializedTest, BasicTest) {
  constexpr size_t N = 10000;
  std::string s;

  EXPECT_EQ(s.size(), 0);
  STLStringResizeUninitialized(&s, N);
  EXPECT_EQ(s.size(), N);

  auto *data = s.data();

  STLStringResizeUninitialized(&s, N / 2);
  EXPECT_EQ(s.size(), N / 2);
  EXPECT_EQ(data, s.data());
}

TEST(ResizeUninitializedTest, CreateNewString) {
  constexpr size_t kLen = 100;
  auto s = CreateStringWithSizeUninitialized(kLen);
  EXPECT_EQ(s.length(), kLen);
}

}  // namespace

}  // namespace ray::utils::strings
