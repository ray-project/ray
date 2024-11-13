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

#include "src/ray/util/size_literals.h"

#include <gtest/gtest.h>

namespace ray::literals {

namespace {

TEST(SizeLiteralsTest, BasicTest) {
  static_assert(2_MiB == 2 * 1024 * 1024);
  static_assert(2.5_KB == 2500);
  static_assert(4_GB == 4'000'000'000);
}

}  // namespace

}  // namespace ray::literals
