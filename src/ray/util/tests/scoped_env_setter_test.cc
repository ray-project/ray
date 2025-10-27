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

#include "ray/util/scoped_env_setter.h"

#include <gtest/gtest.h>

#include <string_view>

namespace ray {

namespace {

constexpr std::string_view kEnvKey = "key";
constexpr std::string_view kEnvVal = "val";

TEST(ScopedEnvSetter, BasicTest) {
  EXPECT_EQ(::getenv(kEnvKey.data()), nullptr);

  {
    ScopedEnvSetter env_setter{kEnvKey.data(), kEnvVal.data()};
    EXPECT_STREQ(::getenv(kEnvKey.data()), kEnvVal.data());
  }

  EXPECT_EQ(::getenv(kEnvKey.data()), nullptr);
}

}  // namespace

}  // namespace ray
