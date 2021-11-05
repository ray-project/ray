// Copyright 2017 The Ray Authors.
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

#include "ray/common/asio/asio_chaos.h"
#include "ray/common/ray_config.h"
#include "gtest/gtest.h"
#include <string>

bool EnsureBelow(const std::string& method_name, int64_t max_val) {
  for(int i = 0; i < 1000; ++i) {
    if(ray::asio::testing::get_delay_ms(method_name) > max_val) {
      return false;
    }
  }
  return true;
}

TEST(AsioChaosTest, Basic) {
  RayConfig::instance().testing_asio_delay_ms() = "method1=10,method2=20";
  ASSERT_TRUE(EnsureBelow("method1", 10));
  ASSERT_TRUE(EnsureBelow("method2", 20));
}

TEST(AsioChaosTest, WithGlobal) {
  RayConfig::instance().testing_asio_delay_ms() = "method1=10,method2=20,*=100";
  ASSERT_TRUE(EnsureBelow("method1", 10));
  ASSERT_TRUE(EnsureBelow("method2", 20));
  ASSERT_TRUE(EnsureBelow("others", 100));
}
