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

#include "ray/rpc/rpc_chaos.h"

#include <string>

#include "gtest/gtest.h"
#include "ray/common/ray_config.h"

TEST(RpcChaosTest, Basic) {
  RayConfig::instance().testing_rpc_failure() = "method1=0,method2=1";
  ray::rpc::testing::init();
  ASSERT_EQ(ray::rpc::testing::get_rpc_failure("unknown"),
            ray::rpc::testing::RpcFailure::None);
  ASSERT_EQ(ray::rpc::testing::get_rpc_failure("method1"),
            ray::rpc::testing::RpcFailure::None);
  // At most one failure.
  ASSERT_FALSE(ray::rpc::testing::get_rpc_failure("method2") !=
                   ray::rpc::testing::RpcFailure::None &&
               ray::rpc::testing::get_rpc_failure("method2") !=
                   ray::rpc::testing::RpcFailure::None);
}
