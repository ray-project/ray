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

#include "gtest/gtest.h"
#include "ray/common/ray_config.h"

TEST(RpcChaosTest, Basic) {
  RayConfig::instance().testing_rpc_failure() = "method1=0:25:25,method2=1:25:25";
  ray::rpc::testing::Init();
  ASSERT_EQ(ray::rpc::testing::GetRpcFailure("unknown"),
            ray::rpc::testing::RpcFailure::None);
  ASSERT_EQ(ray::rpc::testing::GetRpcFailure("method1"),
            ray::rpc::testing::RpcFailure::None);
  // At most one failure.
  ASSERT_FALSE(ray::rpc::testing::GetRpcFailure("method2") !=
                   ray::rpc::testing::RpcFailure::None &&
               ray::rpc::testing::GetRpcFailure("method2") !=
                   ray::rpc::testing::RpcFailure::None);
}

TEST(RpcChaosTest, EdgeCaseProbability) {
  RayConfig::instance().testing_rpc_failure() =
      "method1=1000:100:0,method2=1000:0:100,method3=1000:0:0";
  ray::rpc::testing::Init();
  for (int i = 0; i < 1000; i++) {
    ASSERT_EQ(ray::rpc::testing::GetRpcFailure("method1"),
              ray::rpc::testing::RpcFailure::Request);
    ASSERT_EQ(ray::rpc::testing::GetRpcFailure("method2"),
              ray::rpc::testing::RpcFailure::Response);
    ASSERT_EQ(ray::rpc::testing::GetRpcFailure("method3"),
              ray::rpc::testing::RpcFailure::None);
  }
}
