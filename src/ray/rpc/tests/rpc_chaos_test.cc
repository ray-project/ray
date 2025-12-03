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

namespace ray::rpc::testing {

TEST(RpcChaosTest, MethodRpcFailure) {
  RayConfig::instance().testing_rpc_failure() = "method1=0:25:25:25,method2=1:100:0:0";
  Init();
  ASSERT_EQ(GetRpcFailure("unknown"), RpcFailure::None);
  ASSERT_EQ(GetRpcFailure("method1"), RpcFailure::None);
  // At most one failure.
  ASSERT_TRUE(GetRpcFailure("method2") == RpcFailure::Request);
  ASSERT_TRUE(GetRpcFailure("method2") == RpcFailure::None);
}

TEST(RpcChaosTest, MethodRpcFailureEdgeCase) {
  RayConfig::instance().testing_rpc_failure() =
      "method1=1000:100:0:0,method2=1000:0:100:0,method3=1000:0:0:100,method4=1000:0:0:0";
  Init();
  for (int i = 0; i < 1000; i++) {
    ASSERT_EQ(GetRpcFailure("method1"), RpcFailure::Request);
    ASSERT_EQ(GetRpcFailure("method2"), RpcFailure::Response);
    ASSERT_EQ(GetRpcFailure("method3"), RpcFailure::InFlight);
    ASSERT_EQ(GetRpcFailure("method4"), RpcFailure::None);
  }
}

TEST(RpcChaosTest, WildcardRpcFailure) {
  RayConfig::instance().testing_rpc_failure() = "*=-1:100:0:0";
  Init();
  for (int i = 0; i < 100; i++) {
    ASSERT_EQ(GetRpcFailure("method"), RpcFailure::Request);
  }

  RayConfig::instance().testing_rpc_failure() = "*=-1:0:100:0";
  Init();
  for (int i = 0; i < 100; i++) {
    ASSERT_EQ(GetRpcFailure("method"), RpcFailure::Response);
  }

  RayConfig::instance().testing_rpc_failure() = "*=-1:0:0:100";
  Init();
  for (int i = 0; i < 100; i++) {
    ASSERT_EQ(GetRpcFailure("method"), RpcFailure::InFlight);
  }

  RayConfig::instance().testing_rpc_failure() = "*=-1:0:0:0";
  Init();
  for (int i = 0; i < 100; i++) {
    ASSERT_EQ(GetRpcFailure("method"), RpcFailure::None);
  }
}

TEST(RpcChaosTest, LowerBoundWithWildcard) {
  // Test lower bound failures with wildcard configuration
  // Config: unlimited failures,
  //         100% req prob after lower bound, 0% resp prob, 0% resp in-flight prob,
  //         3 guaranteed req failures, 5 guaranteed resp failures, 2 guaranteed resp
  //         in-flight failures
  RayConfig::instance().testing_rpc_failure() = "*=-1:100:0:0:3:5:2";
  Init();

  // First 3 calls should be guaranteed Request failures (lower bound)
  ASSERT_EQ(GetRpcFailure("method1"), RpcFailure::Request);
  ASSERT_EQ(GetRpcFailure("method1"), RpcFailure::Request);
  ASSERT_EQ(GetRpcFailure("method1"), RpcFailure::Request);

  // Next 5 calls should be guaranteed Response failures (lower bound)
  ASSERT_EQ(GetRpcFailure("method1"), RpcFailure::Response);
  ASSERT_EQ(GetRpcFailure("method1"), RpcFailure::Response);
  ASSERT_EQ(GetRpcFailure("method1"), RpcFailure::Response);
  ASSERT_EQ(GetRpcFailure("method1"), RpcFailure::Response);
  ASSERT_EQ(GetRpcFailure("method1"), RpcFailure::Response);

  // Next 2 calls should be guaranteed Response in flight failures (lower bound)
  ASSERT_EQ(GetRpcFailure("method1"), RpcFailure::InFlight);
  ASSERT_EQ(GetRpcFailure("method1"), RpcFailure::InFlight);

  // After lower bounds exhausted, should revert to probabilistic (100% request failures)
  for (int i = 0; i < 100; i++) {
    ASSERT_EQ(GetRpcFailure("method1"), RpcFailure::Request);
  }

  // Test that wildcard applies to any method - method2 should have same behavior
  // First 3 calls should be guaranteed Request failures
  ASSERT_EQ(GetRpcFailure("method2"), RpcFailure::Request);
  ASSERT_EQ(GetRpcFailure("method2"), RpcFailure::Request);
  ASSERT_EQ(GetRpcFailure("method2"), RpcFailure::Request);

  // Next 5 calls should be guaranteed Response failures
  ASSERT_EQ(GetRpcFailure("method2"), RpcFailure::Response);
  ASSERT_EQ(GetRpcFailure("method2"), RpcFailure::Response);
  ASSERT_EQ(GetRpcFailure("method2"), RpcFailure::Response);
  ASSERT_EQ(GetRpcFailure("method2"), RpcFailure::Response);
  ASSERT_EQ(GetRpcFailure("method2"), RpcFailure::Response);

  // Next 2 calls should be guaranteed Response in-flight failures
  ASSERT_EQ(GetRpcFailure("method2"), RpcFailure::InFlight);
  ASSERT_EQ(GetRpcFailure("method2"), RpcFailure::InFlight);

  // After lower bounds exhausted, revert to probabilistic (100% request failures)
  for (int i = 0; i < 100; i++) {
    ASSERT_EQ(GetRpcFailure("method2"), RpcFailure::Request);
  }
}

}  // namespace ray::rpc::testing
