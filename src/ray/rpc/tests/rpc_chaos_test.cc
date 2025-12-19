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
  RayConfig::instance().testing_rpc_failure() =
      R"({"method1":{"num_failures":0,"req_failure_prob":25,"resp_failure_prob":25,"in_flight_failure_prob":25},"method2":{"num_failures":1,"req_failure_prob":100,"resp_failure_prob":0,"in_flight_failure_prob":0}})";
  Init();
  ASSERT_EQ(GetRpcFailure("unknown"), RpcFailure::None);
  ASSERT_EQ(GetRpcFailure("method1"), RpcFailure::None);
  // At most one failure.
  ASSERT_TRUE(GetRpcFailure("method2") == RpcFailure::Request);
  ASSERT_TRUE(GetRpcFailure("method2") == RpcFailure::None);
}

TEST(RpcChaosTest, MethodRpcFailureEdgeCase) {
  RayConfig::instance().testing_rpc_failure() =
      R"({"method1":{"num_failures":1000,"req_failure_prob":100,"resp_failure_prob":0,"in_flight_failure_prob":0},"method2":{"num_failures":1000,"req_failure_prob":0,"resp_failure_prob":100,"in_flight_failure_prob":0},"method3":{"num_failures":1000,"req_failure_prob":0,"resp_failure_prob":0,"in_flight_failure_prob":100},"method4":{"num_failures":1000,"req_failure_prob":0,"resp_failure_prob":0,"in_flight_failure_prob":0}})";
  Init();
  for (int i = 0; i < 1000; i++) {
    ASSERT_EQ(GetRpcFailure("method1"), RpcFailure::Request);
    ASSERT_EQ(GetRpcFailure("method2"), RpcFailure::Response);
    ASSERT_EQ(GetRpcFailure("method3"), RpcFailure::InFlight);
    ASSERT_EQ(GetRpcFailure("method4"), RpcFailure::None);
  }
}

TEST(RpcChaosTest, WildcardRpcFailure) {
  RayConfig::instance().testing_rpc_failure() =
      R"({"*":{"num_failures":-1,"req_failure_prob":100,"resp_failure_prob":0,"in_flight_failure_prob":0}})";
  Init();
  for (int i = 0; i < 100; i++) {
    ASSERT_EQ(GetRpcFailure("method"), RpcFailure::Request);
  }

  RayConfig::instance().testing_rpc_failure() =
      R"({"*":{"num_failures":-1,"req_failure_prob":0,"resp_failure_prob":100,"in_flight_failure_prob":0}})";
  Init();
  for (int i = 0; i < 100; i++) {
    ASSERT_EQ(GetRpcFailure("method"), RpcFailure::Response);
  }

  RayConfig::instance().testing_rpc_failure() =
      R"({"*":{"num_failures":-1,"req_failure_prob":0,"resp_failure_prob":0,"in_flight_failure_prob":100}})";
  Init();
  for (int i = 0; i < 100; i++) {
    ASSERT_EQ(GetRpcFailure("method"), RpcFailure::InFlight);
  }

  RayConfig::instance().testing_rpc_failure() =
      R"({"*":{"num_failures":-1,"req_failure_prob":0,"resp_failure_prob":0,"in_flight_failure_prob":0}})";
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
  RayConfig::instance().testing_rpc_failure() =
      R"({
        "*": {
          "num_failures": -1,
          "req_failure_prob": 100,
          "resp_failure_prob": 0,
          "in_flight_failure_prob": 0,
          "num_lower_bound_req_failures": 3,
          "num_lower_bound_resp_failures": 5,
          "num_lower_bound_in_flight_failures": 2
        }
      })";
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

TEST(RpcChaosTest, TestInvalidJson) {
  RayConfig::instance().testing_rpc_failure() =
      R"({"*":{"num_failures":-1,"invalid_key":1}})";
  ASSERT_DEATH(Init(),
               "Unknown key specified in testing_rpc_failure config: invalid_key");
}

}  // namespace ray::rpc::testing
