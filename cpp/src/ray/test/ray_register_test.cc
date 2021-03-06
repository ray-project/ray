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

#include <gtest/gtest.h>
#include <ray/api.h>
#include <ray/api/serializer.h>
#include <ray/runtime/task/task_executor.h>

using namespace ray::api;
using namespace ray::internal;

int Return() { return 1; }
int PlusOne(int x) { return x + 1; }

RAY_REMOTE(PlusOne);

TEST(RayApiTest, NormalTask) {
  Ray::Init();
  ray::api::RayConfig::GetInstance()->use_ray_remote = true;
  auto r = Ray::Task(PlusOne).Remote(2);
  EXPECT_EQ(3, *(r.Get()));
  ray::api::RayConfig::GetInstance()->use_ray_remote = false;
}

TEST(RayApiTest, DuplicateRegister) {
  bool r = FunctionManager::Instance().RegisterRemoteFunction("Return", Return);
  EXPECT_TRUE(r);

  /// Duplicate register
  bool r1 = FunctionManager::Instance().RegisterRemoteFunction("Return", Return);
  EXPECT_FALSE(r1);

  bool r2 = FunctionManager::Instance().RegisterRemoteFunction("PlusOne", PlusOne);
  EXPECT_FALSE(r2);
}

TEST(RayApiTest, FindAndExecuteFunction) {
  /// Find and call the registered function.
  auto args = std::make_tuple("PlusOne", 1);
  auto buf = Serializer::Serialize(args);
  auto result_buf = TaskExecutionHandler(buf.data(), buf.size());

  /// Deserialize result.
  auto response =
      Serializer::Deserialize<Response<int>>(result_buf.data(), result_buf.size());

  EXPECT_EQ(response.error_code, ErrorCode::OK);
  EXPECT_EQ(response.data, 2);
}

TEST(RayApiTest, VoidFunction) {
  auto buf1 = Serializer::Serialize(std::make_tuple("Return"));
  auto result_buf = TaskExecutionHandler(buf1.data(), buf1.size());
  auto response =
      Serializer::Deserialize<VoidResponse>(result_buf.data(), result_buf.size());
  EXPECT_EQ(response.error_code, ErrorCode::OK);
}

/// We should consider the driver so is not same with the worker so, and find the error
/// reason.
TEST(RayApiTest, NotExistFunction) {
  auto buf2 = Serializer::Serialize(std::make_tuple("Return11"));
  auto result_buf = TaskExecutionHandler(buf2.data(), buf2.size());
  auto response =
      Serializer::Deserialize<VoidResponse>(result_buf.data(), result_buf.size());
  EXPECT_EQ(response.error_code, ErrorCode::FAIL);
  EXPECT_FALSE(response.error_msg.empty());
}

TEST(RayApiTest, ArgumentsNotMatch) {
  auto buf = Serializer::Serialize(std::make_tuple("PlusOne", "invalid arguments"));
  auto result_buf = TaskExecutionHandler(buf.data(), buf.size());
  auto response =
      Serializer::Deserialize<Response<int>>(result_buf.data(), result_buf.size());
  EXPECT_EQ(response.error_code, ErrorCode::FAIL);
  EXPECT_FALSE(response.error_msg.empty());
}
