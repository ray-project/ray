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

#include "ray/common/status.h"

#include "gtest/gtest.h"
#include "ray/common/grpc_util.h"

namespace ray {
class StatusTest : public ::testing::Test {};

TEST_F(StatusTest, StringToCode) {
  auto ok = Status::OK();
  StatusCode status = Status::StringToCode(ok.CodeAsString());
  ASSERT_EQ(status, StatusCode::OK);

  auto invalid = Status::Invalid("invalid");
  status = Status::StringToCode(invalid.CodeAsString());
  ASSERT_EQ(status, StatusCode::Invalid);

  auto object_store_full = Status::TransientObjectStoreFull("full");
  status = Status::StringToCode(object_store_full.CodeAsString());
  ASSERT_EQ(status, StatusCode::TransientObjectStoreFull);

  ASSERT_EQ(Status::StringToCode("foobar"), StatusCode::IOError);
}

TEST_F(StatusTest, GrpcStatusToRayStatus) {
  const Status ok = Status::OK();
  auto grpc_status = RayStatusToGrpcStatus(ok);
  ASSERT_TRUE(GrpcStatusToRayStatus(grpc_status).ok());

  const Status invalid = Status::Invalid("not now");
  grpc_status = RayStatusToGrpcStatus(invalid);
  auto ray_status = GrpcStatusToRayStatus(grpc_status);
  ASSERT_TRUE(ray_status.IsInvalid());
  ASSERT_EQ(ray_status.message(), "not now");

  grpc_status = grpc::Status(grpc::StatusCode::UNAVAILABLE, "foo", "bar");
  ray_status = GrpcStatusToRayStatus(grpc_status);
  ASSERT_TRUE(ray_status.IsRpcError());
  ASSERT_EQ(ray_status.rpc_code(), grpc::StatusCode::UNAVAILABLE);

  grpc_status = grpc::Status(grpc::StatusCode::UNKNOWN, "foo", "bar");
  ray_status = GrpcStatusToRayStatus(grpc_status);
  ASSERT_TRUE(ray_status.IsRpcError());
  ASSERT_EQ(ray_status.rpc_code(), grpc::StatusCode::UNKNOWN);

  grpc_status = grpc::Status(grpc::StatusCode::ABORTED, "foo", "bar");
  ray_status = GrpcStatusToRayStatus(grpc_status);
  ASSERT_TRUE(ray_status.IsIOError());
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
