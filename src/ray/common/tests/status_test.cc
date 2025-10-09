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

#include <utility>

#include "gtest/gtest.h"
#include "ray/common/grpc_util.h"

namespace ray {

TEST(StatusTest, CopyAndMoveForOkStatus) {
  // OK status.
  Status ok_status = Status::OK();

  // Copy constructor.
  {
    Status new_status = ok_status;
    EXPECT_TRUE(new_status.ok());
  }
  // Copy assignment.
  {
    Status new_status = Status::Invalid("invalid");
    new_status = ok_status;
    EXPECT_TRUE(new_status.ok());
  }

  // Move constructor.
  Status copied_ok_status = ok_status;
  {
    Status new_status = std::move(ok_status);
    EXPECT_TRUE(new_status.ok());
  }
  // Move assignment.
  {
    Status new_status = Status::Invalid("invalid");
    new_status = std::move(copied_ok_status);
    EXPECT_TRUE(new_status.ok());
  }
}

TEST(StatusTest, CopyAndMoveErrorStatus) {
  // Invalid status.
  Status invalid_status = Status::Invalid("invalid");

  // Copy constructor.
  {
    Status new_status = invalid_status;
    EXPECT_EQ(new_status.code(), StatusCode::Invalid);
  }
  // Copy assignment.
  {
    Status new_status = Status::OK();
    new_status = invalid_status;
    EXPECT_EQ(new_status.code(), StatusCode::Invalid);
  }

  // Move constructor.
  Status copied_invalid_status = invalid_status;
  {
    Status new_status = std::move(invalid_status);
    EXPECT_EQ(new_status.code(), StatusCode::Invalid);
  }
  // Move assignment.
  {
    Status new_status = Status::OK();
    new_status = std::move(copied_invalid_status);
    EXPECT_EQ(new_status.code(), StatusCode::Invalid);
  }
}

TEST(StatusTest, StringToCode) {
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

TEST(StatusTest, GrpcStatusToRayStatus) {
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

TEST(StatusSetTest, TestStatusSetAPI) {
  auto return_status_oom = []() -> StatusSet<StatusT::IOError, StatusT::OutOfMemory> {
    return StatusT::OutOfMemory("ooming because Ray Data is making too many objects");
  };
  auto error_status = return_status_oom();
  ASSERT_FALSE(error_status.ok());
  ASSERT_TRUE(error_status.has_error());
  bool hit_correct_visitor = false;
  std::visit(overloaded{[](const StatusT::IOError &) {},
                        [&](const StatusT::OutOfMemory &oom_status) {
                          ASSERT_EQ(oom_status.message(),
                                    "ooming because Ray Data is making too many objects");
                          hit_correct_visitor = true;
                        }},
             error_status.error());
  ASSERT_TRUE(hit_correct_visitor);

  auto return_status_ok = []() -> StatusSet<StatusT::IOError, StatusT::OutOfMemory> {
    return StatusT::OK();
  };
  auto status_ok = return_status_ok();
  ASSERT_TRUE(status_ok.ok());
  ASSERT_FALSE(status_ok.has_error());
}

TEST(StatusSetOrTest, TestStatusSetOrAPI) {
  auto return_status_oom =
      []() -> StatusSetOr<int64_t, StatusT::IOError, StatusT::OutOfMemory> {
    return StatusT::OutOfMemory("ooming because Ray Data is making too many objects");
  };
  auto error_result = return_status_oom();
  ASSERT_FALSE(error_result.has_value());
  ASSERT_TRUE(error_result.has_error());
  bool hit_correct_visitor = false;
  std::visit(overloaded{[](const StatusT::IOError &) {},
                        [&](const StatusT::OutOfMemory &oom_status) {
                          ASSERT_EQ(oom_status.message(),
                                    "ooming because Ray Data is making too many objects");
                          hit_correct_visitor = true;
                        }},
             error_result.error());
  ASSERT_TRUE(hit_correct_visitor);

  auto return_value =
      []() -> StatusSetOr<int64_t, StatusT::IOError, StatusT::OutOfMemory> {
    return 100;
  };
  auto result = return_value();
  ASSERT_TRUE(result.has_value());
  ASSERT_FALSE(result.has_error());
  ASSERT_TRUE(result.value() == 100);
}

}  // namespace ray
