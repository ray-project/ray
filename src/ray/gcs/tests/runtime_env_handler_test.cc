// Copyright 2026 The Ray Authors.
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

#include "ray/gcs/runtime_env_handler.h"

#include <gtest/gtest.h>

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/runtime_env_manager.h"

namespace ray {
namespace gcs {

class RuntimeEnvHandlerTest : public ::testing::Test {
 public:
  RuntimeEnvHandlerTest() : io_service_("RuntimeEnvHandlerTest") {
    // RuntimeEnvManager deleter that does nothing (we don't need actual cleanup).
    runtime_env_manager_ =
        std::make_unique<RuntimeEnvManager>([](const std::string &, std::function<void(bool)> cb) {
          cb(true);
        });
  }

 protected:
  instrumented_io_context io_service_;
  std::unique_ptr<RuntimeEnvManager> runtime_env_manager_;
  // Track delay_executor calls: stores (callback, delay_ms) pairs.
  std::vector<std::pair<std::function<void()>, uint32_t>> delayed_calls_;

  std::unique_ptr<RuntimeEnvHandler> MakeHandler() {
    return std::make_unique<RuntimeEnvHandler>(
        io_service_,
        *runtime_env_manager_,
        [this](std::function<void()> fn,
               uint32_t delay_ms) -> std::shared_ptr<boost::asio::deadline_timer> {
          delayed_calls_.emplace_back(std::move(fn), delay_ms);
          return nullptr;
        });
  }

  static rpc::SendReplyCallback MakeReplyCallback(Status *out_status) {
    return [out_status](const Status &status,
                        std::function<void()>,
                        std::function<void()>) { *out_status = status; };
  }
};

TEST_F(RuntimeEnvHandlerTest, TestPinRuntimeEnvURI_HappyPath) {
  auto handler = MakeHandler();

  rpc::PinRuntimeEnvURIRequest request;
  request.set_uri("s3://bucket/my_env.zip");
  request.set_expiration_s(60);

  rpc::PinRuntimeEnvURIReply reply;
  Status status;
  handler->HandlePinRuntimeEnvURI(request, &reply, MakeReplyCallback(&status));

  // Reply should be OK.
  ASSERT_TRUE(status.ok());
  // A delayed removal should have been scheduled.
  ASSERT_EQ(delayed_calls_.size(), 1);
  ASSERT_EQ(delayed_calls_[0].second, 60 * 1000);  // expiration_s * 1000
}

TEST_F(RuntimeEnvHandlerTest, TestPinRuntimeEnvURI_ZeroExpiration) {
  auto handler = MakeHandler();

  rpc::PinRuntimeEnvURIRequest request;
  request.set_uri("s3://bucket/my_env.zip");
  request.set_expiration_s(0);

  rpc::PinRuntimeEnvURIReply reply;
  Status status;
  handler->HandlePinRuntimeEnvURI(request, &reply, MakeReplyCallback(&status));

  // Reply should be OK.
  ASSERT_TRUE(status.ok());
  // Even with expiration_s=0, the delay executor is still called (with delay_ms=0).
  ASSERT_EQ(delayed_calls_.size(), 1);
  ASSERT_EQ(delayed_calls_[0].second, 0);
}

TEST_F(RuntimeEnvHandlerTest, TestPinRuntimeEnvURI_DelayedRemoval) {
  auto handler = MakeHandler();

  rpc::PinRuntimeEnvURIRequest request;
  request.set_uri("s3://bucket/my_env.zip");
  request.set_expiration_s(10);

  rpc::PinRuntimeEnvURIReply reply;
  Status status;
  handler->HandlePinRuntimeEnvURI(request, &reply, MakeReplyCallback(&status));
  ASSERT_TRUE(status.ok());

  // Before expiration fires, the URI reference should exist.
  // (We can't directly check ref count, but we can verify the delayed callback runs
  // without crashing.)
  ASSERT_EQ(delayed_calls_.size(), 1);

  // Fire the delayed callback (simulates expiration).
  delayed_calls_[0].first();
  // Should not crash -- the URI reference is removed.
}

TEST_F(RuntimeEnvHandlerTest, TestPinRuntimeEnvURI_MultipleURIs) {
  auto handler = MakeHandler();

  // Pin two different URIs.
  for (const auto &uri : {"s3://bucket/env1.zip", "s3://bucket/env2.zip"}) {
    rpc::PinRuntimeEnvURIRequest request;
    request.set_uri(uri);
    request.set_expiration_s(30);

    rpc::PinRuntimeEnvURIReply reply;
    Status status;
    handler->HandlePinRuntimeEnvURI(request, &reply, MakeReplyCallback(&status));
    ASSERT_TRUE(status.ok());
  }

  ASSERT_EQ(delayed_calls_.size(), 2);
  // Fire both expirations.
  delayed_calls_[0].first();
  delayed_calls_[1].first();
}

}  // namespace gcs
}  // namespace ray
