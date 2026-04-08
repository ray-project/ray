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

// Integration test for the custom gRPC health check service that dispatches
// through the io_context. Verifies that:
// 1. Health checks succeed when the io_context is running.
// 2. Health checks time out when the io_context is blocked.

#include <grpcpp/grpcpp.h>

#include <chrono>
#include <memory>
#include <thread>

#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/grpc_services.h"
#include "ray/rpc/grpc_server.h"
#include "src/proto/grpc/health/v1/health.grpc.pb.h"

namespace ray {
namespace {

class GcsHealthCheckServiceTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create a GrpcServer with the default gRPC health check disabled,
    // mimicking how GcsServer is constructed.
    server_ =
        std::make_unique<rpc::GrpcServer>("TestHealthCheckServer",
                                          /*port=*/0,
                                          /*listen_to_localhost_only=*/true,
                                          /*num_threads=*/1,
                                          /*keepalive_time_ms=*/7200000,
                                          /*auth_token=*/nullptr,
                                          /*enable_default_health_check_service=*/false);

    // Register the custom health check service on our io_context.
    server_->RegisterService(std::make_unique<rpc::HealthCheckGrpcService>(io_context_),
                             /*token_auth=*/false);
    server_->Run();

    // Start running the io_context in a background thread.
    io_thread_ = std::make_unique<std::thread>([this] {
      boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work(
          io_context_.get_executor());
      io_context_.run();
    });

    // Create a channel to the server.
    auto channel = grpc::CreateChannel("localhost:" + std::to_string(server_->GetPort()),
                                       grpc::InsecureChannelCredentials());
    stub_ = grpc::health::v1::Health::NewStub(channel);
  }

  void TearDown() override {
    io_context_.stop();
    if (io_thread_ && io_thread_->joinable()) {
      io_thread_->join();
    }
    server_->Shutdown();
  }

  // Perform a health check with the given deadline.
  grpc::Status CheckHealth(std::chrono::milliseconds timeout) {
    grpc::health::v1::HealthCheckRequest request;
    grpc::health::v1::HealthCheckResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + timeout);
    auto status = stub_->Check(&context, request, &response);
    if (status.ok()) {
      EXPECT_EQ(response.status(), grpc::health::v1::HealthCheckResponse::SERVING);
    }
    return status;
  }

  instrumented_io_context io_context_;
  std::unique_ptr<rpc::GrpcServer> server_;
  std::unique_ptr<std::thread> io_thread_;
  std::unique_ptr<grpc::health::v1::Health::Stub> stub_;
};

TEST_F(GcsHealthCheckServiceTest, HealthCheckSucceeds) {
  // The io_context is running, so the health check should succeed.
  auto status = CheckHealth(std::chrono::milliseconds(5000));
  ASSERT_TRUE(status.ok()) << "Health check failed: " << status.error_message();
}

TEST_F(GcsHealthCheckServiceTest, HealthCheckTimesOutWhenIoContextBlocked) {
  // Stop the io_context so it no longer processes handlers.
  io_context_.stop();
  io_thread_->join();
  io_thread_.reset();

  // The health check should time out because the handler cannot be dispatched.
  auto status = CheckHealth(std::chrono::milliseconds(500));
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), grpc::StatusCode::DEADLINE_EXCEEDED);
}

TEST_F(GcsHealthCheckServiceTest, HealthCheckRecoveryAfterRestart) {
  // Stop the io_context.
  io_context_.stop();
  io_thread_->join();
  io_thread_.reset();

  // Verify the health check fails while stopped.
  auto status = CheckHealth(std::chrono::milliseconds(500));
  ASSERT_FALSE(status.ok());

  // Restart the io_context.
  io_context_.restart();
  io_thread_ = std::make_unique<std::thread>([this] {
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work(
        io_context_.get_executor());
    io_context_.run();
  });

  // Health check should succeed again.
  status = CheckHealth(std::chrono::milliseconds(5000));
  ASSERT_TRUE(status.ok()) << "Health check failed after restart: "
                           << status.error_message();
}

}  // namespace
}  // namespace ray
