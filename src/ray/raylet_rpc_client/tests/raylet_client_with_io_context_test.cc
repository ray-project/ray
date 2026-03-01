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

#include "ray/raylet_rpc_client/raylet_client_with_io_context.h"

#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>

#include <future>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "ray/common/status.h"
#include "src/ray/protobuf/node_manager.grpc.pb.h"

using ray::rpc::NodeManagerService;

namespace {

class TestNodeManagerService : public NodeManagerService::Service {
 public:
  explicit TestNodeManagerService(bool reply_error) : reply_error_(reply_error) {}

  grpc::Status ResizeLocalResourceInstances(
      grpc::ServerContext *context,
      const ray::rpc::ResizeLocalResourceInstancesRequest *request,
      ray::rpc::ResizeLocalResourceInstancesReply *reply) override {
    if (reply_error_) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "bad request");
    }
    // Echo back the provided resources as total_resources.
    *reply->mutable_total_resources() = request->resources();
    return grpc::Status::OK;
  }

 private:
  bool reply_error_;
};

class InProcessServer {
 public:
  InProcessServer(bool reply_error) : service_(reply_error), selected_port_(0) {
    grpc::ServerBuilder builder;
    builder.AddListeningPort(
        "127.0.0.1:0", grpc::InsecureServerCredentials(), &selected_port_);
    builder.RegisterService(&service_);
    server_ = builder.BuildAndStart();
  }

  ~InProcessServer() { server_->Shutdown(); }

  int port() const { return selected_port_; }

 private:
  TestNodeManagerService service_;
  int selected_port_;
  std::unique_ptr<grpc::Server> server_;
};

}  // namespace

TEST(RayletClientWithIoContextTest, SuccessPath) {
  InProcessServer server(/*reply_error=*/false);
  ray::rpc::RayletClientWithIoContext client("127.0.0.1", server.port());

  std::map<std::string, double> req{{"CPU", 4.0}, {"memory", 100.0}};
  using Result = std::pair<ray::Status, std::optional<std::map<std::string, double>>>;
  auto promise = std::make_shared<std::promise<Result>>();
  client.AsyncResizeLocalResourceInstances(
      req,
      [promise](const ray::Status &status,
                const std::optional<std::map<std::string, double>> &total_resources) {
        promise->set_value(Result{status, total_resources});
      },
      /*timeout_ms*/ 1000);

  auto res = promise->get_future().get();
  EXPECT_TRUE(res.first.ok());
  ASSERT_TRUE(res.second.has_value());
  ASSERT_EQ(res.second->size(), 2U);
  EXPECT_DOUBLE_EQ(res.second->at("CPU"), 4.0);
  EXPECT_DOUBLE_EQ(res.second->at("memory"), 100.0);
}

TEST(RayletClientWithIoContextTest, ErrorPath) {
  InProcessServer server(/*reply_error=*/true);
  ray::rpc::RayletClientWithIoContext client("127.0.0.1", server.port());

  std::map<std::string, double> req{{"CPU", 1.0}};
  using Result = std::pair<ray::Status, std::optional<std::map<std::string, double>>>;
  auto promise = std::make_shared<std::promise<Result>>();
  client.AsyncResizeLocalResourceInstances(
      req,
      [promise](const ray::Status &status,
                const std::optional<std::map<std::string, double>> &total_resources) {
        promise->set_value(Result{status, total_resources});
      },
      /*timeout_ms*/ 1000);

  auto res = promise->get_future().get();
  EXPECT_FALSE(res.first.ok());
  EXPECT_TRUE(res.first.ToString().find("INVALID_ARGUMENT") != std::string::npos ||
              !res.first.message().empty());
  EXPECT_FALSE(res.second.has_value());
}
