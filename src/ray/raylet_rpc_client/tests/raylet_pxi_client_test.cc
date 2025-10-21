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

#include "ray/raylet_rpc_client/raylet_pxi_client.h"

#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <string>

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

TEST(RayletPXIClientTest, SuccessPath) {
  InProcessServer server(/*reply_error=*/false);
  ray::rpc::RayletPXIClient client("127.0.0.1", server.port());

  std::map<std::string, double> req{{"CPU", 4.0}, {"memory", 100.0}};
  auto res = client.ResizeLocalResourceInstances(req);

  EXPECT_EQ(res.status_code, 0);
  ASSERT_EQ(res.total_resources.size(), 2U);
  EXPECT_DOUBLE_EQ(res.total_resources["CPU"], 4.0);
  EXPECT_DOUBLE_EQ(res.total_resources["memory"], 100.0);
}

TEST(RayletPXIClientTest, ErrorPath) {
  InProcessServer server(/*reply_error=*/true);
  ray::rpc::RayletPXIClient client("127.0.0.1", server.port());

  std::map<std::string, double> req{{"CPU", 1.0}};
  auto res = client.ResizeLocalResourceInstances(req);

  EXPECT_NE(res.status_code, 0);
  EXPECT_TRUE(res.message.find("INVALID_ARGUMENT") != std::string::npos ||
              !res.message.empty());
  EXPECT_TRUE(res.total_resources.empty());
}
