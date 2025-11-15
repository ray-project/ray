// Copyright 2021 The Ray Authors.
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

#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include "ray/rpc/grpc_server.h"
#include "src/ray/protobuf/test_service.grpc.pb.h"

namespace ray {
namespace rpc {

class TestServiceHandler {
 public:
  void HandlePing(PingRequest request,
                  PingReply *reply,
                  SendReplyCallback send_reply_callback) {
    RAY_LOG(INFO) << "Got ping request, no_reply=" << request.no_reply();
    request_count++;
    while (frozen) {
      RAY_LOG(INFO) << "Server is frozen...";
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    RAY_LOG(INFO) << "Handling and replying request.";
    if (request.no_reply()) {
      RAY_LOG(INFO) << "No reply!";
      return;
    }
    send_reply_callback(
        ray::Status::OK(),
        /*reply_success=*/[]() { RAY_LOG(INFO) << "Reply success."; },
        /*reply_failure=*/
        [this]() {
          RAY_LOG(INFO) << "Reply failed.";
          reply_failure_count++;
        });
  }

  void HandlePingTimeout(PingTimeoutRequest request,
                         PingTimeoutReply *reply,
                         SendReplyCallback send_reply_callback) {
    while (frozen) {
      RAY_LOG(INFO) << "Server is frozen...";
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    RAY_LOG(INFO) << "Handling and replying request.";
    send_reply_callback(
        ray::Status::OK(),
        /*reply_success=*/[]() { RAY_LOG(INFO) << "Reply success."; },
        /*reply_failure=*/
        [this]() {
          RAY_LOG(INFO) << "Reply failed.";
          reply_failure_count++;
        });
  }

  std::atomic<int> request_count{0};
  std::atomic<int> reply_failure_count{0};
  std::atomic<bool> frozen{false};
};

class TestGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit TestGrpcService(instrumented_io_context &handler_io_service_,
                           TestServiceHandler &handler)
      : GrpcService(handler_io_service_), service_handler_(handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
      const ClusterID &cluster_id,
      const std::optional<AuthenticationToken> &auth_token) override {
    RPC_SERVICE_HANDLER_CUSTOM_AUTH(
        TestService, Ping, /*max_active_rpcs=*/1, ClusterIdAuthType::NO_AUTH);
    RPC_SERVICE_HANDLER_CUSTOM_AUTH(
        TestService, PingTimeout, /*max_active_rpcs=*/1, ClusterIdAuthType::NO_AUTH);
  }

 private:
  /// The grpc async service object.
  TestService::AsyncService service_;
  /// The service handler that actually handle the requests.
  TestServiceHandler &service_handler_;
};

}  // namespace rpc
}  // namespace ray
