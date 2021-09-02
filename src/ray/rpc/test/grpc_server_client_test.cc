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

#include "ray/rpc/grpc_server.h"
#include <chrono>
#include "gtest/gtest.h"
#include "ray/rpc/grpc_client.h"
#include "src/ray/protobuf/test_service.grpc.pb.h"

namespace ray {
namespace rpc {
class TestServiceHandler {
 public:
  void HandleSleep(const SleepRequest &request, SleepReply *reply,
                   SendReplyCallback send_reply_callback) {
    RAY_LOG(INFO) << "Got sleep request, time=" << request.sleep_time_ms() << "ms";
    while (frozen) {
      RAY_LOG(INFO) << "Server is frozen...";
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    RAY_LOG(INFO) << "Handling and replying request.";
    std::this_thread::sleep_for(std::chrono::milliseconds(request.sleep_time_ms()));
    send_reply_callback(ray::Status::OK(),
                        /*reply_success=*/[]() { RAY_LOG(INFO) << "Reply success."; },
                        /*reply_failure=*/
                        [this]() {
                          RAY_LOG(INFO) << "Reply failed.";
                          reply_failure_count++;
                        });
  }

  int reply_failure_count = 0;
  bool frozen = false;
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
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories) override {
    RPC_SERVICE_HANDLER(TestService, Sleep, /*max_active_rpcs=*/1);
  }

 private:
  /// The grpc async service object.
  TestService::AsyncService service_;
  /// The service handler that actually handle the requests.
  TestServiceHandler &service_handler_;
};

class TestGrpcServerClientFixture : public ::testing::Test {
 public:
  void SetUp() {
    // Prepare and start test server.
    handler_thread_ = std::make_unique<std::thread>([this]() {
      /// The asio work to keep handler_io_service_ alive.
      boost::asio::io_service::work handler_io_service_work_(handler_io_service_);
      handler_io_service_.run();
    });
    test_service_.reset(new TestGrpcService(handler_io_service_, test_service_handler_));
    grpc_server_.reset(new GrpcServer("test", 123321));
    grpc_server_->RegisterService(*test_service_);
    grpc_server_->Run();

    // Prepare a client
    client_thread_ = std::make_unique<std::thread>([this]() {
      /// The asio work to keep client_io_service_ alive.
      boost::asio::io_service::work client_io_service_work_(client_io_service_);
      client_io_service_.run();
    });
    client_call_manager_.reset(new ClientCallManager(client_io_service_));
    grpc_client_.reset(
        new GrpcClient<TestService>("localhost", 123321, *client_call_manager_));
  }

  void TearDown() {
    // Cleanup stuffs.
    grpc_client_.reset();
    client_call_manager_.reset();
    client_io_service_.stop();
    if (client_thread_->joinable()) {
      client_thread_->join();
    }

    grpc_server_->Shutdown();
    handler_io_service_.stop();
    if (handler_thread_->joinable()) {
      handler_thread_->join();
    }
  }

 protected:
  VOID_RPC_CLIENT_METHOD(TestService, Sleep, grpc_client_, )
  // Server
  TestServiceHandler test_service_handler_;
  instrumented_io_context handler_io_service_;
  std::unique_ptr<std::thread> handler_thread_;
  std::unique_ptr<TestGrpcService> test_service_;
  std::unique_ptr<GrpcServer> grpc_server_;
  grpc::CompletionQueue cq_;
  std::shared_ptr<grpc::Channel> channel_;
  // Client
  instrumented_io_context client_io_service_;
  std::unique_ptr<std::thread> client_thread_;
  std::unique_ptr<ClientCallManager> client_call_manager_;
  std::unique_ptr<GrpcClient<TestService>> grpc_client_;
};

TEST_F(TestGrpcServerClientFixture, TestBasic) {
  // Send request
  SleepRequest request;
  request.set_sleep_time_ms(1);
  bool done = false;
  Sleep(request, [&done](const Status status, const SleepReply &reply) {
    RAY_LOG(INFO) << "replied, status=" << status;
    done = true;
  });
  while (!done) {
    RAY_LOG(INFO) << "waiting";
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  }
}

TEST_F(TestGrpcServerClientFixture, TestClientCallManagerTimeout) {
  // Reinit ClientCallManager with short timeout.
  grpc_client_.reset();
  client_call_manager_.reset();
  client_call_manager_.reset(new ClientCallManager(client_io_service_, /*num_thread=*/1,
                                                   /*call_timeout_ms=*/100));
  grpc_client_.reset(
      new GrpcClient<TestService>("localhost", 123321, *client_call_manager_));
  // Freeze server first, it won't reply any request.
  test_service_handler_.frozen = true;
  // Send request.
  SleepRequest request;
  request.set_sleep_time_ms(1);
  bool call_timed_out = false;
  Sleep(request, [&call_timed_out](const Status status, const SleepReply &reply) {
    RAY_LOG(INFO) << "Replied, status=" << status;
    ASSERT_TRUE(status.IsIOError());
    call_timed_out = true;
  });
  // Wait for clinet call timed out.
  while (!call_timed_out) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  }
  // Unfreeze server so the server thread can exit.
  test_service_handler_.frozen = false;
  while (test_service_handler_.reply_failure_count <= 0) {
    RAY_LOG(INFO) << "Waiting for reply failure";
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  }
}

// This test aims to test ServerCall leaking when client died before server's reply.
// Check https://github.com/ray-project/ray/pull/17863 for more context
TEST_F(TestGrpcServerClientFixture, TestClientDiedBeforeReply) {
  // Reinit ClientCallManager with short timeout, so that call won't block.
  grpc_client_.reset();
  client_call_manager_.reset();
  client_call_manager_.reset(new ClientCallManager(client_io_service_, /*num_thread=*/1,
                                                   /*call_timeout_ms=*/100));
  grpc_client_.reset(
      new GrpcClient<TestService>("localhost", 123321, *client_call_manager_));
  // Freeze server first, it won't reply any request.
  test_service_handler_.frozen = true;
  // Send request.
  SleepRequest request;
  request.set_sleep_time_ms(1);
  bool call_timed_out = false;
  Sleep(request, [&call_timed_out](const Status status, const SleepReply &reply) {
    RAY_LOG(INFO) << "Replied, status=" << status;
    ASSERT_TRUE(status.IsIOError());
    call_timed_out = true;
  });
  // Wait for clinet call timed out. Client socket won't be closed until all calls are
  // timed out.
  while (!call_timed_out) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  }
  // Shutdown client before server replies.
  grpc_client_.reset();
  client_call_manager_.reset();
  // Unfreeze server, server will fail to reply.
  test_service_handler_.frozen = false;
  while (test_service_handler_.reply_failure_count <= 0) {
    RAY_LOG(INFO) << "Waiting for reply failure";
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  }
  // Reinit client with infinite timeout.
  client_call_manager_.reset(new ClientCallManager(client_io_service_));
  grpc_client_.reset(
      new GrpcClient<TestService>("localhost", 123321, *client_call_manager_));
  // Send again, this request should be replied. If any leaking happened, this call won't
  // be replied to since the max_active_rpcs is 1.
  bool done = false;
  Sleep(request, [&done](const Status status, const SleepReply &reply) {
    RAY_LOG(INFO) << "replied, status=" << status;
    done = true;
  });
  while (!done) {
    RAY_LOG(INFO) << "waiting";
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  }
}
}  // namespace rpc
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
