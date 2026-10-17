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

#include "ray/object_manager_rpc_client/object_manager_client.h"

#include <chrono>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <thread>

#include "gtest/gtest.h"
#include "ray/asio/instrumented_io_context.h"
#include "ray/common/ray_config.h"
#include "ray/rpc/grpc_server.h"

namespace ray {
namespace rpc {
namespace {

class NoReplyPushService : public ObjectManagerService::Service {
 public:
  ::grpc::Status Push(::grpc::ServerContext *,
                      const PushRequest *,
                      PushReply *) override {
    std::unique_lock<std::mutex> lock(mutex_);
    request_received_ = true;
    condition_.notify_all();
    condition_.wait(lock, [this] { return released_; });
    return ::grpc::Status::OK;
  }

  ::grpc::Status Pull(::grpc::ServerContext *,
                      const PullRequest *,
                      PullReply *) override {
    return ::grpc::Status::OK;
  }

  bool WaitForRequest(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mutex_);
    return condition_.wait_for(lock, timeout, [this] { return request_received_; });
  }

  void Release() {
    std::unique_lock<std::mutex> lock(mutex_);
    released_ = true;
    condition_.notify_all();
  }

 private:
  std::condition_variable condition_;
  std::mutex mutex_;
  bool released_ = false;
  bool request_received_ = false;
};

TEST(ObjectManagerClientTest, PushUsesConfiguredDeadline) {
  RayConfig::instance().initialize(R"({"object_manager_push_rpc_timeout_ms": 100})");

  NoReplyPushService *service = nullptr;
  GrpcServer server("ObjectManagerClientTest", 0, /*listen_to_localhost_only=*/true);
  auto no_reply_service = std::make_unique<NoReplyPushService>();
  service = no_reply_service.get();
  server.RegisterService(std::move(no_reply_service));
  server.Run();
  for (int attempt = 0; attempt < 200 && server.GetPort() == 0; attempt++) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  ASSERT_NE(server.GetPort(), 0);

  instrumented_io_context client_io_service(/*enable_lag_probe=*/false,
                                            /*running_on_single_thread=*/true);
  auto client_work = boost::asio::make_work_guard(client_io_service.get_executor());
  std::thread client_thread([&client_io_service] { client_io_service.run(); });
  auto client_call_manager = std::make_unique<ClientCallManager>(
      client_io_service, /*record_stats=*/false, /*local_address=*/"");
  auto client = std::make_unique<ObjectManagerClient>(
      "127.0.0.1", server.GetPort(), *client_call_manager);

  std::promise<Status> call_status_promise;
  auto call_status_future = call_status_promise.get_future();
  client->Push(PushRequest{},
               [&call_status_promise](const Status &status, const PushReply &reply) {
                 call_status_promise.set_value(status);
               });

  const bool request_received = service->WaitForRequest(std::chrono::seconds(2));
  const bool call_completed =
      call_status_future.wait_for(std::chrono::seconds(2)) == std::future_status::ready;
  service->Release();
  if (!call_completed) {
    EXPECT_EQ(call_status_future.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
  }

  client.reset();
  client_call_manager.reset();
  client_work.reset();
  client_io_service.stop();
  client_thread.join();
  server.Shutdown();

  ASSERT_TRUE(request_received);
  ASSERT_TRUE(call_completed);
  EXPECT_TRUE(call_status_future.get().IsTimedOut());
}

}  // namespace
}  // namespace rpc
}  // namespace ray
