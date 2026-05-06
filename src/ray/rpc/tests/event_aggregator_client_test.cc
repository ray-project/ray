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

#include "ray/rpc/event_aggregator_client.h"

#include <memory>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace ray {
namespace rpc {

constexpr int kCountToReturnOk = 3;
constexpr int kRetryIntervalMs = 100;

class TestableEventAggregatorClientImpl : public EventAggregatorClientImpl {
 public:
  TestableEventAggregatorClientImpl(ClientCallManager &client_call_manager,
                                    int count_to_return_ok)
      : EventAggregatorClientImpl(client_call_manager),
        count_to_return_ok_(count_to_return_ok) {}

  void CheckServerReady(const ClientCallback<AddEventsReply> &callback) override {
    readiness_check_count_++;
    if (readiness_check_count_ <= count_to_return_ok_) {
      callback(Status::RpcError("Failed to connect to the event aggregator server.", 14),
               AddEventsReply());
    } else {
      callback(Status::OK(), AddEventsReply());
    }
  }

 private:
  int count_to_return_ok_;
  int readiness_check_count_ = 1;
};

class DeferredCallbackEventAggregatorClientImpl : public EventAggregatorClientImpl {
 public:
  using EventAggregatorClientImpl::EventAggregatorClientImpl;

  void CheckServerReady(const ClientCallback<AddEventsReply> &callback) override {
    pending_callbacks_.push_back(callback);
  }

  void InvokeAllPendingCallbacksWithOk() {
    for (auto &callback : pending_callbacks_) {
      callback(Status::OK(), AddEventsReply());
    }
    pending_callbacks_.clear();
  }

  std::vector<ClientCallback<AddEventsReply>> pending_callbacks_;
};

class EventAggregatorClientTest : public ::testing::Test {
 protected:
  void SetUp() override {
    client_call_manager_ = std::make_unique<ClientCallManager>(
        io_service_, /*record_stats=*/true, /*local_address=*/"");
    client_ = std::make_unique<TestableEventAggregatorClientImpl>(*client_call_manager_,
                                                                  kCountToReturnOk);
  }

  instrumented_io_context io_service_;
  std::unique_ptr<EventAggregatorClientImpl> client_;
  std::unique_ptr<ClientCallManager> client_call_manager_;
};

TEST_F(EventAggregatorClientTest, WaitForServerReadyWithRetrySuccess) {
  client_->WaitForServerReadyWithRetry(
      [](const Status &server_status) { ASSERT_TRUE(server_status.ok()); },
      0,
      kCountToReturnOk,
      kRetryIntervalMs);
  io_service_.run_for(std::chrono::milliseconds(kCountToReturnOk * kRetryIntervalMs));
  ASSERT_TRUE(client_->exporter_initialized_.load());
}

TEST_F(EventAggregatorClientTest, WaitForServerReadyWithRetryFailure) {
  client_->WaitForServerReadyWithRetry(
      [](const Status &server_status) { ASSERT_FALSE(server_status.ok()); },
      0,
      kCountToReturnOk - 2,
      kRetryIntervalMs);
  io_service_.run_for(std::chrono::milliseconds(kCountToReturnOk * kRetryIntervalMs));
  ASSERT_FALSE(client_->exporter_initialized_.load());
}

TEST_F(EventAggregatorClientTest, ConcurrentCallbacksCallInitExporterFnOnlyOnce) {
  auto deferred_client =
      std::make_unique<DeferredCallbackEventAggregatorClientImpl>(*client_call_manager_);

  int init_call_count = 0;
  auto init_fn = [&init_call_count](const Status &status) { init_call_count++; };

  deferred_client->WaitForServerReadyWithRetry(init_fn, 0, 10, kRetryIntervalMs);
  deferred_client->WaitForServerReadyWithRetry(init_fn, 0, 10, kRetryIntervalMs);

  deferred_client->InvokeAllPendingCallbacksWithOk();

  ASSERT_EQ(init_call_count, 1);
  ASSERT_TRUE(deferred_client->exporter_initialized_.load());
}

TEST_F(EventAggregatorClientTest, ExhaustedRetriesReturnsFailure) {
  auto always_fail_client =
      std::make_unique<TestableEventAggregatorClientImpl>(*client_call_manager_, INT_MAX);

  bool callback_called = false;
  Status final_status;
  always_fail_client->WaitForServerReadyWithRetry(
      [&callback_called, &final_status](const Status &status) {
        callback_called = true;
        final_status = status;
      },
      0,
      2,
      kRetryIntervalMs);

  io_service_.run_for(std::chrono::milliseconds(kRetryIntervalMs * 5));

  ASSERT_TRUE(callback_called);
  ASSERT_FALSE(final_status.ok());
  ASSERT_FALSE(always_fail_client->exporter_initialized_.load());
}

}  // namespace rpc
}  // namespace ray
