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

#include "ray/gcs/store_client/redis_async_context.h"

#include <atomic>
#include <chrono>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>

#include "absl/cleanup/cleanup.h"
#include "absl/container/flat_hash_map.h"
#include "gtest/gtest.h"
#include "ray/asio/instrumented_io_context.h"
#include "ray/common/test_utils.h"
#include "ray/gcs/store_client/redis_context.h"
#include "ray/observability/fake_metric.h"
#include "ray/util/clock.h"
#include "ray/util/logging.h"
#include "ray/util/path_utils.h"
#include "ray/util/raii.h"

extern "C" {
#include "hiredis/async.h"
#include "hiredis/hiredis.h"
}

namespace ray {
namespace gcs {
using namespace std::chrono_literals;  // NOLINT

instrumented_io_context io_service;

void ConnectCallback(const redisAsyncContext *c, int status) {
  if (status != REDIS_OK) {
    // A failed connect frees the context without ever running the disconnect
    // callback: hiredis only runs that one once REDIS_CONNECTED has been set
    // (__redisAsyncFree). Release here, or the destructor frees the context a
    // second time. Must come before the assertion, which returns early.
    RAY_CHECK(c->data != nullptr) << "ac->data must point at the owning context";
    static_cast<RedisAsyncContext *>(c->data)->ResetRawRedisAsyncContext();
  }
  ASSERT_EQ(status, REDIS_OK);
}

void DisconnectCallback(const redisAsyncContext *c, int status) {
  // hiredis frees the raw context around this callback
  // (__redisAsyncDisconnect -> __redisAsyncFree), so hand ownership back
  // first. Otherwise the RedisAsyncContext destructor frees it a second time
  // and the test crashes instead of reporting the failure. Do this before any
  // assertion, which would return early.
  RAY_CHECK(c->data != nullptr) << "ac->data must point at the owning context";
  static_cast<RedisAsyncContext *>(c->data)->ResetRawRedisAsyncContext();
  ASSERT_EQ(status, REDIS_OK);
}

void GetCallback(redisAsyncContext *c, void *r, void *privdata) {
  redisReply *reply = reinterpret_cast<redisReply *>(r);
  ASSERT_TRUE(reply != nullptr);
  ASSERT_EQ(std::string(reinterpret_cast<char *>(reply->str)), "test");
  io_service.stop();
}

class RedisAsyncContextTest : public ::testing::Test {
 public:
  RedisAsyncContextTest() { TestSetupUtil::StartUpRedisServers(std::vector<int>()); }

  virtual ~RedisAsyncContextTest() { TestSetupUtil::ShutDownRedisServers(); }

 protected:
  static bool TrySubmissionLock(RedisAsyncContext &context) {
    std::unique_lock<std::mutex> lock(context.mutex_, std::try_to_lock);
    return lock.owns_lock();
  }
};

TEST_F(RedisAsyncContextTest, TestRedisCommands) {
  redisAsyncContext *ac = redisAsyncConnect("127.0.0.1", TEST_REDIS_SERVER_PORTS.front());
  ASSERT_EQ(ac->err, 0);
  ray::gcs::RedisAsyncContext redis_async_context(
      io_service,
      std::unique_ptr<redisAsyncContext, RedisContextDeleter>(ac, RedisContextDeleter()));

  // Mirrors SetDisconnectCallback() in redis_context.cc: the callbacks need a
  // way back to the owning RedisAsyncContext to release the raw pointer.
  ac->data = &redis_async_context;
  redisAsyncSetConnectCallback(ac, ConnectCallback);
  redisAsyncSetDisconnectCallback(ac, DisconnectCallback);

  redisAsyncCommand(ac, NULL, NULL, "SET key test");
  redisAsyncCommand(ac, GetCallback, nullptr, "GET key");

  ray::Clock clock;
  std::shared_ptr<RedisContext> shard_context =
      std::make_shared<RedisContext>(io_service, clock);
  ASSERT_TRUE(shard_context
                  ->Connect(std::string("127.0.0.1"),
                            TEST_REDIS_SERVER_PORTS.front(),
                            /*username=*/std::string(),
                            /*password=*/std::string())
                  .ok());

  io_service.run();
}

TEST_F(RedisAsyncContextTest, RejectedSubmissionDoesNotRecordRequestMetrics) {
  auto local_io_service = std::make_unique<instrumented_io_context>(
      /*emit_metrics=*/false, /*running_on_single_thread=*/true);
  ray::Clock clock;
  observability::FakeCounter request_bytes;
  observability::FakeCounter response_bytes;
  observability::FakeCounter command_count;
  RedisMetrics metrics{request_bytes, response_bytes, command_count};
  auto context = std::make_unique<RedisContext>(*local_io_service, clock, metrics);
  ASSERT_TRUE(context
                  ->Connect("127.0.0.1",
                            TEST_REDIS_SERVER_PORTS.front(),
                            /*username=*/"",
                            /*password=*/"")
                  .ok());

  // Leave the wrapper alive but make command submission fail immediately. The
  // raw context is released first because hiredis invokes the disconnect
  // callback while freeing it, and that callback resets the wrapper again.
  auto *raw_context = context->async_context().GetRawRedisAsyncContext();
  ASSERT_NE(raw_context, nullptr);
  context->async_context().ResetRawRedisAsyncContext();
  redisAsyncFree(raw_context);

  auto request = std::make_unique<RedisRequestContext>(
      *local_io_service,
      [](std::shared_ptr<CallbackReply>) {},
      &context->async_context(),
      std::vector<std::string>{"HGET", "key"},
      clock,
      &metrics,
      "NODE");
  request->Run();

  EXPECT_TRUE(request_bytes.GetTagToValue().empty());
  EXPECT_TRUE(response_bytes.GetTagToValue().empty());
  EXPECT_TRUE(command_count.GetTagToValue().empty());

  // Run() scheduled a delayed retry. Destroy its handler before deleting the
  // self-owned request, while keeping the io_service alive for socket teardown.
  context.reset();
  local_io_service.reset();
  request.reset();
}

TEST_F(RedisAsyncContextTest, SubmissionNotificationHoldsReplyLock) {
  instrumented_io_context local_io_service;
  ray::Clock clock;
  std::promise<void> release;
  struct Submission {
    instrumented_io_context &io_service;
    std::shared_future<void> release;
    std::promise<void> started;
    std::promise<bool> replied;
    std::atomic<bool> accepted{false};
    std::thread::id notification_thread;
  } submission{local_io_service, release.get_future().share()};
  RedisContext context(local_io_service, clock);
  ASSERT_TRUE(context.Connect("127.0.0.1", TEST_REDIS_SERVER_PORTS.front(), "", "").ok());

  auto started = submission.started.get_future();
  auto replied = submission.replied.get_future();
  std::thread submitter([&]() {
    const char *argv[] = {"PING"};
    const size_t argvlen[] = {4};
    EXPECT_TRUE(context.async_context()
                    .RedisAsyncCommandArgv(
                        [](redisAsyncContext *, void *raw_reply, void *privdata) {
                          auto &state = *static_cast<Submission *>(privdata);
                          // Observe the raw hiredis callback, before any user callback
                          // can be posted to an event loop.
                          state.replied.set_value(raw_reply != nullptr && state.accepted);
                          state.io_service.stop();
                        },
                        &submission,
                        1,
                        argv,
                        argvlen,
                        [](void *privdata) {
                          auto &state = *static_cast<Submission *>(privdata);
                          state.notification_thread = std::this_thread::get_id();
                          state.started.set_value();
                          state.release.wait();
                          state.accepted = true;
                        })
                    .ok());
  });
  auto cleanup = absl::MakeCleanup([&]() {
    release.set_value();
    submitter.join();
  });
  ASSERT_EQ(started.wait_for(5s), std::future_status::ready);
  EXPECT_EQ(submission.notification_thread, submitter.get_id());
  // No IO thread is running yet, so only the submitter could hold this lock.
  // Probe from a different thread: try_lock on one's own std::mutex is undefined.
  EXPECT_FALSE(TrySubmissionLock(context.async_context()));
  std::move(cleanup).Invoke();
  EXPECT_TRUE(TrySubmissionLock(context.async_context()));

  local_io_service.run_for(5s);
  ASSERT_EQ(replied.wait_for(0s), std::future_status::ready);
  EXPECT_TRUE(replied.get());
}

TEST_F(RedisAsyncContextTest, SubmissionDoesNotWaitForIoWithOrWithoutMetrics) {
  for (const bool enabled : {false, true}) {
    SCOPED_TRACE(enabled);
    instrumented_io_context local_io_service;
    ray::Clock clock;
    observability::FakeCounter request_bytes;
    observability::FakeCounter response_bytes;
    observability::FakeCounter command_count;
    bool completed = false;
    RedisMetrics metrics{request_bytes, response_bytes, command_count};
    RedisContext context(
        local_io_service, clock, enabled ? std::make_optional(metrics) : std::nullopt);
    ASSERT_TRUE(
        context.Connect("127.0.0.1", TEST_REDIS_SERVER_PORTS.front(), "", "").ok());
    local_io_service.stop();
    context.RunArgvAsync(
        {"hgetall", "payload-test"},
        [&](std::shared_ptr<CallbackReply> reply) {
          EXPECT_TRUE(reply->ReadAsStringArray().empty());
          completed = true;
          local_io_service.stop();
        },
        "NODE");
    // The command must already be queued in hiredis even with a stopped event
    // loop and metrics disabled, when there are no counters to observe.
    auto *queued = context.async_context().GetRawRedisAsyncContext()->replies.tail;
    ASSERT_NE(queued, nullptr);
    EXPECT_EQ(queued->fn, RedisRequestContext::RedisResponseFn);
    EXPECT_NE(queued->privdata, nullptr);
    const absl::flat_hash_map<std::string, std::string> tags{{"Command", "HGETALL"},
                                                             {"TableName", "NODE"}};
    if (enabled) {
      EXPECT_EQ(request_bytes.GetTagToValue().at(tags), 19.0);
      EXPECT_EQ(command_count.GetTagToValue().at(tags), 1.0);
    } else {
      EXPECT_TRUE(request_bytes.GetTagToValue().empty());
      EXPECT_TRUE(command_count.GetTagToValue().empty());
    }
    EXPECT_TRUE(response_bytes.GetTagToValue().empty());

    local_io_service.restart();
    local_io_service.run_for(5s);
    EXPECT_TRUE(completed);
    if (enabled) {
      EXPECT_EQ(response_bytes.GetTagToValue().at(tags), 0.0);
    } else {
      EXPECT_TRUE(response_bytes.GetTagToValue().empty());
    }
  }
}

TEST_F(RedisAsyncContextTest, RejectedSubmissionThenSuccessCountsOnce) {
  instrumented_io_context local_io_service;
  ray::Clock clock;
  observability::FakeCounter request_bytes;
  observability::FakeCounter response_bytes;
  observability::FakeCounter command_count;
  bool completed = false;
  RedisContext context(local_io_service,
                       clock,
                       RedisMetrics{request_bytes, response_bytes, command_count});
  ASSERT_TRUE(context.Connect("127.0.0.1", TEST_REDIS_SERVER_PORTS.front(), "", "").ok());
  auto *raw = context.async_context().GetRawRedisAsyncContext();
  // hiredis rejects a command while disconnecting, without registering its
  // callback. Clear the flag before driving IO so the delayed retry can succeed.
  raw->c.flags |= REDIS_DISCONNECTING;
  context.RunArgvAsync(
      {"PING"},
      [&](std::shared_ptr<CallbackReply> reply) {
        EXPECT_EQ(reply->ReadAsStatus().message(), "PONG");
        completed = true;
        local_io_service.stop();
      },
      kNoTable);
  raw->c.flags &= ~REDIS_DISCONNECTING;
  EXPECT_TRUE(request_bytes.GetTagToValue().empty());
  EXPECT_TRUE(command_count.GetTagToValue().empty());
  EXPECT_TRUE(response_bytes.GetTagToValue().empty());

  local_io_service.run_for(5s);
  ASSERT_TRUE(completed);
  const absl::flat_hash_map<std::string, std::string> tags{{"Command", "PING"},
                                                           {"TableName", "NONE"}};
  EXPECT_EQ(request_bytes.GetTagToValue().at(tags), 4.0);
  EXPECT_EQ(command_count.GetTagToValue().at(tags), 1.0);
  EXPECT_EQ(response_bytes.GetTagToValue().at(tags), 4.0);
}

TEST_F(RedisAsyncContextTest, NullReplyRetryDoesNotRecountAcceptedCommand) {
  instrumented_io_context local_io_service;
  ray::Clock clock;
  observability::FakeCounter request_bytes;
  observability::FakeCounter response_bytes;
  observability::FakeCounter command_count;
  bool completed = false;
  RedisContext context(local_io_service,
                       clock,
                       RedisMetrics{request_bytes, response_bytes, command_count});
  ASSERT_TRUE(context.Connect("127.0.0.1", TEST_REDIS_SERVER_PORTS.front(), "", "").ok());
  context.RunArgvAsync(
      {"PING"},
      [&](std::shared_ptr<CallbackReply> reply) {
        EXPECT_EQ(reply->ReadAsStatus().message(), "PONG");
        completed = true;
        local_io_service.stop();
      },
      kNoTable);
  const absl::flat_hash_map<std::string, std::string> tags{{"Command", "PING"},
                                                           {"TableName", "NONE"}};
  EXPECT_EQ(request_bytes.GetTagToValue().at(tags), 4.0);
  EXPECT_EQ(command_count.GetTagToValue().at(tags), 1.0);

  auto *queued = context.async_context().GetRawRedisAsyncContext()->replies.tail;
  ASSERT_NE(queued, nullptr);
  ASSERT_EQ(queued->fn, RedisRequestContext::RedisResponseFn);
  // Simulate a lost reply for just the first accepted attempt. Let hiredis
  // consume its callback normally, so the retry is the only outstanding request.
  queued->fn = [](redisAsyncContext *ac, void *raw_reply, void *privdata) {
    EXPECT_NE(raw_reply, nullptr);
    RedisRequestContext::RedisResponseFn(ac, nullptr, privdata);
  };
  local_io_service.run_for(5s);
  ASSERT_TRUE(completed);
  EXPECT_EQ(request_bytes.GetTagToValue().at(tags), 4.0);
  EXPECT_EQ(command_count.GetTagToValue().at(tags), 1.0);
  EXPECT_EQ(response_bytes.GetTagToValue().at(tags), 4.0);
}
}  // namespace gcs
}  // namespace ray
