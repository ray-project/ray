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
#include "absl/time/time.h"
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

  // Mirrors SetConnectionCallbacks() in redis_context.cc: the callbacks need a
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

namespace {

std::unique_ptr<redisAsyncContext, RedisContextDeleter> ConnectRaw(int port) {
  redisAsyncContext *ac = redisAsyncConnect("127.0.0.1", port);
  EXPECT_TRUE(ac != nullptr);
  EXPECT_EQ(ac->err, 0);
  return std::unique_ptr<redisAsyncContext, RedisContextDeleter>(ac,
                                                                 RedisContextDeleter());
}

// Stand in for a real disconnect. On a dropped connection hiredis frees the
// raw context itself (__redisAsyncDisconnect -> __redisAsyncFree) and then
// invokes the disconnect callback, which releases our unique_ptr. Do both, in
// that order, so the test neither double-frees nor leaks the context.
void SimulateHiredisDisconnect(RedisAsyncContext &ctx) {
  redisAsyncContext *raw = ctx.GetRawRedisAsyncContext();
  ctx.ResetRawRedisAsyncContext();
  redisAsyncFree(raw);
}

}  // namespace

// A command issued after the raw context is gone must report Disconnected
// rather than dereferencing the released pointer.
TEST_F(RedisAsyncContextTest, TestCommandAfterResetRawContextIsDisconnected) {
  instrumented_io_context local_io_service;
  const int port = TEST_REDIS_SERVER_PORTS.front();
  RedisAsyncContext ctx(local_io_service, ConnectRaw(port));

  ASSERT_NE(ctx.GetRawRedisAsyncContext(), nullptr);

  SimulateHiredisDisconnect(ctx);
  ASSERT_EQ(ctx.GetRawRedisAsyncContext(), nullptr);

  const char *argv[] = {"PING"};
  const size_t argvlen[] = {4};
  Status status = ctx.RedisAsyncCommandArgv(nullptr, nullptr, 1, argv, argvlen);
  ASSERT_TRUE(status.IsDisconnected()) << status;
}

// Reset() must rebind the object to a fresh connection while keeping the
// object's own address stable: in-flight RedisRequestContexts hold a raw
// pointer to it.
TEST_F(RedisAsyncContextTest, TestResetRebindsInPlace) {
  instrumented_io_context local_io_service;
  const int port = TEST_REDIS_SERVER_PORTS.front();
  RedisAsyncContext ctx(local_io_service, ConnectRaw(port));

  const RedisAsyncContext *address_before = &ctx;

  SimulateHiredisDisconnect(ctx);
  ASSERT_EQ(ctx.GetRawRedisAsyncContext(), nullptr);

  ctx.Reset(ConnectRaw(port));

  // Don't compare against the pre-disconnect raw pointer: it has been freed,
  // and the allocator is free to hand the same address back for the new one.
  ASSERT_NE(ctx.GetRawRedisAsyncContext(), nullptr);
  ASSERT_EQ(address_before, &ctx);

  // The rebound context accepts commands again.
  const char *argv[] = {"PING"};
  const size_t argvlen[] = {4};
  ASSERT_TRUE(ctx.RedisAsyncCommandArgv(nullptr, nullptr, 1, argv, argvlen).ok());
}

namespace {

// Drop the async connection of `ctx` from the server side, the way a proxy or a
// Redis restart would, and check that a command issued right after rides out
// the in-place reconnect. CLIENT KILL spares the admin connection issuing it
// (SKIPME defaults to yes), so the server itself stays up throughout.
void ExpectReconnectAfterServerDropsConnection(const std::string &host) {
  const int port = TEST_REDIS_SERVER_PORTS.front();
  instrumented_io_context io;
  auto work = boost::asio::make_work_guard(io.get_executor());
  std::thread io_thread([&io] { io.run(); });
  ray::Clock clock;
  auto ctx = std::make_unique<RedisContext>(io, clock);
  // The contract in redis_context.h: destroy the context only once its
  // io_context has stopped running.
  absl::Cleanup stop = [&] {
    work.reset();
    io.stop();
    io_thread.join();
    ctx.reset();
  };
  ASSERT_TRUE(ctx->Connect(host, port, /*username=*/"", /*password=*/"").ok());

  redisContext *admin = redisConnect("127.0.0.1", port);
  ASSERT_TRUE(admin != nullptr && admin->err == 0);
  auto *killed =
      static_cast<redisReply *>(redisCommand(admin, "CLIENT KILL TYPE normal"));
  ASSERT_TRUE(killed != nullptr);
  EXPECT_EQ(killed->type, REDIS_REPLY_INTEGER);
  EXPECT_GE(killed->integer, 1);
  freeReplyObject(killed);
  redisFree(admin);

  std::promise<bool> done;
  ctx->RunArgvAsync(
      {"SET", "reconnect_probe", host},
      [&done](const std::shared_ptr<CallbackReply> &reply) {
        done.set_value(reply->ReadAsStatus().ok());
      },
      kNoTable);
  auto future = done.get_future();
  ASSERT_EQ(future.wait_for(std::chrono::seconds(20)), std::future_status::ready);
  EXPECT_TRUE(future.get());
}

}  // namespace

// A literal IP is reconnected to directly, without a lookup.
TEST_F(RedisAsyncContextTest, TestReconnectsToLiteralAddress) {
  ExpectReconnectAfterServerDropsConnection("127.0.0.1");
}

// A host name is re-resolved on every attempt, asynchronously so the GCS
// io_context is never blocked on DNS. This is the only test that takes the
// async_resolve path: everything else connects to a literal IP.
TEST_F(RedisAsyncContextTest, TestReconnectsThroughHostName) {
  ExpectReconnectAfterServerDropsConnection("localhost");
}

// A server that answers -NOAUTH never ran the command: that is what commands
// queued behind a rejected reconnect AUTH get back. Such replies must not spend
// the retry budget while the grace period lasts. The budget is six attempts
// over about 3.5s; the server keeps refusing for 5s, and the command must
// still succeed once it is allowed through.
TEST_F(RedisAsyncContextTest, TestNoAuthRepliesDoNotSpendRetries) {
  const int port = TEST_REDIS_SERVER_PORTS.front();
  instrumented_io_context io;
  auto work = boost::asio::make_work_guard(io.get_executor());
  std::thread io_thread([&io] { io.run(); });
  ray::Clock clock;
  auto ctx = std::make_unique<RedisContext>(io, clock);
  redisContext *admin = redisConnect("127.0.0.1", port);
  ASSERT_TRUE(admin != nullptr && admin->err == 0);
  auto admin_command = [admin](const char *command, const char *arg = nullptr) {
    auto *reply =
        static_cast<redisReply *>(arg == nullptr ? redisCommand(admin, command)
                                                 : redisCommand(admin, command, arg));
    ASSERT_TRUE(reply != nullptr);
    EXPECT_NE(reply->type, REDIS_REPLY_ERROR) << command << ": " << reply->str;
    freeReplyObject(reply);
  };
  // Changing requirepass also drops the authentication of connections that are
  // already open, the admin's included, so it authenticates before lifting it.
  // The empty password goes in as an argument: hiredis does not parse quotes,
  // so a literal "" in the format string would set a two-character password.
  auto lift_password = [admin]() {
    freeReplyObject(redisCommand(admin, "AUTH noauth_test_pw"));
    freeReplyObject(redisCommand(admin, "CONFIG SET requirepass %s", ""));
  };
  absl::Cleanup stop = [&] {
    // Lift the password for the tests that follow even if this one failed
    // half way; harmless when it is already lifted.
    lift_password();
    redisFree(admin);
    work.reset();
    io.stop();
    io_thread.join();
    ctx.reset();
  };
  ASSERT_TRUE(ctx->Connect("127.0.0.1", port, /*username=*/"", /*password=*/"").ok());

  // Require a password the client does not have, then drop its connection:
  // it reconnects without AUTH and every command comes back -NOAUTH.
  admin_command("CONFIG SET requirepass noauth_test_pw");
  admin_command("CLIENT KILL TYPE normal");

  std::promise<bool> done;
  ctx->RunArgvAsync(
      {"SET", "noauth_probe", "1"},
      [&done](const std::shared_ptr<CallbackReply> &reply) {
        done.set_value(reply->ReadAsStatus().ok());
      },
      kNoTable);
  auto future = done.get_future();
  // Longer than the whole retry budget: without the refund the command would
  // have run out of attempts and aborted the process by now.
  EXPECT_EQ(future.wait_for(std::chrono::seconds(5)), std::future_status::timeout);

  // Lift the password and drop the connection once more: lifting it does not
  // authenticate a connection that is already open, and in production the
  // rejected AUTH tears the connection down too, so the retry lands on a fresh
  // one.
  admin_command("AUTH noauth_test_pw");
  admin_command("CONFIG SET requirepass %s", "");
  admin_command("CLIENT KILL TYPE normal");
  ASSERT_EQ(future.wait_for(std::chrono::seconds(20)), std::future_status::ready);
  EXPECT_TRUE(future.get());
}

// The outage deadline is stamped once, by whoever gets there first, and every
// later caller in the same outage sees that same deadline regardless of the
// grace it passes. Clearing it lets the next outage start fresh.
TEST_F(RedisAsyncContextTest, TestOutageDeadlineIsSharedAndResettable) {
  instrumented_io_context local_io_service;
  const int port = TEST_REDIS_SERVER_PORTS.front();
  RedisAsyncContext ctx(local_io_service, ConnectRaw(port));

  const absl::Time t0 = absl::FromUnixSeconds(1000);
  const absl::Time first = ctx.OutageDeadline(t0, absl::Seconds(60));
  EXPECT_EQ(first, t0 + absl::Seconds(60));
  // A command that shows up 30s into the outage does not get its own 60s.
  EXPECT_EQ(ctx.OutageDeadline(t0 + absl::Seconds(30), absl::Seconds(60)), first);

  ctx.ClearOutage();
  const absl::Time t1 = t0 + absl::Seconds(500);
  EXPECT_EQ(ctx.OutageDeadline(t1, absl::Seconds(60)), t1 + absl::Seconds(60));
}

// A zero grace period stamps a deadline equal to now, so nothing is ever
// strictly before it: no refunds, which is the pre-reconnect behaviour.
TEST_F(RedisAsyncContextTest, TestOutageDeadlineZeroGraceRefundsNothing) {
  instrumented_io_context local_io_service;
  const int port = TEST_REDIS_SERVER_PORTS.front();
  RedisAsyncContext ctx(local_io_service, ConnectRaw(port));

  const absl::Time now = absl::FromUnixSeconds(1000);
  EXPECT_FALSE(now < ctx.OutageDeadline(now, absl::ZeroDuration()));
}

namespace {
std::atomic<int> connect_callback_status{-1};
void RecordConnectStatus(const redisAsyncContext * /*c*/, int status) {
  connect_callback_status = status;
}
}  // namespace

// The reconnect path registers the connect callback on the raw context before
// Reset() publishes it, so no hiredis field changes after other threads can
// see the context. hiredis arms its first write while that callback is being
// registered, which goes nowhere because our event hooks do not exist yet;
// Reset() has to arm it again or the connect is never noticed.
TEST_F(RedisAsyncContextTest, TestResetArmsWriteForPreRegisteredConnectCallback) {
  instrumented_io_context local_io_service;
  const int port = TEST_REDIS_SERVER_PORTS.front();
  RedisAsyncContext ctx(local_io_service, ConnectRaw(port));
  SimulateHiredisDisconnect(ctx);

  auto fresh = ConnectRaw(port);
  fresh->data = &ctx;
  connect_callback_status = -1;
  ASSERT_EQ(redisAsyncSetConnectCallback(fresh.get(), RecordConnectStatus), REDIS_OK);
  ctx.Reset(std::move(fresh));

  for (int i = 0; i < 50 && connect_callback_status.load() == -1; ++i) {
    local_io_service.run_for(std::chrono::milliseconds(100));
    local_io_service.restart();
  }
  EXPECT_EQ(connect_callback_status.load(), REDIS_OK);
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
    auto capture_acceptance = [&]() {
      submission.notification_thread = std::this_thread::get_id();
      submission.started.set_value();
      submission.release.wait();
      submission.accepted = true;
    };
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
                        capture_acceptance)
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

TEST_F(RedisAsyncContextTest, RetryCompletesWhileFirstRequestMetricsAreBlocked) {
  struct BlockingCounter : observability::FakeCounter {
    explicit BlockingCounter(std::shared_future<void> release)
        : release(std::move(release)) {}

    void Record(double value, stats::TagsType tags) override {
      std::call_once(started_once, [this]() { started.set_value(); });
      release.wait();
      observability::FakeCounter::Record(value, std::move(tags));
    }

    std::promise<void> started;
    std::once_flag started_once;
    std::shared_future<void> release;
  };

  instrumented_io_context local_io_service;
  ray::Clock clock;
  std::promise<void> release_record;
  BlockingCounter request_bytes(release_record.get_future().share());
  auto record_started = request_bytes.started.get_future();
  observability::FakeCounter response_bytes;
  observability::FakeCounter command_count;
  RedisContext context(local_io_service,
                       clock,
                       RedisMetrics{request_bytes, response_bytes, command_count});
  ASSERT_TRUE(context.Connect("127.0.0.1", TEST_REDIS_SERVER_PORTS.front(), "", "").ok());
  const std::string table = "TABLE_WITH_A_LONG_METRIC_LABEL";
  const absl::flat_hash_map<std::string, std::string> tags{{"Command", "PING"},
                                                           {"TableName", table}};
  std::promise<void> replied;
  auto reply_future = replied.get_future();
  std::thread io_thread;
  std::thread submitter([&]() {
    context.RunArgvAsync(
        {"PING"},
        [&](std::shared_ptr<CallbackReply> reply) {
          EXPECT_EQ(reply->ReadAsStatus().message(), "PONG");
          replied.set_value();
        },
        table);
  });
  auto cleanup = absl::MakeCleanup([&]() {
    // A broken claim guard can block the retry's Record() on the IO thread too.
    // Release all records before joining either thread, even after an assertion.
    release_record.set_value();
    submitter.join();
    if (!io_thread.joinable()) {
      io_thread = std::thread([&]() { local_io_service.run(); });
    }
    reply_future.wait_for(5s);
    local_io_service.stop();
    io_thread.join();
  });

  ASSERT_EQ(record_started.wait_for(5s), std::future_status::ready);
  // IO has not started, and the submitter is blocked after hiredis queued the
  // command. No thread can consume or mutate this callback during injection.
  auto *queued = context.async_context().GetRawRedisAsyncContext()->replies.tail;
  ASSERT_NE(queued, nullptr);
  ASSERT_EQ(queued->fn, RedisRequestContext::RedisResponseFn);
  queued->fn = [](redisAsyncContext *ac, void *raw_reply, void *privdata) {
    EXPECT_NE(raw_reply, nullptr);
    RedisRequestContext::RedisResponseFn(ac, nullptr, privdata);
  };
  io_thread = std::thread([&]() { local_io_service.run(); });

  // run_for() would not interrupt a blocked Record(). Keep the timeout and
  // release gate on this thread so regressions fail without hanging teardown.
  ASSERT_EQ(reply_future.wait_for(5s), std::future_status::ready);
  EXPECT_TRUE(request_bytes.GetTagToValue().empty());
  EXPECT_TRUE(command_count.GetTagToValue().empty());
  EXPECT_EQ(response_bytes.GetTagToValue().at(tags), 4.0);

  std::move(cleanup).Invoke();
  EXPECT_EQ(request_bytes.GetTagToValue().at(tags), 4.0);
  EXPECT_EQ(command_count.GetTagToValue().at(tags), 1.0);
  EXPECT_EQ(response_bytes.GetTagToValue().at(tags), 4.0);
}
}  // namespace gcs
}  // namespace ray
