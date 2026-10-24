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
#include <condition_variable>
#include <cstring>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#if !defined(_WIN32)
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

#include "absl/cleanup/cleanup.h"
#include "absl/container/flat_hash_map.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/time/time.h"
#include "gtest/gtest.h"
#include "ray/asio/instrumented_io_context.h"
#include "ray/common/ray_config.h"
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

  // End every socket wait of `context` without telling hiredis, the way a
  // write wait completed by EPOLLERR before the connect finishes leaves it.
  static void CancelSocketWaits(RedisAsyncContext &context) {
    boost::system::error_code ec;
    context.socket_.cancel(ec);
  }

  static void ExpectResetConnectCompletesWithoutItsWriteEvent();
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

namespace {

// Stop the test Redis and bring it back on the same port a moment later, the
// way a restarted Redis container does: the attempts in between are refused,
// and the one that connects must still go through with nothing else queued
// behind the reconnect check.
void ExpectReconnectAfterRedisRestarts(const std::string &host) {
  const int port = TEST_REDIS_SERVER_PORTS.front();
  instrumented_io_context io;
  auto work = boost::asio::make_work_guard(io.get_executor());
  std::thread io_thread([&io] { io.run(); });
  ray::Clock clock;
  auto ctx = std::make_unique<RedisContext>(io, clock);
  absl::Cleanup stop = [&] {
    work.reset();
    io.stop();
    io_thread.join();
    ctx.reset();
  };
  ASSERT_TRUE(ctx->Connect(host, port, /*username=*/"", /*password=*/"").ok());

  TestSetupUtil::ShutDownRedisServers();
  std::this_thread::sleep_for(1s);
  TestSetupUtil::StartUpRedisServers({port});

  std::promise<bool> done;
  ctx->RunArgvAsync(
      {"SET", "after_restart", "1"},
      [&done](const std::shared_ptr<CallbackReply> &reply) {
        done.set_value(reply->ReadAsStatus().ok());
      },
      kNoTable);
  auto future = done.get_future();
  ASSERT_EQ(future.wait_for(std::chrono::seconds(20)), std::future_status::ready);
  EXPECT_TRUE(future.get());
}

}  // namespace

TEST_F(RedisAsyncContextTest, TestReconnectsAfterRedisRestartsToLiteralAddress) {
  ExpectReconnectAfterRedisRestarts("127.0.0.1");
}

TEST_F(RedisAsyncContextTest, TestReconnectsAfterRedisRestartsThroughHostName) {
  ExpectReconnectAfterRedisRestarts("localhost");
}

// GcsServer connects its KV store client from the main thread while that
// client's io_context already runs on its own thread, so the async connection
// can drop before Connect() returns. The reconnect must still happen.
TEST_F(RedisAsyncContextTest, TestDropDuringConnectStillReconnects) {
  const int port = TEST_REDIS_SERVER_PORTS.front();
  instrumented_io_context io;
  auto work = boost::asio::make_work_guard(io.get_executor());
  std::thread io_thread([&io] { io.run(); });
  ray::Clock clock;
  auto ctx = std::make_unique<RedisContext>(io, clock);
  absl::Cleanup stop = [&] {
    work.reset();
    io.stop();
    io_thread.join();
    ctx.reset();
  };
  // Fail the async connection on the io thread while Connect() is still
  // running, the way RedisAsyncContextConnectCallback reports a failed TCP
  // connect, and wait for that before Connect() goes on.
  ctx->SetAsyncConnectedHookForTesting([&io](RedisAsyncContext &async_context) {
    std::promise<void> dropped;
    io.post(
        [&] {
          redisAsyncContext *raw = async_context.GetRawRedisAsyncContext();
          raw->onConnect = nullptr;
          raw->onDisconnect = nullptr;
          async_context.ResetRawRedisAsyncContext();
          redisAsyncFree(raw);
          async_context.NotifyDisconnected();
          dropped.set_value();
        },
        "test.DropDuringConnect");
    dropped.get_future().wait();
  });
  ASSERT_TRUE(ctx->Connect("127.0.0.1", port, /*username=*/"", /*password=*/"").ok());

  std::promise<bool> done;
  ctx->RunArgvAsync(
      {"SET", "drop_during_connect", "1"},
      [&done](const std::shared_ptr<CallbackReply> &reply) {
        done.set_value(reply->ReadAsStatus().ok());
      },
      kNoTable);
  auto future = done.get_future();
  ASSERT_EQ(future.wait_for(std::chrono::seconds(20)), std::future_status::ready);
  EXPECT_TRUE(future.get());
}

#if !defined(_WIN32)
namespace {

// A TCP proxy to the test Redis that can switch to closing every connection
// as soon as it accepts it, which is what a proxy or a port forwarder does
// while the Redis behind it is down.
class ClosingProxy {
 public:
  explicit ClosingProxy(int redis_port) : redis_port_(redis_port) {
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    RAY_CHECK(listen_fd_ >= 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    RAY_CHECK(bind(listen_fd_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) == 0);
    RAY_CHECK(listen(listen_fd_, 64) == 0);
    socklen_t len = sizeof(addr);
    RAY_CHECK(getsockname(listen_fd_, reinterpret_cast<sockaddr *>(&addr), &len) == 0);
    port_ = ntohs(addr.sin_port);
    accept_thread_ = std::thread([this] { AcceptLoop(); });
  }

  ~ClosingProxy() {
    stopping_ = true;
    StartClosing();
    accept_thread_.join();
    for (auto &t : pipes_) {
      t.join();
    }
    for (int fd : open_fds_) {
      close(fd);
    }
    close(listen_fd_);
  }

  int port() const { return port_; }

  /// Drop every forwarded connection and close new ones on accept.
  void StartClosing() {
    closing_ = true;
    std::lock_guard<std::mutex> lock(mu_);
    for (int fd : open_fds_) {
      shutdown(fd, SHUT_RDWR);
    }
  }

  int closed_on_accept() const { return closed_on_accept_; }

 private:
  void AcceptLoop() {
    while (!stopping_) {
      pollfd p{listen_fd_, POLLIN, 0};
      if (poll(&p, 1, 50) <= 0) {
        continue;
      }
      int client = accept(listen_fd_, nullptr, nullptr);
      if (client < 0) {
        continue;
      }
      if (closing_) {
        ++closed_on_accept_;
        close(client);
        continue;
      }
      int upstream = socket(AF_INET, SOCK_STREAM, 0);
      sockaddr_in addr{};
      addr.sin_family = AF_INET;
      addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
      addr.sin_port = htons(redis_port_);
      RAY_CHECK(connect(upstream, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) ==
                0);
      {
        std::lock_guard<std::mutex> lock(mu_);
        open_fds_.push_back(client);
        open_fds_.push_back(upstream);
      }
      pipes_.emplace_back([client, upstream] { Pipe(client, upstream); });
    }
  }

  static void Pipe(int a, int b) {
    char buf[16384];
    pollfd fds[2] = {{a, POLLIN, 0}, {b, POLLIN, 0}};
    while (poll(fds, 2, -1) > 0) {
      for (int i = 0; i < 2; ++i) {
        if (fds[i].revents == 0) {
          continue;
        }
        ssize_t n = read(fds[i].fd, buf, sizeof(buf));
        if (n <= 0 || write(fds[1 - i].fd, buf, n) != n) {
          shutdown(a, SHUT_RDWR);
          shutdown(b, SHUT_RDWR);
          return;
        }
      }
    }
  }

  const int redis_port_;
  int listen_fd_ = -1;
  int port_ = 0;
  std::atomic<bool> stopping_{false};
  std::atomic<bool> closing_{false};
  std::atomic<int> closed_on_accept_{0};
  std::mutex mu_;
  std::vector<int> open_fds_;
  std::vector<std::thread> pipes_;
  std::thread accept_thread_;
};

}  // namespace

// A connection that is accepted and then closed before Redis answers is a
// failed attempt, not a reconnect. Counting it as a success reset the backoff
// and turned the outage into a tight loop of connects.
TEST_F(RedisAsyncContextTest, TestAcceptThenCloseBacksOff) {
  ClosingProxy proxy(TEST_REDIS_SERVER_PORTS.front());
  instrumented_io_context io;
  auto work = boost::asio::make_work_guard(io.get_executor());
  std::thread io_thread([&io] { io.run(); });
  ray::Clock clock;
  auto ctx = std::make_unique<RedisContext>(io, clock);
  // Declared after the proxy, so the context stops reconnecting before the
  // proxy goes away.
  absl::Cleanup stop = [&] {
    work.reset();
    io.stop();
    io_thread.join();
    ctx.reset();
  };
  ASSERT_TRUE(
      ctx->Connect("127.0.0.1", proxy.port(), /*username=*/"", /*password=*/"").ok());

  proxy.StartClosing();
  std::this_thread::sleep_for(2s);
  // The backoff starts at 100ms and doubles: about five attempts in 2s.
  EXPECT_GE(proxy.closed_on_accept(), 1);
  EXPECT_LE(proxy.closed_on_accept(), 10);
}
#endif

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

// A server that rejects the reconnect AUTH but still answers the check, such
// as one with no password configured, fails the attempt: counting it as a
// reconnect started a fresh episode, with a fresh budget and no backoff, on
// every attempt.
TEST_F(RedisAsyncContextTest, TestRejectedAuthFailsTheAttemptEvenIfTheCheckPasses) {
  const int port = TEST_REDIS_SERVER_PORTS.front();
  redisContext *admin = redisConnect("127.0.0.1", port);
  ASSERT_TRUE(admin != nullptr && admin->err == 0);
  auto admin_command = [admin](const char *command, const char *arg = nullptr) {
    auto *reply =
        static_cast<redisReply *>(arg == nullptr ? redisCommand(admin, command)
                                                 : redisCommand(admin, command, arg));
    std::string result;
    if (reply != nullptr) {
      result = reply->type == REDIS_REPLY_INTEGER
                   ? std::to_string(reply->integer)
                   : std::string(reply->str == nullptr ? "" : reply->str, reply->len);
      freeReplyObject(reply);
    }
    return result;
  };
  admin_command("CONFIG SET requirepass %s", "auth_loop_pw");
  admin_command("AUTH auth_loop_pw");
  instrumented_io_context io;
  auto work = boost::asio::make_work_guard(io.get_executor());
  std::thread io_thread([&io] { io.run(); });
  ray::Clock clock;
  auto ctx = std::make_unique<RedisContext>(io, clock);
  absl::Cleanup stop = [&] {
    admin_command("CONFIG SET requirepass %s", "");
    redisFree(admin);
    work.reset();
    io.stop();
    io_thread.join();
    ctx.reset();
  };
  ASSERT_TRUE(
      ctx->Connect("127.0.0.1", port, /*username=*/"", /*password=*/"auth_loop_pw").ok());

  // From now on AUTH is an error, while a command without it goes through.
  admin_command("CONFIG SET requirepass %s", "");
  auto connections = [&admin_command]() {
    const std::string info = admin_command("INFO stats");
    const std::string field = "total_connections_received:";
    const size_t at = info.find(field);
    RAY_CHECK(at != std::string::npos);
    return std::stoll(info.substr(at + field.size()));
  };
  const int64_t before = connections();
  admin_command("CLIENT KILL TYPE normal");
  std::this_thread::sleep_for(2s);
  // The backoff starts at 100ms and doubles: about five attempts in 2s.
  EXPECT_LE(connections() - before, 10);
}

namespace {

constexpr char kWatermarkKey[] = "test_write_watermark";
constexpr char kWatermarkField[] = "test_client";

// A RedisContext on its own io thread whose failed reconnect checks are
// recorded instead of exiting, and an admin connection to the same Redis for
// arranging what the server holds.
class ReconnectCheckHarness {
 public:
  ReconnectCheckHarness()
      : port_(TEST_REDIS_SERVER_PORTS.front()),
        work_(boost::asio::make_work_guard(io_.get_executor())),
        io_thread_([this] { io_.run(); }),
        ctx_(std::make_unique<RedisContext>(io_, clock_)) {
    admin_ = redisConnect("127.0.0.1", port_);
    RAY_CHECK(admin_ != nullptr && admin_->err == 0);
    ctx_->SetWritesLostHandlerForTesting([this](const std::string &reason) {
      std::lock_guard<std::mutex> lock(mu_);
      lost_reasons_.push_back(reason);
      lost_cv_.notify_all();
    });
    RAY_CHECK_OK(ctx_->Connect("127.0.0.1", port_, /*username=*/"", /*password=*/""));
  }

  ~ReconnectCheckHarness() {
    redisFree(admin_);
    // The contract in redis_context.h: destroy the context only once its
    // io_context has stopped running.
    work_.reset();
    io_.stop();
    io_thread_.join();
    ctx_.reset();
  }

  RedisContext &ctx() { return *ctx_; }

  void EnableWatermark() { ctx_->EnableWriteWatermark(kWatermarkKey, kWatermarkField); }

  // Run a command through the context and wait for its reply; nullptr if none
  // comes within `timeout`.
  std::shared_ptr<CallbackReply> Run(std::vector<std::string> args,
                                     std::chrono::seconds timeout = 20s) {
    auto done = std::make_shared<std::promise<std::shared_ptr<CallbackReply>>>();
    auto future = done->get_future();
    ctx_->RunArgvAsync(
        std::move(args),
        [done](const std::shared_ptr<CallbackReply> &reply) { done->set_value(reply); },
        kNoTable);
    if (future.wait_for(timeout) != std::future_status::ready) {
      return nullptr;
    }
    return future.get();
  }

  // A round trip behind everything submitted so far, markers included.
  void Sync() { ASSERT_NE(Run({"PING"}), nullptr); }

  // Run a command on the admin connection and return the reply's text, its
  // integer as text, or "(nil)".
  std::string Admin(const std::vector<std::string> &args) {
    std::vector<const char *> argv;
    std::vector<size_t> argvlen;
    for (const auto &arg : args) {
      argv.push_back(arg.data());
      argvlen.push_back(arg.size());
    }
    auto *reply = static_cast<redisReply *>(
        redisCommandArgv(admin_, argv.size(), argv.data(), argvlen.data()));
    RAY_CHECK(reply != nullptr);
    std::string result;
    if (reply->type == REDIS_REPLY_INTEGER) {
      result = std::to_string(reply->integer);
    } else if (reply->type == REDIS_REPLY_NIL) {
      result = "(nil)";
    } else {
      result = std::string(reply->str, reply->len);
    }
    freeReplyObject(reply);
    return result;
  }

  std::string Watermark() { return Admin({"HGET", kWatermarkKey, kWatermarkField}); }

  // Close the context's connection from the server side; the server and its
  // data stay as they are.
  void DropConnection() { ASSERT_NE(Admin({"CLIENT", "KILL", "TYPE", "normal"}), "0"); }

  std::optional<std::string> WaitForWritesLost(std::chrono::seconds timeout = 20s) {
    std::unique_lock<std::mutex> lock(mu_);
    if (!lost_cv_.wait_for(lock, timeout, [this] { return !lost_reasons_.empty(); })) {
      return std::nullopt;
    }
    return lost_reasons_.front();
  }

  size_t NumWritesLost() {
    std::lock_guard<std::mutex> lock(mu_);
    return lost_reasons_.size();
  }

 private:
  const int port_;
  instrumented_io_context io_;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_;
  std::thread io_thread_;
  ray::Clock clock_;
  std::unique_ptr<RedisContext> ctx_;
  redisContext *admin_ = nullptr;
  std::mutex mu_;
  std::condition_variable lost_cv_;
  std::vector<std::string> lost_reasons_;
};

// Split a watermark value, "<incarnation>:<n>".
std::pair<std::string, uint64_t> SplitWatermark(const std::string &value) {
  const size_t colon = value.rfind(':');
  RAY_CHECK(colon != std::string::npos) << value;
  return {value.substr(0, colon), std::stoull(value.substr(colon + 1))};
}

}  // namespace

// Every command that changes data is followed by a marker carrying the next
// write number; reads are not. The first marker goes out when the watermark
// is enabled.
// Writes queued together share one marker, written behind the last of them:
// the marker count follows flushes, not writes.
TEST_F(RedisAsyncContextTest, TestOneMarkerCoversABurstOfWrites) {
  ReconnectCheckHarness h;
  h.EnableWatermark();
  h.Sync();
  const auto [incarnation, first] = SplitWatermark(h.Watermark());
  ASSERT_EQ(h.Admin({"CONFIG", "RESETSTAT"}), "OK");

  constexpr int kWrites = 500;
  std::atomic<int> replies = 0;
  std::promise<void> all_replied;
  for (int i = 0; i < kWrites; ++i) {
    h.ctx().RunArgvAsync(
        {"SET", absl::StrCat("burst", i), "1"},
        [&](const std::shared_ptr<CallbackReply> &) {
          if (++replies == kWrites) {
            all_replied.set_value();
          }
        },
        kNoTable);
  }
  ASSERT_EQ(all_replied.get_future().wait_for(20s), std::future_status::ready);
  h.Sync();

  EXPECT_EQ(h.Watermark(), absl::StrCat(incarnation, ":", first + kWrites));
  // Writes are plain SETs, so every HSET Redis ran was a marker.
  const std::string stats = h.Admin({"INFO", "commandstats"});
  const size_t at = stats.find("cmdstat_hset:calls=");
  ASSERT_NE(at, std::string::npos) << stats;
  const int markers = std::stoi(stats.substr(at + std::strlen("cmdstat_hset:calls=")));
  EXPECT_GE(markers, 1);
  EXPECT_LT(markers, kWrites);

  h.DropConnection();
  h.Sync();
  EXPECT_EQ(h.NumWritesLost(), 0u);
}

TEST_F(RedisAsyncContextTest, TestWriteWatermarkCountsWritesOnly) {
  ReconnectCheckHarness h;
  h.EnableWatermark();
  h.Sync();
  const auto [incarnation, first] = SplitWatermark(h.Watermark());
  EXPECT_EQ(first, 1u);

  ASSERT_NE(h.Run({"SET", "a", "1"}), nullptr);
  h.Sync();
  EXPECT_EQ(h.Watermark(), absl::StrCat(incarnation, ":2"));

  ASSERT_NE(h.Run({"GET", "a"}), nullptr);
  ASSERT_NE(h.Run({"HGET", kWatermarkKey, kWatermarkField}), nullptr);
  ASSERT_NE(h.Run({"HEXISTS", "h", "x"}), nullptr);
  h.Sync();
  EXPECT_EQ(h.Watermark(), absl::StrCat(incarnation, ":2"));

  ASSERT_NE(h.Run({"HDEL", "h", "x"}), nullptr);
  ASSERT_NE(h.Run({"INCRBY", "counter", "1"}), nullptr);
  h.Sync();
  EXPECT_EQ(h.Watermark(), absl::StrCat(incarnation, ":4"));
}

// A dropped connection to a Redis that kept its data passes the check, and
// commands go through again.
TEST_F(RedisAsyncContextTest, TestReconnectResumesWhenRedisKeptWrites) {
  ReconnectCheckHarness h;
  h.EnableWatermark();
  ASSERT_NE(h.Run({"SET", "before", "1"}), nullptr);
  h.Sync();

  h.DropConnection();
  auto reply = h.Run({"SET", "after", "1"});
  ASSERT_NE(reply, nullptr);
  EXPECT_TRUE(reply->ReadAsStatus().ok());
  EXPECT_EQ(h.Admin({"GET", "after"}), "1");
  EXPECT_EQ(h.NumWritesLost(), 0u);
}

// A Redis that came back empty fails the check. Commands stay paused, so a
// write issued during the outage never lands on the emptied server.
TEST_F(RedisAsyncContextTest, TestReconnectCatchesRedisThatCameBackEmpty) {
  ReconnectCheckHarness h;
  h.EnableWatermark();
  ASSERT_NE(h.Run({"SET", "before", "1"}), nullptr);
  h.Sync();

  ASSERT_EQ(h.Admin({"FLUSHALL"}), "OK");
  h.DropConnection();
  h.ctx().RunArgvAsync({"SET", "during_outage", "1"}, nullptr, kNoTable);

  auto reason = h.WaitForWritesLost();
  ASSERT_TRUE(reason.has_value());
  EXPECT_NE(reason->find("none of this process's writes"), std::string::npos) << *reason;
  std::this_thread::sleep_for(1s);
  EXPECT_EQ(h.Admin({"EXISTS", "during_outage"}), "0");
}

// The check catches a Redis that kept older writes but lost the latest ones,
// which is what an older snapshot or a lagging replica leaves behind.
TEST_F(RedisAsyncContextTest, TestReconnectCatchesLostRecentWrites) {
  ReconnectCheckHarness h;
  h.EnableWatermark();
  ASSERT_NE(h.Run({"SET", "a", "1"}), nullptr);
  ASSERT_NE(h.Run({"SET", "b", "1"}), nullptr);
  h.Sync();
  const auto [incarnation, last] = SplitWatermark(h.Watermark());

  ASSERT_EQ(h.Admin({"HSET",
                     kWatermarkKey,
                     kWatermarkField,
                     absl::StrCat(incarnation, ":", last - 1)}),
            "0");
  h.DropConnection();

  auto reason = h.WaitForWritesLost();
  ASSERT_TRUE(reason.has_value());
  EXPECT_NE(reason->find(absl::StrCat("only writes up to ", last - 1)), std::string::npos)
      << *reason;
}

// A marker left by another process under the same field says nothing about
// this process's writes: Redis was restored to a point before them.
TEST_F(RedisAsyncContextTest, TestReconnectCatchesAnotherIncarnationsMarker) {
  ReconnectCheckHarness h;
  h.EnableWatermark();
  ASSERT_NE(h.Run({"SET", "a", "1"}), nullptr);
  h.Sync();

  ASSERT_EQ(h.Admin({"HSET", kWatermarkKey, kWatermarkField, "0123456789abcdef:99"}),
            "0");
  h.DropConnection();

  auto reason = h.WaitForWritesLost();
  ASSERT_TRUE(reason.has_value());
  EXPECT_NE(reason->find("none of this process's writes"), std::string::npos) << *reason;
}

// The first marker covers what Redis held before this process wrote anything,
// such as the state GCS loaded at startup.
TEST_F(RedisAsyncContextTest, TestFirstMarkerCoversDataFromBeforeAnyWrite) {
  ReconnectCheckHarness h;
  h.EnableWatermark();
  h.Sync();

  ASSERT_EQ(h.Admin({"FLUSHALL"}), "OK");
  h.DropConnection();

  EXPECT_TRUE(h.WaitForWritesLost().has_value());
}

// The watermark can be enabled while a reconnect check is pending, as when a
// promotion races a reconnect. Marker 1 is then held back, not dropped: it goes
// out once commands resume, even with nothing else queued, so the state loaded
// at enable time is covered without waiting for a write.
TEST_F(RedisAsyncContextTest, TestFirstMarkerHeldDuringAReconnectCheckIsSent) {
  ReconnectCheckHarness h;
  ASSERT_NE(h.Run({"SET", "loaded", "1"}), nullptr);
  h.ctx().async_context().PauseCommands(true);
  h.EnableWatermark();
  EXPECT_EQ(h.Watermark(), "(nil)");
  h.ctx().async_context().PauseCommands(false);

  const auto deadline = std::chrono::steady_clock::now() + 10s;
  while (!absl::EndsWith(h.Watermark(), ":1") &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(20ms);
  }
  ASSERT_TRUE(absl::EndsWith(h.Watermark(), ":1")) << h.Watermark();
  h.Sync();

  ASSERT_EQ(h.Admin({"FLUSHALL"}), "OK");
  h.DropConnection();
  EXPECT_TRUE(h.WaitForWritesLost().has_value());
}

// A rejected first marker is not counted as acknowledged, so a reconnect does
// not take Redis's word for data the marker was meant to cover.
TEST_F(RedisAsyncContextTest, TestRejectedFirstMarkerIsNotAcknowledged) {
  ReconnectCheckHarness h;
  // HSET on a string key fails with WRONGTYPE.
  ASSERT_EQ(h.Admin({"SET", kWatermarkKey, "not a hash"}), "OK");
  h.EnableWatermark();
  h.Sync();
  EXPECT_EQ(h.ctx().async_context().LastAckedWrite(), 0u);
}

// A promoted GCS enables its watermark long after startup, possibly after a
// reconnect, which re-establishes only the async connection. The first marker
// must still land, and later reconnects must still pass.
TEST_F(RedisAsyncContextTest, TestWatermarkEnabledAfterAReconnect) {
  ReconnectCheckHarness h;
  h.Sync();
  h.DropConnection();
  h.Sync();

  h.EnableWatermark();
  h.Sync();
  const std::string marker = h.Watermark();
  EXPECT_TRUE(absl::EndsWith(marker, ":1")) << marker;

  ASSERT_NE(h.Run({"SET", "after", "1"}), nullptr);
  h.DropConnection();
  h.Sync();
  EXPECT_EQ(h.NumWritesLost(), 0u);
}

// Without a watermark, as for contexts outside gcs_server, a reconnect only
// needs Redis to answer.
TEST_F(RedisAsyncContextTest, TestReconnectWithoutWatermarkOnlyNeedsAnAnswer) {
  ReconnectCheckHarness h;
  ASSERT_NE(h.Run({"SET", "before", "1"}), nullptr);

  ASSERT_EQ(h.Admin({"FLUSHALL"}), "OK");
  h.DropConnection();
  auto reply = h.Run({"SET", "after", "1"});
  ASSERT_NE(reply, nullptr);
  EXPECT_TRUE(reply->ReadAsStatus().ok());
  EXPECT_EQ(h.NumWritesLost(), 0u);
}

// redis_reconnect_verify_writes=false writes no markers and skips the check.
TEST_F(RedisAsyncContextTest, TestWriteWatermarkCanBeTurnedOff) {
  RayConfig::instance().initialize(R"({"redis_reconnect_verify_writes": false})");
  absl::Cleanup restore = [] {
    RayConfig::instance().initialize(R"({"redis_reconnect_verify_writes": true})");
  };
  ReconnectCheckHarness h;
  h.EnableWatermark();
  ASSERT_NE(h.Run({"SET", "before", "1"}), nullptr);
  h.Sync();
  EXPECT_EQ(h.Admin({"EXISTS", kWatermarkKey}), "0");

  ASSERT_EQ(h.Admin({"FLUSHALL"}), "OK");
  h.DropConnection();
  auto reply = h.Run({"SET", "after", "1"});
  ASSERT_NE(reply, nullptr);
  EXPECT_TRUE(reply->ReadAsStatus().ok());
  EXPECT_EQ(h.NumWritesLost(), 0u);
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

// The write event hiredis waits for to finish a connect can be used up before
// the connect completes. A reconnected context must still complete it and send
// what was queued behind it, with nothing else submitted to ask for another
// write event.
void RedisAsyncContextTest::ExpectResetConnectCompletesWithoutItsWriteEvent() {
  instrumented_io_context local_io_service;
  const int port = TEST_REDIS_SERVER_PORTS.front();
  RedisAsyncContext ctx(local_io_service, ConnectRaw(port));
  SimulateHiredisDisconnect(ctx);

  auto fresh = ConnectRaw(port);
  fresh->data = &ctx;
  std::atomic<bool> replied{false};
  ASSERT_EQ(redisAsyncCommand(
                fresh.get(),
                [](redisAsyncContext *, void *reply, void *privdata) {
                  if (reply != nullptr) {
                    static_cast<std::atomic<bool> *>(privdata)->store(true);
                  }
                },
                &replied,
                "PING"),
            REDIS_OK);
  ctx.Reset(std::move(fresh));
  // Run the write wait Reset() arms, then end it unanswered.
  local_io_service.poll_one();
  CancelSocketWaits(ctx);

  for (int i = 0; i < 50 && !replied.load(); ++i) {
    local_io_service.run_for(std::chrono::milliseconds(100));
    local_io_service.restart();
  }
  EXPECT_TRUE(replied.load());
}

TEST_F(RedisAsyncContextTest, TestResetConnectCompletesWithoutItsWriteEvent) {
  ExpectResetConnectCompletesWithoutItsWriteEvent();
}

// A connect timeout of 0 turns the timeout off rather than giving up on every
// attempt at its first check.
TEST_F(RedisAsyncContextTest, TestZeroConnectTimeoutMeansNoTimeout) {
  RayConfig::instance().initialize(R"({"redis_reconnect_connect_timeout_ms": 0})");
  absl::Cleanup restore = [] {
    RayConfig::instance().initialize(R"({"redis_reconnect_connect_timeout_ms": 5000})");
  };
  ExpectResetConnectCompletesWithoutItsWriteEvent();
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

#if !defined(_WIN32)
namespace {

// A listener whose accept queue is full, so the kernel drops further SYNs and
// a connect to it stays in progress, like one to an address that is gone.
class BlackholeListener {
 public:
  BlackholeListener() {
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    RAY_CHECK(listen_fd_ >= 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    RAY_CHECK(bind(listen_fd_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) == 0);
    RAY_CHECK(listen(listen_fd_, 0) == 0);
    socklen_t len = sizeof(addr);
    RAY_CHECK(getsockname(listen_fd_, reinterpret_cast<sockaddr *>(&addr), &len) == 0);
    port_ = ntohs(addr.sin_port);
    for (int i = 0; i < 8; ++i) {
      fillers_.push_back(StartConnect());
    }
  }

  ~BlackholeListener() {
    for (int fd : fillers_) {
      close(fd);
    }
    close(listen_fd_);
  }

  int port() const { return port_; }

  // Whether a new connect stays in progress, which not every platform does
  // for a full accept queue.
  bool Blackholes() {
    int fd = StartConnect();
    pollfd p{fd, POLLOUT, 0};
    const bool in_progress = poll(&p, 1, 300) == 0;
    close(fd);
    return in_progress;
  }

 private:
  int StartConnect() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    RAY_CHECK(fd >= 0);
    RAY_CHECK(fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK) == 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port_);
    connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr));
    return fd;
  }

  int listen_fd_ = -1;
  int port_ = 0;
  std::vector<int> fillers_;
};

// REDIS_ERR is -1, so "not called yet" needs a value of its own.
constexpr int kConnectNotReported = 1000;
std::atomic<int> timed_out_connect_status{kConnectNotReported};
void RecordFailedConnect(const redisAsyncContext *c, int status) {
  if (status != REDIS_OK) {
    // hiredis frees a context whose connect failed; release it first.
    static_cast<RedisAsyncContext *>(c->data)->ResetRawRedisAsyncContext();
  }
  timed_out_connect_status = status;
}

}  // namespace

// A reconnect whose connect never completes is given up after
// redis_reconnect_connect_timeout_ms and reported as a failed connect, so the
// next attempt can start, instead of waiting out the kernel's SYN retries.
TEST_F(RedisAsyncContextTest, TestResetGivesUpOnAConnectThatNeverCompletes) {
  BlackholeListener hole;
  if (!hole.Blackholes()) {
    GTEST_SKIP() << "A full accept queue does not drop SYNs here.";
  }
  RayConfig::instance().initialize(R"({"redis_reconnect_connect_timeout_ms": 300})");
  absl::Cleanup restore = [] {
    RayConfig::instance().initialize(R"({"redis_reconnect_connect_timeout_ms": 5000})");
  };
  instrumented_io_context local_io_service;
  RedisAsyncContext ctx(local_io_service, ConnectRaw(TEST_REDIS_SERVER_PORTS.front()));
  SimulateHiredisDisconnect(ctx);

  auto fresh = ConnectRaw(hole.port());
  fresh->data = &ctx;
  timed_out_connect_status = kConnectNotReported;
  ASSERT_EQ(redisAsyncSetConnectCallback(fresh.get(), RecordFailedConnect), REDIS_OK);
  const auto start = std::chrono::steady_clock::now();
  ctx.Reset(std::move(fresh));
  for (int i = 0; i < 50 && timed_out_connect_status.load() == kConnectNotReported; ++i) {
    local_io_service.run_for(std::chrono::milliseconds(100));
    local_io_service.restart();
  }
  const auto elapsed = std::chrono::steady_clock::now() - start;
  EXPECT_EQ(timed_out_connect_status.load(), REDIS_ERR);
  EXPECT_GE(elapsed, std::chrono::milliseconds(250));
  EXPECT_LT(elapsed, std::chrono::seconds(2));
  EXPECT_EQ(ctx.GetRawRedisAsyncContext(), nullptr);
}

namespace {

std::atomic<bool> silent_check_answered{false};
std::atomic<bool> silent_disconnected{false};
void ReleaseOnDisconnect(const redisAsyncContext *c, int /*status*/) {
  static_cast<RedisAsyncContext *>(c->data)->ResetRawRedisAsyncContext();
  silent_disconnected = true;
}

}  // namespace

// A server that completes the TCP handshake but never answers must not hold a
// reconnect attempt forever: while commands are paused for the reconnect check,
// the attempt is given up after redis_reconnect_connect_timeout_ms too.
TEST_F(RedisAsyncContextTest, TestResetGivesUpOnACheckThatIsNeverAnswered) {
  // The kernel completes handshakes for this listener, which never accepts or
  // reads.
  int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(listen_fd, 0);
  absl::Cleanup close_listener = [listen_fd] { close(listen_fd); };
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  ASSERT_EQ(bind(listen_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)), 0);
  ASSERT_EQ(listen(listen_fd, 16), 0);
  socklen_t len = sizeof(addr);
  ASSERT_EQ(getsockname(listen_fd, reinterpret_cast<sockaddr *>(&addr), &len), 0);

  RayConfig::instance().initialize(R"({"redis_reconnect_connect_timeout_ms": 300})");
  absl::Cleanup restore = [] {
    RayConfig::instance().initialize(R"({"redis_reconnect_connect_timeout_ms": 5000})");
  };
  instrumented_io_context local_io_service;
  RedisAsyncContext ctx(local_io_service, ConnectRaw(TEST_REDIS_SERVER_PORTS.front()));
  SimulateHiredisDisconnect(ctx);

  auto fresh = ConnectRaw(ntohs(addr.sin_port));
  fresh->data = &ctx;
  silent_check_answered = false;
  silent_disconnected = false;
  ASSERT_EQ(redisAsyncSetDisconnectCallback(fresh.get(), ReleaseOnDisconnect), REDIS_OK);
  ASSERT_EQ(redisAsyncCommand(
                fresh.get(),
                [](redisAsyncContext *, void *reply, void *) {
                  if (reply != nullptr) {
                    silent_check_answered = true;
                  }
                },
                nullptr,
                "PING"),
            REDIS_OK);
  ctx.PauseCommands(true);
  const auto start = std::chrono::steady_clock::now();
  ctx.Reset(std::move(fresh));
  for (int i = 0; i < 50 && !silent_disconnected.load(); ++i) {
    local_io_service.run_for(std::chrono::milliseconds(100));
    local_io_service.restart();
  }
  const auto elapsed = std::chrono::steady_clock::now() - start;
  EXPECT_TRUE(silent_disconnected.load());
  EXPECT_FALSE(silent_check_answered.load());
  EXPECT_GE(elapsed, std::chrono::milliseconds(250));
  EXPECT_LT(elapsed, std::chrono::seconds(2));
  EXPECT_EQ(ctx.GetRawRedisAsyncContext(), nullptr);
}
#endif

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

TEST_F(RedisAsyncContextTest, RetryWaitingOnADestroyedContextIsDropped) {
  auto local_io_service = std::make_unique<instrumented_io_context>(
      /*emit_metrics=*/false, /*running_on_single_thread=*/true);
  ray::Clock clock;
  auto context = std::make_unique<RedisContext>(*local_io_service, clock);
  ASSERT_TRUE(context
                  ->Connect("127.0.0.1",
                            TEST_REDIS_SERVER_PORTS.front(),
                            /*username=*/"",
                            /*password=*/"")
                  .ok());
  auto *raw_context = context->async_context().GetRawRedisAsyncContext();
  ASSERT_NE(raw_context, nullptr);
  context->async_context().ResetRawRedisAsyncContext();
  redisAsyncFree(raw_context);

  // The request owns itself. Its callback holds the only reference to the
  // sentinel, so the sentinel goes away exactly when the request is deleted.
  auto sentinel = std::make_shared<int>(0);
  std::weak_ptr<int> request_alive = sentinel;
  std::atomic<bool> answered = false;
  auto *request = new RedisRequestContext(
      *local_io_service,
      [sentinel = std::move(sentinel), &answered](std::shared_ptr<CallbackReply>) {
        answered = true;
      },
      &context->async_context(),
      std::vector<std::string>{"HGET", "key"},
      clock,
      /*metrics=*/nullptr,
      "NODE");
  // Submission fails, so Run() schedules a retry. Destroy the context before
  // the retry fires: the retry must drop the request instead of running it.
  request->Run();
  ASSERT_FALSE(request_alive.expired());
  context.reset();

  local_io_service->run_for(std::chrono::seconds(2));
  EXPECT_TRUE(request_alive.expired());
  EXPECT_FALSE(answered.load());
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
