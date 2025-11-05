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
#include "ray/raylet/runtime_env_agent_client.h"

#include <algorithm>
#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <boost/chrono.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/thread.hpp>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>

#include "gtest/gtest.h"
#include "ray/common/asio/asio_util.h"
#include "ray/common/id.h"
#include "ray/common/ray_config.h"
#include "ray/rpc/authentication/authentication_token_loader.h"
#include "src/ray/protobuf/runtime_env_agent.pb.h"

namespace ray {

namespace beast = boost::beast;
namespace http = beast::http;
namespace net = boost::asio;
using tcp = boost::asio::ip::tcp;
using boost::asio::ip::port_type;

port_type GetFreePort() {
  boost::asio::io_context io_service;
  boost::asio::ip::tcp::acceptor acceptor(io_service);
  boost::asio::ip::tcp::endpoint endpoint;

  // try to bind to port 0 to find a free port
  acceptor.open(tcp::v4());
  acceptor.bind(tcp::endpoint(tcp::v4(), 0));
  endpoint = acceptor.local_endpoint();
  auto port = endpoint.port();
  acceptor.close();
  return port;
}

// Handler functions that reads from a request and writes to a response.
// It can take a sync form or an async form. Under the hood it's always async as we will
// call the user provided sync one in our async impl.
class HttpConnection;
using AsyncHandler = std::function<void(std::shared_ptr<class HttpConnection>)>;
using SyncHandler = std::function<void(const http::request<http::string_body> &,
                                       http::response<http::string_body> &)>;

// Accepts 1 HTTP connection and handles it.
class HttpConnection : public std::enable_shared_from_this<HttpConnection> {
 public:
  // `handler` must out live this HttpConnection.
  HttpConnection(net::io_context &ioc, const AsyncHandler &handler)
      : ioc_(ioc), socket_(ioc), handler_(handler) {}

  // Initiate the asynchronous operations associated with the connection.
  void start() { read_request(); }

  net::io_context &ioc_;
  tcp::socket socket_;
  AsyncHandler handler_;
  beast::flat_buffer buffer_{8192};
  http::request<http::string_body> request_;
  http::response<http::string_body> response_;

  // Asynchronously receive a complete request message.
  void read_request() {
    auto self = shared_from_this();

    http::async_read(socket_,
                     buffer_,
                     request_,
                     [self](beast::error_code ec, std::size_t bytes_transferred) {
                       boost::ignore_unused(bytes_transferred);
                       if (ec) {
                         RAY_LOG(WARNING)
                             << "http connection error in read_request: " << ec.message();
                       } else {
                         self->process_request();
                       }
                     });
  }

  // Determine what needs to be done with the request message.
  void process_request() {
    response_.version(request_.version());
    response_.keep_alive(false);
    handler_(shared_from_this());
  }

  // Asynchronously transmit the response message.
  void write_response() {
    auto self = shared_from_this();

    response_.content_length(response_.body().size());

    http::async_write(socket_, response_, [self](beast::error_code ec, std::size_t) {
      self->socket_.shutdown(tcp::socket::shutdown_send, ec);
    });
  }
};

// Runs an http server on a dedicated thread.
// RAII: On dtor, stops the thread and cancels all requests.
class HttpServerThread {
 public:
  HttpServerThread(SyncHandler sync_handler, std::string address, port_type port)
      : ioc_(),
        handler_([sync_handler](std::shared_ptr<HttpConnection> conn) {
          sync_handler(conn->request_, conn->response_);
          conn->write_response();
        }),
        acceptor_(ioc_),
        endpoint_(net::ip::make_address(address), port) {}
  HttpServerThread(AsyncHandler async_handler, std::string address, port_type port)
      : ioc_(),
        handler_(std::move(async_handler)),
        acceptor_(ioc_),
        endpoint_(net::ip::make_address(address), port) {}

  // Starts a new thread to listen HTTP connections.
  // NOT THREAD SAFE to call.
  void start() {
    if (!thread_.joinable()) {
      acceptor_ = tcp::acceptor(ioc_, endpoint_);
      thread_ = std::thread([this]() {
        this->accept_one();
        RAY_LOG(INFO) << "HttpServerThread starting to accept...";
        this->ioc_.run();
      });
    }
  }

  // Accepts 1 connection. If a new connection is created, accept again until desctructed.
  void accept_one() {
    auto conn = std::make_shared<HttpConnection>(ioc_, handler_);
    this->acceptor_.async_accept(conn->socket_, [=](beast::error_code ec) {
      if (ec) {
        RAY_LOG(WARNING) << "http server thread can not accept: " << ec.message();
      } else {
        conn->start();
      }
      accept_one();
    });
  }

  ~HttpServerThread() {
    ioc_.stop();
    if (thread_.joinable()) {
      thread_.join();
    }
  }
  net::io_context ioc_;
  AsyncHandler handler_;
  tcp::acceptor acceptor_;
  tcp::endpoint endpoint_;
  std::thread thread_;
};

std::function<std::shared_ptr<boost::asio::deadline_timer>(std::function<void()>,
                                                           uint32_t delay_ms)>
delay_after(instrumented_io_context &ioc) {
  return [&](std::function<void()> task, uint32_t delay_ms) {
    return execute_after(
        ioc,
        [=]() {
          RAY_LOG(INFO) << "delay_after running task...";
          task();
        },
        std::chrono::milliseconds(delay_ms));
  };
}

auto dummy_shutdown_raylet_gracefully = [](const rpc::NodeDeathInfo &) {};

TEST(RuntimeEnvAgentClientTest, GetOrCreateRuntimeEnvOK) {
  RayConfig::instance().initialize(R"({"auth_mode": "disabled"})");
  unsetenv("RAY_AUTH_TOKEN");
  rpc::AuthenticationTokenLoader::instance().ResetCache();

  int port = GetFreePort();
  HttpServerThread http_server_thread(
      [](const http::request<http::string_body> &request,
         http::response<http::string_body> &response) {
        rpc::GetOrCreateRuntimeEnvRequest req;
        ASSERT_TRUE(req.ParseFromString(request.body()));
        ASSERT_EQ(req.job_id(), "7b000000");  // Hex 7B == Int 123
        ASSERT_EQ(req.runtime_env_config().setup_timeout_seconds(), 12);
        ASSERT_EQ(req.serialized_runtime_env(), "serialized_runtime_env");
        ASSERT_EQ(request.find(http::field::authorization), request.end());

        rpc::GetOrCreateRuntimeEnvReply reply;
        reply.set_status(rpc::AGENT_RPC_STATUS_OK);
        reply.set_serialized_runtime_env_context("serialized_runtime_env_context");
        response.body() = reply.SerializeAsString();
        response.content_length(response.body().size());
        response.result(http::status::ok);
      },
      "127.0.0.1",
      port);
  http_server_thread.start();

  instrumented_io_context ioc;

  auto client =
      raylet::RuntimeEnvAgentClient::Create(ioc,
                                            "127.0.0.1",
                                            port,
                                            delay_after(ioc),
                                            dummy_shutdown_raylet_gracefully,
                                            /*agent_register_timeout_ms=*/10000,
                                            /*agent_manager_retry_interval_ms=*/100);
  auto job_id = JobID::FromInt(123);
  std::string serialized_runtime_env = "serialized_runtime_env";
  ray::rpc::RuntimeEnvConfig runtime_env_config;
  runtime_env_config.set_setup_timeout_seconds(12);

  size_t called_times = 0;
  auto callback = [&](bool successful,
                      const std::string &serialized_runtime_env_context,
                      const std::string &setup_error_message) {
    ASSERT_TRUE(successful);
    ASSERT_EQ(serialized_runtime_env_context, "serialized_runtime_env_context");
    ASSERT_TRUE(setup_error_message.empty());
    called_times += 1;
  };

  client->GetOrCreateRuntimeEnv(
      job_id, serialized_runtime_env, runtime_env_config, callback);

  ioc.run();
  ASSERT_EQ(called_times, 1);
}

TEST(RuntimeEnvAgentClientTest, GetOrCreateRuntimeEnvApplicationError) {
  int port = GetFreePort();
  HttpServerThread http_server_thread(
      [](const http::request<http::string_body> &request,
         http::response<http::string_body> &response) {
        rpc::GetOrCreateRuntimeEnvRequest req;
        ASSERT_TRUE(req.ParseFromString(request.body()));
        ASSERT_EQ(req.job_id(), "7b000000");  // Hex 7B == Int 123
        ASSERT_EQ(req.runtime_env_config().setup_timeout_seconds(), 12);
        ASSERT_EQ(req.serialized_runtime_env(), "serialized_runtime_env");

        rpc::GetOrCreateRuntimeEnvReply reply;
        reply.set_status(rpc::AGENT_RPC_STATUS_FAILED);
        reply.set_error_message("the server is not feeling well");
        response.body() = reply.SerializeAsString();
        response.content_length(response.body().size());
        response.result(http::status::ok);
      },
      "127.0.0.1",
      port);
  http_server_thread.start();

  instrumented_io_context ioc;

  auto client =
      raylet::RuntimeEnvAgentClient::Create(ioc,
                                            "127.0.0.1",
                                            port,
                                            delay_after(ioc),
                                            dummy_shutdown_raylet_gracefully,
                                            /*agent_register_timeout_ms=*/10000,
                                            /*agent_manager_retry_interval_ms=*/100);
  auto job_id = JobID::FromInt(123);
  std::string serialized_runtime_env = "serialized_runtime_env";
  ray::rpc::RuntimeEnvConfig runtime_env_config;
  runtime_env_config.set_setup_timeout_seconds(12);

  size_t called_times = 0;
  auto callback = [&](bool successful,
                      const std::string &serialized_runtime_env_context,
                      const std::string &setup_error_message) {
    ASSERT_FALSE(successful);
    ASSERT_TRUE(serialized_runtime_env_context.empty());
    ASSERT_EQ(setup_error_message, "the server is not feeling well");
    called_times += 1;
  };

  client->GetOrCreateRuntimeEnv(
      job_id, serialized_runtime_env, runtime_env_config, callback);

  ioc.run();
  ASSERT_EQ(called_times, 1);
}

// Client sends a request on `ioc`. request got NotFound
// We intercept in delay_scheduler to start the http_server_thread
// Next time, client retries and got OK, callback called
TEST(RuntimeEnvAgentClientTest, GetOrCreateRuntimeEnvRetriesOnServerNotStarted) {
  int port = GetFreePort();
  HttpServerThread http_server_thread(
      [](const http::request<http::string_body> &request,
         http::response<http::string_body> &response) {
        rpc::GetOrCreateRuntimeEnvRequest req;
        ASSERT_TRUE(req.ParseFromString(request.body()));
        ASSERT_EQ(req.job_id(), "7b000000");  // Hex 7B == Int 123
        ASSERT_EQ(req.runtime_env_config().setup_timeout_seconds(), 12);
        ASSERT_EQ(req.serialized_runtime_env(), "serialized_runtime_env");

        rpc::GetOrCreateRuntimeEnvReply reply;
        reply.set_status(rpc::AGENT_RPC_STATUS_OK);
        reply.set_serialized_runtime_env_context("serialized_runtime_env_context");
        response.body() = reply.SerializeAsString();
        response.content_length(response.body().size());
        response.result(http::status::ok);
      },
      "127.0.0.1",
      port);

  instrumented_io_context ioc;

  auto client = raylet::RuntimeEnvAgentClient::Create(
      ioc,
      "127.0.0.1",
      port,
      [&](std::function<void()> task, uint32_t delay_ms) {
        http_server_thread.start();
        return execute_after(ioc, task, std::chrono::milliseconds(delay_ms));
      },
      dummy_shutdown_raylet_gracefully,
      /*agent_register_timeout_ms=*/10000,
      /*agent_manager_retry_interval_ms=*/100);
  auto job_id = JobID::FromInt(123);
  std::string serialized_runtime_env = "serialized_runtime_env";
  ray::rpc::RuntimeEnvConfig runtime_env_config;
  runtime_env_config.set_setup_timeout_seconds(12);

  size_t called_times = 0;
  auto callback = [&](bool successful,
                      const std::string &serialized_runtime_env_context,
                      const std::string &setup_error_message) {
    ASSERT_TRUE(successful);
    ASSERT_EQ(serialized_runtime_env_context, "serialized_runtime_env_context");
    ASSERT_TRUE(setup_error_message.empty());
    called_times += 1;
  };

  client->GetOrCreateRuntimeEnv(
      job_id, serialized_runtime_env, runtime_env_config, callback);

  ioc.run();
  ASSERT_EQ(called_times, 1);
}

TEST(RuntimeEnvAgentClientTest, AttachesAuthHeaderWhenEnabled) {
  RayConfig::instance().initialize(R"({"auth_mode": "token"})");
  setenv("RAY_AUTH_TOKEN", "header_token", 1);
  rpc::AuthenticationTokenLoader::instance().ResetCache();

  int port = GetFreePort();
  std::string observed_auth_header;

  HttpServerThread http_server_thread(
      [&observed_auth_header](const http::request<http::string_body> &request,
                              http::response<http::string_body> &response) {
        rpc::GetOrCreateRuntimeEnvRequest req;
        ASSERT_TRUE(req.ParseFromString(request.body()));
        auto it = request.find(http::field::authorization);
        if (it != request.end()) {
          observed_auth_header = std::string(it->value());
        }

        rpc::GetOrCreateRuntimeEnvReply reply;
        reply.set_status(rpc::AGENT_RPC_STATUS_OK);
        reply.set_serialized_runtime_env_context("serialized_runtime_env_context");
        response.body() = reply.SerializeAsString();
        response.content_length(response.body().size());
        response.result(http::status::ok);
      },
      "127.0.0.1",
      port);
  http_server_thread.start();

  instrumented_io_context ioc;

  auto client =
      raylet::RuntimeEnvAgentClient::Create(ioc,
                                            "127.0.0.1",
                                            port,
                                            delay_after(ioc),
                                            dummy_shutdown_raylet_gracefully,
                                            /*agent_register_timeout_ms=*/10000,
                                            /*agent_manager_retry_interval_ms=*/100);

  auto job_id = JobID::FromInt(123);
  std::string serialized_runtime_env = "serialized_runtime_env";
  ray::rpc::RuntimeEnvConfig runtime_env_config;
  runtime_env_config.set_setup_timeout_seconds(12);

  size_t called_times = 0;
  auto callback = [&](bool successful,
                      const std::string &serialized_runtime_env_context,
                      const std::string &setup_error_message) {
    ASSERT_TRUE(successful);
    ASSERT_EQ(serialized_runtime_env_context, "serialized_runtime_env_context");
    ASSERT_TRUE(setup_error_message.empty());
    called_times += 1;
  };

  client->GetOrCreateRuntimeEnv(
      job_id, serialized_runtime_env, runtime_env_config, callback);

  ioc.run();

  ASSERT_EQ(called_times, 1);
  ASSERT_EQ(observed_auth_header, "Bearer header_token");

  RayConfig::instance().initialize(R"({"auth_mode": "disabled"})");
  unsetenv("RAY_AUTH_TOKEN");
  rpc::AuthenticationTokenLoader::instance().ResetCache();
}

TEST(RuntimeEnvAgentClientTest, DeleteRuntimeEnvIfPossibleOK) {
  int port = GetFreePort();
  HttpServerThread http_server_thread(
      [](const http::request<http::string_body> &request,
         http::response<http::string_body> &response) {
        rpc::DeleteRuntimeEnvIfPossibleRequest req;
        ASSERT_TRUE(req.ParseFromString(request.body()));
        ASSERT_EQ(req.serialized_runtime_env(), "serialized_runtime_env");
        ASSERT_EQ(req.source_process(), "raylet");

        rpc::DeleteRuntimeEnvIfPossibleReply reply;
        reply.set_status(rpc::AGENT_RPC_STATUS_OK);
        response.body() = reply.SerializeAsString();
        response.content_length(response.body().size());
        response.result(http::status::ok);
      },
      "127.0.0.1",
      port);
  http_server_thread.start();

  instrumented_io_context ioc;

  auto client =
      raylet::RuntimeEnvAgentClient::Create(ioc,
                                            "127.0.0.1",
                                            port,
                                            delay_after(ioc),
                                            dummy_shutdown_raylet_gracefully,
                                            /*agent_register_timeout_ms=*/10000,
                                            /*agent_manager_retry_interval_ms=*/100);

  size_t called_times = 0;
  auto callback = [&](bool successful) {
    ASSERT_TRUE(successful);
    called_times += 1;
  };

  client->DeleteRuntimeEnvIfPossible("serialized_runtime_env", callback);

  ioc.run();
  ASSERT_EQ(called_times, 1);
}

TEST(RuntimeEnvAgentClientTest, DeleteRuntimeEnvIfPossibleApplicationError) {
  int port = GetFreePort();
  HttpServerThread http_server_thread(
      [](const http::request<http::string_body> &request,
         http::response<http::string_body> &response) {
        rpc::DeleteRuntimeEnvIfPossibleRequest req;
        ASSERT_TRUE(req.ParseFromString(request.body()));
        ASSERT_EQ(req.serialized_runtime_env(), "serialized_runtime_env");
        ASSERT_EQ(req.source_process(), "raylet");

        rpc::DeleteRuntimeEnvIfPossibleReply reply;
        reply.set_status(rpc::AGENT_RPC_STATUS_FAILED);
        reply.set_error_message("server is not feeling well");
        response.body() = reply.SerializeAsString();
        response.content_length(response.body().size());
        response.result(http::status::ok);
      },
      "127.0.0.1",
      port);
  http_server_thread.start();

  instrumented_io_context ioc;

  auto client =
      raylet::RuntimeEnvAgentClient::Create(ioc,
                                            "127.0.0.1",
                                            port,
                                            delay_after(ioc),
                                            dummy_shutdown_raylet_gracefully,
                                            /*agent_register_timeout_ms=*/10000,
                                            /*agent_manager_retry_interval_ms=*/100);

  size_t called_times = 0;
  auto callback = [&](bool successful) {
    ASSERT_FALSE(successful);
    called_times += 1;
  };

  client->DeleteRuntimeEnvIfPossible("serialized_runtime_env", callback);

  ioc.run();
  ASSERT_EQ(called_times, 1);
}

// Client sends a request on `ioc`. request got NotFound
// We intercept in delay_scheduler to start the http_server_thread
// Next time, client retries and got OK, callback called
TEST(RuntimeEnvAgentClientTest, DeleteRuntimeEnvIfPossibleRetriesOnServerNotStarted) {
  int port = GetFreePort();
  HttpServerThread http_server_thread(
      [](const http::request<http::string_body> &request,
         http::response<http::string_body> &response) {
        rpc::DeleteRuntimeEnvIfPossibleRequest req;
        ASSERT_TRUE(req.ParseFromString(request.body()));
        ASSERT_EQ(req.serialized_runtime_env(), "serialized_runtime_env");
        ASSERT_EQ(req.source_process(), "raylet");

        rpc::DeleteRuntimeEnvIfPossibleReply reply;
        reply.set_status(rpc::AGENT_RPC_STATUS_FAILED);
        reply.set_error_message("server is not feeling well");
        response.body() = reply.SerializeAsString();
        response.content_length(response.body().size());
        response.result(http::status::ok);
      },
      "127.0.0.1",
      port);

  instrumented_io_context ioc;

  auto client = raylet::RuntimeEnvAgentClient::Create(
      ioc,
      "127.0.0.1",
      port,
      [&](std::function<void()> task, uint32_t delay_ms) {
        http_server_thread.start();
        return execute_after(ioc, task, std::chrono::milliseconds(delay_ms));
      },
      dummy_shutdown_raylet_gracefully,
      /*agent_register_timeout_ms=*/10000,
      /*agent_manager_retry_interval_ms=*/100);

  size_t called_times = 0;
  auto callback = [&](bool successful) {
    ASSERT_FALSE(successful);
    called_times += 1;
  };

  client->DeleteRuntimeEnvIfPossible("serialized_runtime_env", callback);

  ioc.run();
  ASSERT_EQ(called_times, 1);
}

// Why do we make a constexpr function instead of a variable? It's compilers' hubris.
// The number is used in a lambda. Being constexpr, it it *not* required to be captured.
// Hence If we capture it, in macos and linux we have this error:
//
// error: lambda capture 'expected_succs' is not required to be captured for this use
// [-Werror,-Wunused-lambda-capture]
//
// So let's remove it from the capture list! Then in windows we have this:
//
// error C3493: 'expected_succs' cannot be implicitly captured because no default capture
// mode has been specified
//
// Good job MSVC for not following the standard!
constexpr size_t expected_succs() { return 42; }

TEST(RuntimeEnvAgentClientTest, HoldsConcurrency) {
  int port = GetFreePort();

  // Async HTTP server that completes the request after sleeping 1 sec.
  // The server returns success in the first 42 requests, then all failures.
  std::mutex server_mutex;
  size_t concurrency = 0;
  size_t max_concurrency = 0;
  size_t issued_succs = 0;
  HttpServerThread http_server_thread(
      [&](std::shared_ptr<HttpConnection> conn) {
        {
          std::lock_guard lock(server_mutex);
          concurrency += 1;
        }

        auto timer = std::make_shared<boost::asio::steady_timer>(conn->ioc_,
                                                                 std::chrono::seconds(1));
        timer->async_wait(
            [timer, conn, &server_mutex, &max_concurrency, &concurrency, &issued_succs](
                const boost::system::error_code &ec) {
              // We have to redefine this number again due to compilers being picky &
              // fool.

              const http::request<http::string_body> &request = conn->request_;
              http::response<http::string_body> &response = conn->response_;

              rpc::DeleteRuntimeEnvIfPossibleRequest req;
              ASSERT_TRUE(req.ParseFromString(request.body()));
              ASSERT_EQ(req.serialized_runtime_env(), "serialized_runtime_env");
              ASSERT_EQ(req.source_process(), "raylet");

              rpc::DeleteRuntimeEnvIfPossibleReply reply;
              {
                std::lock_guard lock(server_mutex);

                if (issued_succs < expected_succs()) {
                  reply.set_status(rpc::AGENT_RPC_STATUS_OK);
                  issued_succs += 1;
                } else {
                  reply.set_status(rpc::AGENT_RPC_STATUS_FAILED);
                  reply.set_error_message("the server is not feeling well");
                }
              }
              response.body() = reply.SerializeAsString();
              response.content_length(response.body().size());
              response.result(http::status::ok);
              {
                std::lock_guard lock(server_mutex);
                RAY_LOG(INFO) << "server sending " << concurrency
                              << " concurrent results, max = " << max_concurrency;
                max_concurrency = std::max(max_concurrency, concurrency);
                concurrency -= 1;
              }
              conn->write_response();
            });
      },
      "127.0.0.1",
      port);
  http_server_thread.start();

  instrumented_io_context ioc;

  auto client =
      raylet::RuntimeEnvAgentClient::Create(ioc,
                                            "127.0.0.1",
                                            port,
                                            delay_after(ioc),
                                            dummy_shutdown_raylet_gracefully,
                                            /*agent_register_timeout_ms=*/10000,
                                            /*agent_manager_retry_interval_ms=*/100);

  std::atomic<size_t> seen_succs = 0;
  std::atomic<size_t> seen_failures = 0;
  auto callback = [&](bool successful) {
    if (successful) {
      seen_succs += 1;
    } else {
      seen_failures += 1;
    }
  };

  for (int i = 0; i < 100; ++i) {
    client->DeleteRuntimeEnvIfPossible("serialized_runtime_env", callback);
  }

  ioc.run();
  EXPECT_EQ(issued_succs, expected_succs());
  EXPECT_EQ(seen_succs, expected_succs());
  EXPECT_EQ(seen_failures, 100 - expected_succs());
  EXPECT_EQ(concurrency, 0);
  EXPECT_EQ(max_concurrency, 10);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
