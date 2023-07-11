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

#include <boost/asio/ip/tcp.hpp>
#include <boost/beast.hpp>
#include <boost/beast/http.hpp>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <string>

#include "absl/strings/str_format.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/ray_config.h"
#include "ray/common/status.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/runtime_env_agent.pb.h"

namespace beast = boost::beast;  // from <boost/beast.hpp>
namespace http = beast::http;    // from <boost/beast/http.hpp>
namespace net = boost::asio;     // from <boost/asio.hpp>
using tcp = net::ip::tcp;        // from <boost/asio/ip/tcp.hpp>

namespace ray {
namespace raylet {

namespace {

//------------------------------------------------------------------------------
// Simple class to make a async POST call.
// Will call callback exactly once with pair{non-ok, any} or pair{ok, reply body}.
//
// Hard coded behavior:
// - content type is "application/octet-stream".
// - connection has no timeout (i.e. waits forever. This is because runtime env agent can
// work for a long time.)
// - on_resolve and on_connect failures return NotFound. This allows retry on the
// server not (yet) started up.
// - on_read and on_write failures return IOError.
//
// Spirit from
// https://www.boost.org/doc/libs/develop/libs/beast/example/http/client/async/http_client_async.cpp
class Session : public std::enable_shared_from_this<Session> {
  tcp::resolver resolver_;
  beast::tcp_stream stream_;
  std::string host_;
  std::string port_;
  std::function<void(std::string)> succ_callback_;
  std::function<void(ray::Status)> fail_callback_;
  beast::flat_buffer buffer_;  // (Must persist between reads)
  http::request<http::string_body> req_;
  http::response<http::string_body> res_;

 public:
  // Factory method.
  // Not exposing ctor because it's expected to always be in a shared_ptr.
  static std::shared_ptr<Session> Create(net::io_context &ioc,
                                         const std::string &host,
                                         const std::string &port,
                                         const std::string &target,
                                         std::string body,
                                         std::function<void(std::string)> succ_callback,
                                         std::function<void(ray::Status)> fail_callback) {
    // C++ limitations: make_shared can't be used because std::shared_ptr can't invoke
    // private ctor.
    return std::shared_ptr<Session>(new Session(ioc,
                                                host,
                                                port,
                                                target,
                                                std::move(body),
                                                std::move(succ_callback),
                                                std::move(fail_callback)));
  }

  void run() {
    // Starts the state machine by looking up the domain name.
    resolver_.async_resolve(
        host_,
        port_,
        beast::bind_front_handler(&Session::on_resolve, shared_from_this()));
  }

 private:
  explicit Session(net::io_context &ioc,
                   const std::string &host,
                   const std::string &port,
                   const std::string &target,
                   std::string body,
                   std::function<void(std::string)> succ_callback,
                   std::function<void(ray::Status)> fail_callback)
      : resolver_(ioc),
        stream_(ioc),
        host_(host),
        port_(port),
        succ_callback_(std::move(succ_callback)),
        fail_callback_(std::move(fail_callback)) {
    stream_.expires_never();
    req_.method(http::verb::post);
    req_.target(target);
    req_.body() = std::move(body);
    req_.set(http::field::host, host);
    req_.set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);
    req_.set(http::field::content_type, "application/octet-stream");
    // aiohttp has a bug that, if you don't set this value, it returns 400.
    // https://github.com/aio-libs/aiohttp/issues/7208
    req_.content_length(req_.body().size());
  }

  void on_resolve(beast::error_code ec, tcp::resolver::results_type results) {
    if (ec) {
      fail_callback_(ray::Status::NotFound("on_resolve " + ec.message()));
      return;
    }

    // Make the connection on the IP address we get from a lookup
    stream_.async_connect(
        results, beast::bind_front_handler(&Session::on_connect, shared_from_this()));
  }

  void on_connect(beast::error_code ec, tcp::resolver::results_type::endpoint_type) {
    if (ec) {
      fail_callback_(ray::Status::NotFound("on_connect " + ec.message()));
      return;
    }

    // Send the HTTP request to the remote host
    http::async_write(
        stream_, req_, beast::bind_front_handler(&Session::on_write, shared_from_this()));
  }

  void on_write(beast::error_code ec, std::size_t bytes_transferred) {
    boost::ignore_unused(bytes_transferred);

    if (ec) {
      fail_callback_(ray::Status::IOError("on_write " + ec.message()));
      return;
    }

    // Receive the HTTP response
    http::async_read(stream_,
                     buffer_,
                     res_,
                     beast::bind_front_handler(&Session::on_read, shared_from_this()));
  }

  void on_read(beast::error_code ec, std::size_t bytes_transferred) {
    boost::ignore_unused(bytes_transferred);

    if (ec) {
      fail_callback_(ray::Status::IOError("on_read " + ec.message()));
      return;
    }
    if (http::to_status_class(res_.result()) == http::status_class::successful) {
      succ_callback_(std::move(res_).body());
    } else {
      fail_callback_(ray::Status::IOError(" POST result non-ok status code " +
                                          std::to_string(res_.result_int())));
    }

    // Gracefully close the socket
    stream_.socket().shutdown(tcp::socket::shutdown_both, ec);
    // not_connected happens sometimes so don't bother reporting it.
    if (ec && ec != beast::errc::not_connected) {
      RAY_LOG(INFO) << "on_read error after response body reseived: " << ec.message();
    }
  }
};

class HttpRuntimeEnvAgentClient : public RuntimeEnvAgentClient {
 public:
  HttpRuntimeEnvAgentClient(instrumented_io_context &io_context,
                            const std::string &address,
                            int port,
                            std::function<std::shared_ptr<boost::asio::deadline_timer>(
                                std::function<void()>, uint32_t delay_ms)> delay_executor)
      : io_context_(io_context),
        address_(address),
        port_str_(std::to_string(port)),
        delay_executor_(delay_executor) {}
  ~HttpRuntimeEnvAgentClient() {}

  template <typename T>
  using SuccCallback = std::function<void(T)>;
  using FailCallback = std::function<void(ray::Status)>;
  template <typename T>
  using TryInvokeOnce = std::function<void(SuccCallback<T>, FailCallback)>;

  /// @brief Invoke `try_invoke_once`. If it fails with a retriable error, retries after
  /// `agent_manager_retry_interval_ms` via the `delay_executor_`.
  //
  // "Retriable error" means `IsNotFound`.
  /// @tparam T the return type on success.
  /// @param try_invoke_once
  /// @param succ_callback
  /// @param fail_callback
  template <typename T>
  void RetryInvoke(TryInvokeOnce<T> try_invoke_once,
                   SuccCallback<T> succ_callback,
                   FailCallback fail_callback) {
    try_invoke_once(succ_callback, [=](ray::Status status) {
      if (status.IsNotFound()) {
        RAY_LOG(INFO) << "Scheduling a retry in "
                      << RayConfig::instance().agent_manager_retry_interval_ms()
                      << "ms...";
        this->delay_executor_(
            [=]() { RetryInvoke(try_invoke_once, succ_callback, fail_callback); },
            RayConfig::instance().agent_manager_retry_interval_ms());
      } else {
        // Non retryable errors, invoke fail_callback
        fail_callback(std::move(status));
      }
    });
  }

  // Making HTTP call.
  // POST /get_or_create_runtime_env
  // Body = proto rpc::GetOrCreateRuntimeEnvRequest
  void GetOrCreateRuntimeEnv(const JobID &job_id,
                             const std::string &serialized_runtime_env,
                             const rpc::RuntimeEnvConfig &runtime_env_config,
                             const std::string &serialized_allocated_resource_instances,
                             GetOrCreateRuntimeEnvCallback callback) override {
    RetryInvoke<rpc::GetOrCreateRuntimeEnvReply>(
        [=](SuccCallback<rpc::GetOrCreateRuntimeEnvReply> succ_callback,
            FailCallback fail_callback) {
          return TryGetOrCreateRuntimeEnv(job_id,
                                          serialized_runtime_env,
                                          runtime_env_config,
                                          serialized_allocated_resource_instances,
                                          succ_callback,
                                          fail_callback);
        },
        /*succ_callback=*/
        [=](rpc::GetOrCreateRuntimeEnvReply reply) {
          if (reply.status() != rpc::AGENT_RPC_STATUS_OK) {
            RAY_LOG(INFO) << "Failed to create runtime env for job " << job_id
                          << ", error message: " << reply.error_message();
            RAY_LOG(DEBUG) << "Serialized runtime env for job " << job_id << ": "
                           << serialized_runtime_env;
            callback(false,
                     reply.serialized_runtime_env_context(),
                     /*setup_error_message*/ reply.error_message());
          } else {
            RAY_LOG(INFO) << "Create runtime env for job " << job_id;
            callback(true,
                     reply.serialized_runtime_env_context(),
                     /*setup_error_message*/ "");
          }
        },
        /*fail_callback=*/
        [=](ray::Status status) {
          RAY_LOG(INFO)
              << "Failed to create runtime env for job " << job_id
              << ", status = " << status
              << ", maybe there are some network problems, will fail the request.";
          RAY_LOG(DEBUG) << "Serialized runtime env for job " << job_id << ": "
                         << serialized_runtime_env;
          callback(false, "", "Failed to request agent.");
        });
  }

  // Does the real work of calling HTTP.
  // Returns {error, empty} or {ok, reply}.
  void TryGetOrCreateRuntimeEnv(
      const JobID &job_id,
      const std::string &serialized_runtime_env,
      const rpc::RuntimeEnvConfig &runtime_env_config,
      const std::string &serialized_allocated_resource_instances,
      std::function<void(rpc::GetOrCreateRuntimeEnvReply)> succ_callback,
      std::function<void(ray::Status)> fail_callback) {
    rpc::GetOrCreateRuntimeEnvRequest request;
    request.set_job_id(job_id.Hex());
    request.set_serialized_runtime_env(serialized_runtime_env);
    request.mutable_runtime_env_config()->CopyFrom(runtime_env_config);
    request.set_serialized_allocated_resource_instances(
        serialized_allocated_resource_instances);
    std::string payload = request.SerializeAsString();

    auto session = Session::Create(
        io_context_,
        address_,
        port_str_,
        "/get_or_create_runtime_env",
        std::move(payload),
        /*succ_callback=*/
        [succ_callback, fail_callback](std::string body) {
          rpc::GetOrCreateRuntimeEnvReply reply;
          if (!reply.ParseFromString(body)) {
            fail_callback(Status::IOError("protobuf parse error"));
          } else {
            succ_callback(std::move(reply));
          }
        },
        fail_callback);
    session->run();
  }

  // Making HTTP call.
  // POST /delete_runtime_env_if_possible
  // Body = proto rpc::DeleteRuntimeEnvIfPossibleRequest
  void DeleteRuntimeEnvIfPossible(const std::string &serialized_runtime_env,
                                  DeleteRuntimeEnvIfPossibleCallback callback) override {
    RetryInvoke<rpc::DeleteRuntimeEnvIfPossibleReply>(
        [=](SuccCallback<rpc::DeleteRuntimeEnvIfPossibleReply> succ_callback,
            FailCallback fail_callback) {
          return TryDeleteRuntimeEnvIfPossible(
              serialized_runtime_env, succ_callback, fail_callback);
        },
        /*succ_callback=*/
        [=](rpc::DeleteRuntimeEnvIfPossibleReply reply) {
          if (reply.status() != rpc::AGENT_RPC_STATUS_OK) {
            // TODO(sang): Find a better way to delivering error messages in this
            RAY_LOG(ERROR) << "Failed to delete runtime env "
                           << ", error message: " << reply.error_message();
            RAY_LOG(DEBUG) << "Serialized runtime env: " << serialized_runtime_env;
            callback(false);
          } else {
            callback(true);
          }
        },
        /*fail_callback=*/
        [=](ray::Status status) {
          RAY_LOG(ERROR)
              << "Failed to delete runtime env reference, status = " << status
              << ", maybe there are some network problems, will fail the request.";
          RAY_LOG(DEBUG) << "Serialized runtime env: " << serialized_runtime_env;
          callback(false);
        });
  }

  void TryDeleteRuntimeEnvIfPossible(
      const std::string &serialized_runtime_env,
      std::function<void(rpc::DeleteRuntimeEnvIfPossibleReply)> succ_callback,
      std::function<void(ray::Status)> fail_callback) {
    rpc::DeleteRuntimeEnvIfPossibleRequest request;
    request.set_serialized_runtime_env(serialized_runtime_env);
    request.set_source_process("raylet");
    std::string payload = request.SerializeAsString();

    auto session = Session::Create(
        io_context_,
        address_,
        port_str_,
        "/delete_runtime_env_if_possible",
        std::move(payload),
        /*succ_callback=*/
        [succ_callback, fail_callback](std::string body) {
          rpc::DeleteRuntimeEnvIfPossibleReply reply;
          if (!reply.ParseFromString(body)) {
            fail_callback(Status::IOError("protobuf parse error"));
          } else {
            succ_callback(std::move(reply));
          }
        },
        fail_callback);
    session->run();
  }

 private:
  boost::asio::io_context &io_context_;

  const std::string address_;
  const std::string port_str_;

  std::function<std::shared_ptr<boost::asio::deadline_timer>(std::function<void()>,
                                                             uint32_t delay_ms)>
      delay_executor_;
};
}  // namespace

std::shared_ptr<RuntimeEnvAgentClient> RuntimeEnvAgentClient::Create(
    instrumented_io_context &io_context,
    const std::string &address,
    int port,
    std::function<std::shared_ptr<boost::asio::deadline_timer>(
        std::function<void()>, uint32_t delay_ms)> delay_executor) {
  return std::make_shared<HttpRuntimeEnvAgentClient>(
      io_context, address, port, delay_executor);
}

}  // namespace raylet
}  // namespace ray