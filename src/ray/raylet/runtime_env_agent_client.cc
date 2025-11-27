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
#include <queue>
#include <string>
#include <utility>

#include "absl/container/flat_hash_set.h"
#include "absl/strings/str_format.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/status.h"
#include "ray/rpc/authentication/authentication_token_loader.h"
#include "ray/util/logging.h"
#include "ray/util/process.h"
#include "ray/util/time.h"
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
// - method is POST.
// - version is HTTP/1.1.
// - content type is "application/octet-stream".
// - connection has infinite timeout (This is because runtime env agent can
// work for a long time.)
//
// Error handling: (return means invoking the fail_callback with the error)
// - on_resolve and on_connect failures return NotFound.
// - on_write and on_read failures return Disconnected.
// - if the HTTP response is received and well-formed, but the status code is not OK,
//  return IOError.
//
// Spirit from
// https://www.boost.org/doc/libs/develop/libs/beast/example/http/client/async/http_client_async.cpp
class Session : public std::enable_shared_from_this<Session> {
 public:
  using FinishedCallback = std::function<void(std::shared_ptr<Session>)>;

  // Factory method.
  // Not exposing ctor because it's expected to always be in a shared_ptr.
  static std::shared_ptr<Session> Create(net::io_context &ioc,
                                         std::string_view host,
                                         std::string_view port,
                                         http::verb method,
                                         std::string_view target,
                                         std::string body,
                                         std::function<void(std::string)> succ_callback,
                                         std::function<void(ray::Status)> fail_callback) {
    // C++ limitations: make_shared can't be used because std::shared_ptr can't invoke
    // private ctor.
    return std::shared_ptr<Session>(new Session(ioc,
                                                host,
                                                port,
                                                method,
                                                target,
                                                std::move(body),
                                                std::move(succ_callback),
                                                std::move(fail_callback)));
  }

  // Runs the session asynchrounously. Immediately returns.
  // It's ok to release a shared_ptr to `this` because the io context will hold a
  // shared_ptr that holds a reference to `this`.
  //
  // This method should only be called once.
  void run(FinishedCallback finished_callback) {
    finished_callback_ = std::move(finished_callback);
    // Starts the state machine by looking up the domain name.
    resolver_.async_resolve(
        host_,
        port_,
        beast::bind_front_handler(&Session::on_resolve, shared_from_this()));
  }

 private:
  explicit Session(net::io_context &ioc,
                   std::string_view host,
                   std::string_view port,
                   http::verb method,
                   std::string_view target,
                   std::string body,
                   std::function<void(std::string)> succ_callback,
                   std::function<void(ray::Status)> fail_callback)
      : resolver_(ioc),
        stream_(ioc),
        host_(std::string(host)),
        port_(std::string(port)),
        method_(method),
        succ_callback_(std::move(succ_callback)),
        fail_callback_(std::move(fail_callback)) {
    stream_.expires_never();
    req_.method(method_);
    req_.target(target);
    req_.body() = std::move(body);
    req_.version(11);  // HTTP/1.1
    req_.set(http::field::host, host);
    req_.set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);
    req_.set(http::field::content_type, "application/octet-stream");
    // Sets Content-Length header.
    req_.prepare_payload();

    auto auth_token = rpc::AuthenticationTokenLoader::instance().GetToken();
    if (auth_token.has_value() && !auth_token->empty()) {
      req_.set(http::field::authorization, auth_token->ToAuthorizationHeaderValue());
    }
  }

  void Failed(ray::Status status) {
    fail_callback_(std::move(status));
    finished_callback_(shared_from_this());
  }

  void Succeeded(std::string body) {
    succ_callback_(std::move(body));
    finished_callback_(shared_from_this());
  }

  void on_resolve(beast::error_code ec, tcp::resolver::results_type results) {
    if (ec) {
      Failed(ray::Status::NotFound("on_resolve " + ec.message()));
      return;
    }

    stream_.expires_never();
    // Make the connection on the IP address we get from a lookup
    stream_.async_connect(
        results, beast::bind_front_handler(&Session::on_connect, shared_from_this()));
  }

  void on_connect(beast::error_code ec, tcp::resolver::results_type::endpoint_type) {
    if (ec) {
      Failed(ray::Status::NotFound(absl::StrCat("on_connect ", ec.message())));
      return;
    }

    stream_.expires_never();
    // Send the HTTP request to the remote host
    http::async_write(
        stream_, req_, beast::bind_front_handler(&Session::on_write, shared_from_this()));
  }

  void on_write(beast::error_code ec, std::size_t bytes_transferred) {
    if (ec) {
      Failed(ray::Status::Disconnected(absl::StrCat(
          "on_write ", ec.message(), ", bytes_transferred ", bytes_transferred)));
      return;
    }
    stream_.expires_never();
    // Receive the HTTP response
    http::async_read(stream_,
                     buffer_,
                     res_,
                     beast::bind_front_handler(&Session::on_read, shared_from_this()));
  }

  void on_read(beast::error_code ec, std::size_t bytes_transferred) {
    if (ec) {
      Failed(ray::Status::Disconnected(absl::StrCat(
          "on_read ", ec.message(), ", bytes_transferred ", bytes_transferred)));
      return;
    }

    if (http::to_status_class(res_.result()) == http::status_class::successful) {
      Succeeded(std::move(res_).body());
    } else {
      Failed(ray::Status::IOError(absl::StrCat("HTTP request returns non-ok status code ",
                                               res_.result_int(),
                                               ", body",
                                               std::move(res_).body())));
    }

    // Gracefully close the socket
    stream_.socket().shutdown(tcp::socket::shutdown_both, ec);
    // not_connected happens sometimes so don't bother reporting it.
    if (ec && ec != beast::errc::not_connected) {
      RAY_LOG(INFO) << "on_read error after response body received: " << ec.message();
    }
  }

  tcp::resolver resolver_;
  beast::tcp_stream stream_;
  std::string host_;
  std::string port_;
  http::verb method_;
  std::function<void(std::string)> succ_callback_;
  std::function<void(ray::Status)> fail_callback_;
  beast::flat_buffer buffer_;  // (Must persist between reads)
  http::request<http::string_body> req_;
  http::response<http::string_body> res_;
  FinishedCallback finished_callback_;
};

// A pool of sessions with a fixed max concurrency. Each session can handle 1 concurrent
// request.
// Users can post a session to this pool, but should not *run* them. The Session pool runs
// them if the concurrency is not exceeding the desired value.
//
// Once a session is done, remove it from the pool.
//
// NOT thread safe: if the methods or the fallbacks are invoked in different threads, the
// workloads may be lost or the running order may not be fair.
class SessionPool {
 public:
  explicit SessionPool(size_t max_concurrency)
      : max_concurrency_(max_concurrency), running_sessions_(), pending_sessions_() {}

  void enqueue(std::shared_ptr<Session> session) {
    if (running_sessions_.size() < max_concurrency_) {
      running_sessions_.insert(session);
      session->run(
          /*finished_callback=*/[this](std::shared_ptr<Session> session_to_remove) {
            this->remove_session_from_running(session_to_remove);
          });
    } else {
      pending_sessions_.emplace(std::move(session));
    }
  }

 private:
  // Removes 1 session from running. After that, we have 1 free session slot so we can
  // enqueue one.
  void remove_session_from_running(std::shared_ptr<Session> session) {
    running_sessions_.erase(session);
    if (!pending_sessions_.empty()) {
      auto pending = std::move(pending_sessions_.front());
      pending_sessions_.pop();
      enqueue(std::move(pending));
    }
  }

  const size_t max_concurrency_;
  absl::flat_hash_set<std::shared_ptr<Session>> running_sessions_;
  std::queue<std::shared_ptr<Session>> pending_sessions_;
};

inline constexpr std::string_view HTTP_PATH_GET_OR_CREATE_RUNTIME_ENV =
    "/get_or_create_runtime_env";
inline constexpr std::string_view HTTP_PATH_DELETE_RUNTIME_ENV_IF_POSSIBLE =
    "/delete_runtime_env_if_possible";

class HttpRuntimeEnvAgentClient : public RuntimeEnvAgentClient {
 public:
  HttpRuntimeEnvAgentClient(
      instrumented_io_context &io_context,
      const std::string &address,
      int port,
      std::function<std::shared_ptr<boost::asio::deadline_timer>(
          std::function<void()>, uint32_t delay_ms)> delay_executor,
      std::function<void(const rpc::NodeDeathInfo &)> shutdown_raylet_gracefully,
      uint32_t agent_register_timeout_ms,
      uint32_t agent_manager_retry_interval_ms,
      uint32_t session_pool_size = 10)
      : io_context_(io_context),
        session_pool_(session_pool_size),
        address_(address),
        port_str_(absl::StrFormat("%d", port)),
        delay_executor_(delay_executor),
        shutdown_raylet_gracefully_(shutdown_raylet_gracefully),
        agent_register_timeout_ms_(agent_register_timeout_ms),
        agent_manager_retry_interval_ms_(agent_manager_retry_interval_ms) {}
  ~HttpRuntimeEnvAgentClient() override = default;

  template <typename T>
  using SuccCallback = std::function<void(T)>;
  using FailCallback = std::function<void(ray::Status)>;
  template <typename T>
  using TryInvokeOnce = std::function<void(SuccCallback<T>, FailCallback)>;

  void ExitImmediately() {
    RAY_LOG(ERROR)
        << "The raylet exited immediately because the runtime env agent timed out when "
           "Raylet try to connect to it. This can happen because the runtime env agent "
           "was never started, or is listening to the wrong port. Read the log `cat "
           "/tmp/ray/session_latest/logs/runtime_env_agent.log`. You can find the log "
           "file structure here "
           "https://docs.ray.io/en/master/ray-observability/user-guides/"
           "configure-logging.html#logging-directory-structure.\n";
    rpc::NodeDeathInfo node_death_info;
    node_death_info.set_reason(rpc::NodeDeathInfo::UNEXPECTED_TERMINATION);
    node_death_info.set_reason_message("Raylet could not connect to Runtime Env Agent");
    shutdown_raylet_gracefully_(node_death_info);
    // If the process is not terminated within 10 seconds, forcefully kill itself.
    delay_executor_([]() { QuickExit(); }, /*ms*/ 10000);
  }

  /// @brief Invokes `try_invoke_once`. If it fails with a network error, retries every
  /// after `agent_manager_retry_interval_ms` up until `deadline` passed. After which,
  /// fail_callback is called with the NotFound error from `try_invoke_once`.
  ///
  /// Note that retry only happens on network errors, i.e. NotFound and Disconnected, on
  /// which cases we did not receive a well-formed HTTP response. Application errors
  /// returned by the server are not retried.
  ///
  /// If the retries took so long and exceeded deadline, Raylet exits immediately. Note
  /// the check happens after `try_invoke_once` returns. This means if you have a
  /// successful but very long connection (e.g. runtime env agent is busy downloading
  /// from s3), you are safe.
  ///
  /// @tparam T the return type on success.
  /// @param try_invoke_once
  /// @param succ_callback
  /// @param fail_callback
  /// @param deadline
  template <typename T>
  void RetryInvokeOnNotFoundWithDeadline(TryInvokeOnce<T> try_invoke_once,
                                         SuccCallback<T> succ_callback,
                                         FailCallback fail_callback,
                                         int64_t deadline_ms) {
    try_invoke_once(succ_callback, [=](ray::Status status) {
      if ((!status.IsNotFound()) && (!status.IsDisconnected())) {
        // Non retryable errors, invoke fail_callback
        fail_callback(status);
      } else if (current_time_ms() > deadline_ms) {
        RAY_LOG(ERROR) << "Runtime Env Agent timed out in " << agent_register_timeout_ms_
                       << "ms. Status: " << status << ", address: " << this->address_
                       << ", port: " << this->port_str_ << ", existing immediately...";
        ExitImmediately();
      } else {
        RAY_LOG(INFO) << "Runtime Env Agent network error: " << status
                      << ", the server may be still starting or is already failed. "
                         "Scheduling a retry in "
                      << agent_manager_retry_interval_ms_ << "ms...";
        this->delay_executor_(
            [=]() {
              RetryInvokeOnNotFoundWithDeadline(
                  try_invoke_once, succ_callback, fail_callback, deadline_ms);
            },
            agent_manager_retry_interval_ms_);
      }
    });
  }

  // Making HTTP call.
  // POST /get_or_create_runtime_env
  // Body = proto rpc::GetOrCreateRuntimeEnvRequest
  void GetOrCreateRuntimeEnv(const JobID &job_id,
                             const std::string &serialized_runtime_env,
                             const rpc::RuntimeEnvConfig &runtime_env_config,
                             GetOrCreateRuntimeEnvCallback callback) override {
    RetryInvokeOnNotFoundWithDeadline<rpc::GetOrCreateRuntimeEnvReply>(
        [=](SuccCallback<rpc::GetOrCreateRuntimeEnvReply> succ_callback,
            FailCallback fail_callback) {
          return TryGetOrCreateRuntimeEnv(job_id,
                                          serialized_runtime_env,
                                          runtime_env_config,
                                          succ_callback,
                                          fail_callback);
        },
        /*succ_callback=*/
        [=](rpc::GetOrCreateRuntimeEnvReply reply) {
          // HTTP request & protobuf parsing succeeded, but we got a non-OK from the
          // remote server.
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
          std::string error_message = absl::StrCat(
              "Failed to create runtime env for job ",
              job_id.Hex(),
              ", status = ",
              status.ToString(),
              ", maybe there are some network problems, will fail the request.");
          RAY_LOG(INFO) << error_message;
          RAY_LOG(DEBUG) << "Serialized runtime env for job " << job_id << ": "
                         << serialized_runtime_env;
          callback(false, "", error_message);
        },
        current_time_ms() + agent_register_timeout_ms_);
  }

  // Does the real work of calling HTTP.
  // Invokes `succ_callback` with server reply (which may be OK or application errors),
  // or invokes `fail_callback` on network error or protobuf deserialization error.
  void TryGetOrCreateRuntimeEnv(
      const JobID &job_id,
      const std::string &serialized_runtime_env,
      const rpc::RuntimeEnvConfig &runtime_env_config,
      std::function<void(rpc::GetOrCreateRuntimeEnvReply)> succ_callback,
      std::function<void(ray::Status)> fail_callback) {
    rpc::GetOrCreateRuntimeEnvRequest request;
    request.set_job_id(job_id.Hex());
    request.set_serialized_runtime_env(serialized_runtime_env);
    request.mutable_runtime_env_config()->CopyFrom(runtime_env_config);
    std::string payload = request.SerializeAsString();

    auto session = Session::Create(
        io_context_,
        address_,
        port_str_,
        http::verb::post,
        HTTP_PATH_GET_OR_CREATE_RUNTIME_ENV,
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
    session_pool_.enqueue(std::move(session));
  }

  // Making HTTP call.
  // POST /delete_runtime_env_if_possible
  // Body = proto rpc::DeleteRuntimeEnvIfPossibleRequest
  void DeleteRuntimeEnvIfPossible(const std::string &serialized_runtime_env,
                                  DeleteRuntimeEnvIfPossibleCallback callback) override {
    RetryInvokeOnNotFoundWithDeadline<rpc::DeleteRuntimeEnvIfPossibleReply>(
        [=](SuccCallback<rpc::DeleteRuntimeEnvIfPossibleReply> succ_callback,
            FailCallback fail_callback) {
          return TryDeleteRuntimeEnvIfPossible(
              serialized_runtime_env, std::move(succ_callback), std::move(fail_callback));
        },
        /*succ_callback=*/
        [=](rpc::DeleteRuntimeEnvIfPossibleReply reply) {
          if (reply.status() != rpc::AGENT_RPC_STATUS_OK) {
            // HTTP request & protobuf parsing succeeded, but we got a non-OK from the
            // remote server.
            // TODO(sang): Find a better way to delivering error messages in this
            RAY_LOG(WARNING) << "Failed to delete runtime env"
                             << ", error message: " << reply.error_message();
            RAY_LOG(DEBUG) << "Serialized runtime env: " << serialized_runtime_env;
            callback(false);
          } else {
            callback(true);
          }
        },
        /*fail_callback=*/
        [=](ray::Status status) {
          RAY_LOG(WARNING)
              << "Failed to delete runtime env reference, status = " << status
              << ", maybe there are some network problems, will fail the request.";
          RAY_LOG(DEBUG) << "Serialized runtime env: " << serialized_runtime_env;
          callback(false);
        },
        current_time_ms() + agent_register_timeout_ms_);
  }

  // Invokes `succ_callback` with server reply (which may be OK or application errors),
  // or invokes `fail_callback` on network error or protobuf deserialization error.
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
        http::verb::post,
        HTTP_PATH_DELETE_RUNTIME_ENV_IF_POSSIBLE,
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
    session_pool_.enqueue(std::move(session));
  }

 private:
  boost::asio::io_context &io_context_;
  SessionPool session_pool_;

  const std::string address_;
  const std::string port_str_;
  std::function<std::shared_ptr<boost::asio::deadline_timer>(std::function<void()>,
                                                             uint32_t delay_ms)>
      delay_executor_;
  std::function<void(const rpc::NodeDeathInfo &)> shutdown_raylet_gracefully_;
  const uint32_t agent_register_timeout_ms_;
  const uint32_t agent_manager_retry_interval_ms_;
};
}  // namespace

std::unique_ptr<RuntimeEnvAgentClient> RuntimeEnvAgentClient::Create(
    instrumented_io_context &io_context,
    const std::string &address,
    int port,
    std::function<std::shared_ptr<boost::asio::deadline_timer>(
        std::function<void()>, uint32_t delay_ms)> delay_executor,
    std::function<void(const rpc::NodeDeathInfo &)> shutdown_raylet_gracefully,
    uint32_t agent_register_timeout_ms,
    uint32_t agent_manager_retry_interval_ms) {
  return std::make_unique<HttpRuntimeEnvAgentClient>(io_context,
                                                     address,
                                                     port,
                                                     delay_executor,
                                                     shutdown_raylet_gracefully,
                                                     agent_register_timeout_ms,
                                                     agent_manager_retry_interval_ms);
}

}  // namespace raylet
}  // namespace ray
