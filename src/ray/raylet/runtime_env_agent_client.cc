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
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/strings/str_format.h"
#include "ray/asio/instrumented_io_context.h"
#include "ray/common/status.h"
#include "ray/rpc/authentication/authentication_token_loader.h"
#include "ray/util/clock.h"
#include "ray/util/logging.h"
#include "ray/util/process_utils.h"
#include "src/ray/protobuf/runtime_env_agent.pb.h"

namespace beast = boost::beast;  // from <boost/beast.hpp>
namespace http = beast::http;    // from <boost/beast/http.hpp>
namespace net = boost::asio;     // from <boost/asio.hpp>
using tcp = net::ip::tcp;        // from <boost/asio/ip/tcp.hpp>

namespace ray {
namespace raylet {

namespace {

// One HTTP call: what to send, and where to deliver the outcome.
// Exactly one of `succ_callback` and `fail_callback` is invoked.
struct Request {
  http::verb method;
  std::string target;
  std::string body;
  std::function<void(std::string)> succ_callback;
  std::function<void(ray::Status)> fail_callback;
};

//------------------------------------------------------------------------------
// A persistent HTTP/1.1 connection to the runtime env agent. Requests are sent one at a
// time; the socket is kept open between them so the raylet does not burn an ephemeral
// port per call (a port stays in TIME_WAIT for 60s after close, so per-call connections
// exhaust the ephemeral range under load).
//
// Hard coded behavior:
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
// A request that fails on a reused socket before any response byte arrives is retried
// once on a fresh socket without surfacing an error: the peer closes idle connections
// (aiohttp's keepalive_timeout) but never mid-request, so the server has provably not
// processed it. Requests are not idempotent on the agent, which reference counts each
// GetOrCreateRuntimeEnv, so this is only safe under that condition.
//
// Spirit from
// https://www.boost.org/doc/libs/develop/libs/beast/example/http/client/async/http_client_async.cpp
class Connection : public std::enable_shared_from_this<Connection> {
 public:
  // `reusable` is false if the socket is no longer usable, i.e. the caller must discard
  // this connection.
  using FinishedCallback =
      std::function<void(std::shared_ptr<Connection>, bool reusable)>;

  // Factory method.
  // Not exposing ctor because it's expected to always be in a shared_ptr.
  static std::shared_ptr<Connection> Create(net::io_context &ioc,
                                            std::string_view host,
                                            std::string_view port) {
    // C++ limitations: make_shared can't be used because std::shared_ptr can't invoke
    // private ctor.
    return std::shared_ptr<Connection>(new Connection(ioc, host, port));
  }

  // Sends `request` asynchrounously. Immediately returns. Connects first if the socket is
  // not already open.
  //
  // It's ok to release a shared_ptr to `this` because the io context will hold a
  // shared_ptr that holds a reference to `this`.
  //
  // Must not be called while another request is in flight.
  void Execute(Request request, FinishedCallback finished_callback) {
    request_ = std::move(request);
    finished_callback_ = std::move(finished_callback);
    // Only a socket that already survived one request may be silently retried.
    may_retry_ = connected_;

    req_ = {};
    req_.method(request_.method);
    req_.target(request_.target);
    req_.body() = std::move(request_.body);
    req_.version(11);  // HTTP/1.1
    req_.set(http::field::host, host_);
    req_.set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);
    req_.set(http::field::content_type, "application/octet-stream");
    // Sets Content-Length header.
    req_.prepare_payload();

    // Read per request rather than per connection so a rotated token is picked up.
    auto auth_token = rpc::AuthenticationTokenLoader::instance().GetToken();
    if (auth_token && !auth_token->empty()) {
      req_.set(http::field::authorization, auth_token->ToAuthorizationHeaderValue());
    }

    if (connected_) {
      Write();
    } else {
      Resolve();
    }
  }

 private:
  explicit Connection(net::io_context &ioc, std::string_view host, std::string_view port)
      : resolver_(ioc), stream_(ioc), host_(std::string(host)), port_(std::string(port)) {
    stream_.expires_never();
  }

  void Resolve() {
    resolver_.async_resolve(
        host_,
        port_,
        beast::bind_front_handler(&Connection::on_resolve, shared_from_this()));
  }

  void Write() {
    buffer_.clear();
    res_ = {};
    stream_.expires_never();
    http::async_write(
        stream_,
        req_,
        beast::bind_front_handler(&Connection::on_write, shared_from_this()));
  }

  void Close() {
    connected_ = false;
    beast::error_code ec;
    stream_.socket().shutdown(tcp::socket::shutdown_both, ec);
    // not_connected happens sometimes so don't bother reporting it.
    if (ec && ec != beast::errc::not_connected) {
      RAY_LOG(INFO) << "error shutting down runtime env agent connection: "
                    << ec.message();
    }
    stream_.socket().close(ec);
  }

  // Retries once on a fresh socket if the failure can only mean the server never read the
  // request; otherwise reports it.
  void FailedMidRequest(ray::Status status) {
    if (!may_retry_) {
      Failed(std::move(status));
      return;
    }
    may_retry_ = false;
    RAY_LOG(DEBUG) << "Runtime env agent closed an idle connection, reconnecting: "
                   << status;
    Close();
    Resolve();
  }

  // Releases this connection back to its pool, then delivers the outcome. Releasing
  // first lets a callback that issues the next request reuse this very connection
  // instead of opening another one.
  void Finish(bool reusable, std::function<void()> deliver_outcome) {
    auto self = shared_from_this();
    auto finished = std::move(finished_callback_);
    finished(self, reusable);
    deliver_outcome();
  }

  void Failed(ray::Status status) {
    Close();
    Finish(/*reusable=*/false,
           [fail_callback = std::move(request_.fail_callback),
            status = std::move(status)]() mutable { fail_callback(std::move(status)); });
  }

  void Succeeded(std::string body) {
    Finish(connected_,
           [succ_callback = std::move(request_.succ_callback),
            body = std::move(body)]() mutable { succ_callback(std::move(body)); });
  }

  void on_resolve(beast::error_code ec, tcp::resolver::results_type results) {
    if (ec) {
      Failed(ray::Status::NotFound("on_resolve " + ec.message()));
      return;
    }

    stream_.expires_never();
    // Make the connection on the IP address we get from a lookup
    stream_.async_connect(
        results, beast::bind_front_handler(&Connection::on_connect, shared_from_this()));
  }

  void on_connect(beast::error_code ec, tcp::resolver::results_type::endpoint_type) {
    if (ec) {
      Failed(ray::Status::NotFound(absl::StrCat("on_connect ", ec.message())));
      return;
    }

    connected_ = true;
    // Send the HTTP request to the remote host
    Write();
  }

  void on_write(beast::error_code ec, std::size_t bytes_transferred) {
    if (ec) {
      // A partially written request is never processed by the agent, so retrying is safe
      // regardless of how many bytes went out.
      FailedMidRequest(ray::Status::Disconnected(absl::StrCat(
          "on_write ", ec.message(), ", bytes_transferred ", bytes_transferred)));
      return;
    }
    stream_.expires_never();
    // Receive the HTTP response
    http::async_read(stream_,
                     buffer_,
                     res_,
                     beast::bind_front_handler(&Connection::on_read, shared_from_this()));
  }

  void on_read(beast::error_code ec, std::size_t bytes_transferred) {
    if (ec) {
      auto status = ray::Status::Disconnected(absl::StrCat(
          "on_read ", ec.message(), ", bytes_transferred ", bytes_transferred));
      if (bytes_transferred == 0) {
        FailedMidRequest(std::move(status));
      } else {
        Failed(std::move(status));
      }
      return;
    }

    // Decide the socket's fate before handing control back, because the callbacks may
    // start the next request on this connection.
    if (!res_.keep_alive()) {
      Close();
    }

    if (http::to_status_class(res_.result()) == http::status_class::successful) {
      Succeeded(std::move(res_).body());
    } else {
      Failed(ray::Status::IOError(absl::StrCat("HTTP request returns non-ok status code ",
                                               res_.result_int(),
                                               ", body",
                                               std::move(res_).body())));
    }
  }

  tcp::resolver resolver_;
  beast::tcp_stream stream_;
  std::string host_;
  std::string port_;
  bool connected_ = false;
  bool may_retry_ = false;
  Request request_;
  beast::flat_buffer buffer_;  // (Must persist between reads)
  http::request<http::string_body> req_;
  http::response<http::string_body> res_;
  FinishedCallback finished_callback_;
};

// A pool of persistent connections with a fixed max concurrency. Each connection handles
// 1 concurrent request; requests beyond the max are queued.
//
// Steady state costs exactly `max_concurrency` ephemeral ports no matter the request
// rate. Idle connections are left open and are reaped by the agent, so the raylet never
// closes first and never accumulates TIME_WAIT sockets.
//
// NOT thread safe: if the methods or the fallbacks are invoked in different threads, the
// workloads may be lost or the running order may not be fair.
class ConnectionPool {
 public:
  ConnectionPool(net::io_context &ioc,
                 std::string_view host,
                 std::string_view port,
                 size_t max_concurrency)
      : ioc_(ioc),
        host_(std::string(host)),
        port_(std::string(port)),
        max_concurrency_(max_concurrency) {}

  void enqueue(Request request) {
    if (!idle_connections_.empty()) {
      auto connection = std::move(idle_connections_.back());
      idle_connections_.pop_back();
      dispatch(std::move(connection), std::move(request));
    } else if (connections_.size() < max_concurrency_) {
      dispatch(create_connection(), std::move(request));
    } else {
      pending_requests_.emplace(std::move(request));
    }
  }

 private:
  std::shared_ptr<Connection> create_connection() {
    auto connection = Connection::Create(ioc_, host_, port_);
    connections_.insert(connection);
    return connection;
  }

  void dispatch(std::shared_ptr<Connection> connection, Request request) {
    connection->Execute(std::move(request),
                        [this](std::shared_ptr<Connection> finished, bool reusable) {
                          this->on_finished(std::move(finished), reusable);
                        });
  }

  // After a request completes we have 1 free slot, so we can start a pending request.
  void on_finished(std::shared_ptr<Connection> connection, bool reusable) {
    if (!reusable) {
      connections_.erase(connection);
      if (!pending_requests_.empty()) {
        auto request = std::move(pending_requests_.front());
        pending_requests_.pop();
        dispatch(create_connection(), std::move(request));
      }
      return;
    }
    if (pending_requests_.empty()) {
      idle_connections_.push_back(std::move(connection));
      return;
    }
    auto request = std::move(pending_requests_.front());
    pending_requests_.pop();
    dispatch(std::move(connection), std::move(request));
  }

  net::io_context &ioc_;
  const std::string host_;
  const std::string port_;
  const size_t max_concurrency_;
  absl::flat_hash_set<std::shared_ptr<Connection>> connections_;
  std::vector<std::shared_ptr<Connection>> idle_connections_;
  std::queue<Request> pending_requests_;
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
      ClockInterface &clock,
      uint32_t agent_register_timeout_ms,
      uint32_t agent_manager_retry_interval_ms,
      uint32_t session_pool_size = 10)
      : connection_pool_(
            io_context, address, absl::StrFormat("%d", port), session_pool_size),
        address_(address),
        port_str_(absl::StrFormat("%d", port)),
        delay_executor_(delay_executor),
        shutdown_raylet_gracefully_(shutdown_raylet_gracefully),
        clock_(clock),
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
      } else if (clock_.SteadyNowMillis() > deadline_ms) {
        RAY_LOG(ERROR) << "Runtime Env Agent timed out in " << agent_register_timeout_ms_
                       << "ms. Status: " << status << ", address: " << this->address_
                       << ", port: " << this->port_str_ << ", exiting immediately...";
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
        clock_.SteadyNowMillis() + agent_register_timeout_ms_);
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

    connection_pool_.enqueue(Request{http::verb::post,
                                     std::string(HTTP_PATH_GET_OR_CREATE_RUNTIME_ENV),
                                     std::move(payload),
                                     /*succ_callback=*/
                                     [succ_callback, fail_callback](std::string body) {
                                       rpc::GetOrCreateRuntimeEnvReply reply;
                                       if (!reply.ParseFromString(body)) {
                                         fail_callback(
                                             Status::IOError("protobuf parse error"));
                                       } else {
                                         succ_callback(std::move(reply));
                                       }
                                     },
                                     fail_callback});
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
        clock_.SteadyNowMillis() + agent_register_timeout_ms_);
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

    connection_pool_.enqueue(
        Request{http::verb::post,
                std::string(HTTP_PATH_DELETE_RUNTIME_ENV_IF_POSSIBLE),
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
                fail_callback});
  }

 private:
  ConnectionPool connection_pool_;

  const std::string address_;
  const std::string port_str_;
  std::function<std::shared_ptr<boost::asio::deadline_timer>(std::function<void()>,
                                                             uint32_t delay_ms)>
      delay_executor_;
  std::function<void(const rpc::NodeDeathInfo &)> shutdown_raylet_gracefully_;
  ClockInterface &clock_;
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
    ClockInterface &clock,
    uint32_t agent_register_timeout_ms,
    uint32_t agent_manager_retry_interval_ms) {
  return std::make_unique<HttpRuntimeEnvAgentClient>(io_context,
                                                     address,
                                                     port,
                                                     delay_executor,
                                                     shutdown_raylet_gracefully,
                                                     clock,
                                                     agent_register_timeout_ms,
                                                     agent_manager_retry_interval_ms);
}

}  // namespace raylet
}  // namespace ray
