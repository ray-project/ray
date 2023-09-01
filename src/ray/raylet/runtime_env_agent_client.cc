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
#include <chrono>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <queue>
#include <string>

#include "absl/strings/str_format.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/status.h"
#include "ray/raylet/raylet_util.h"
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
// Will call exactly once, either on succ_callback or fail_callback.
//
// Hard coded behavior:
// - retries forever.
// - content type is "application/octet-stream".
// - connection has no timeout (i.e. waits forever. This is because runtime env agent can
// work for a long time.)
// - on_read and on_write failures return IOError.
// - if a connection failure happens, reconnects and retries indefinitely.
//
// Requests are in a FIFO queue.
//
// States:
// - NOT_CONNECTED (idle, pending_resolve, pending_connect),
// - CONNECTED (idle),
// - REQUEST_SENT (pending_write, pending_read),
// - DEAD.
//
// Each sub-state has a corrsponding handler to transition to a next state.
//
// Note DEAD can only come from NOT_CONNECTED (idle) when it passes connection_timeout_ms.
//
// Public API:
//
// Create(): creates a session. automatically starts connecting.
// post_request(): adds a request to the queue. Invokes a callback on completion.
// Infinitely rerties on connection error. Returns IOError on read and write errors.
//
// TODO: make it possible to stop the loop
//
class Session : public std::enable_shared_from_this<Session> {
 public:
  // Factory method.
  // Not exposing ctor because it's expected to always be in a shared_ptr.
  //
  // connection_timeout_ms: if connection fails this long, invokes death_callback.
  // connection_retry_interval_ms: if connection fails, wait this much and retry.
  // death_callback: invoked when `connection_timeout_ms` is timed out.
  static std::shared_ptr<Session> Create(net::io_context &ioc,
                                         std::string_view host,
                                         std::string_view port,
                                         uint32_t connection_timeout_ms,
                                         uint32_t connection_retry_interval_ms,
                                         std::function<void()> death_callback) {
    // C++ limitations: make_shared can't be used because std::shared_ptr can't invoke
    // private ctor.
    return std::shared_ptr<Session>(new Session(ioc,
                                                host,
                                                port,
                                                connection_timeout_ms,
                                                connection_retry_interval_ms,
                                                death_callback));
  }

  void post_request(std::string_view target,
                    std::string body,
                    std::function<void(http::response<http::string_body>)> succ_callback,
                    std::function<void(ray::Status)> fail_callback) {
    auto work = std::make_unique<Work>();
    work->request.method(http::verb::post);
    work->request.target(target);
    work->request.body() = std::move(body);
    work->request.keep_alive(true);
    work->request.set(http::field::host, host_);
    work->request.set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);
    work->request.set(http::field::content_type, "application/octet-stream");
    // aiohttp has a bug that, if you don't set this value, it returns 400.
    // https://github.com/aio-libs/aiohttp/issues/7208
    work->request.content_length(work->request.body().size());

    work->succ_callback = succ_callback;
    work->fail_callback = fail_callback;

    // asio completion handlers have to be copy constructible [1]. This means we cannot
    // directly move our unique ptr into the lambda. To circumvent this let's pass raw
    // pointers across the boundary.
    // [1]
    // https://stackoverflow.com/questions/37709819/why-must-a-boost-asio-handler-be-copy-constructible
    Work *raw = work.release();
    strand_.post([this, raw]() {
      std::unique_ptr<Work> w(raw);
      if (state_ == State::kDead) {
        RAY_LOG(WARNING) << "not queueing request, session already dead.";
        w->fail_callback(ray::Status::IOError("session already dead"));
        return;
      }
      queue_.push(std::move(w));
      if (state_ == State::kConnected) {
        do_work();
      }
    });
  }

  // State Change handlers
 private:
  explicit Session(net::io_context &ioc,
                   std::string_view host,
                   std::string_view port,
                   uint32_t connection_timeout_ms,
                   uint32_t connection_retry_interval_ms,
                   std::function<void()> death_callback)
      : strand_(ioc),
        resolver_(ioc),
        stream_(ioc),
        reconnect_timer_(ioc),
        host_(std::string(host)),
        port_(std::string(port)),
        connection_timeout_ms_(connection_timeout_ms),
        connection_retry_interval_ms_(connection_retry_interval_ms),
        death_callback_(death_callback),
        connect_attempt_start_time_(std::nullopt) {
    stream_.expires_never();
    strand_.post([this]() { connect(); });
  }

  // kNotConnected (idle) -> (pending_resolve)
  void connect() {
    RAY_CHECK(state_ == State::kNotConnected);
    if (connect_attempt_start_time_ == std::nullopt) {
      connect_attempt_start_time_ = std::chrono::steady_clock::now();
    } else if (std::chrono::steady_clock::now() - connect_attempt_start_time_.value() >
               std::chrono::milliseconds(connection_timeout_ms_)) {
      state_ = State::kDead;
      on_dead();
      return;
    }

    RAY_LOG(INFO) << "connecting to " << host_ << ", port " << port_;
    resolver_.async_resolve(
        host_,
        port_,
        beast::bind_front_handler(&Session::on_resolve, shared_from_this()));
  }

  // Will only be called once on the edge of state -> kNotConnected (idle) | kDead
  void reconnect() {
    RAY_CHECK(state_ == State::kNotConnected);

    RAY_LOG(INFO) << "Cannot connect to " << host_ << ", port " << port_
                  << ", scheduling a retry in " << connection_retry_interval_ms_
                  << "ms...";

    reconnect_timer_.expires_from_now(
        boost::posix_time::milliseconds(connection_retry_interval_ms_));
    reconnect_timer_.async_wait([this](const boost::system::error_code &ec) {
      RAY_CHECK(!ec);  // we don't abort.
      connect();
    });
  }

  // fails all requests, and invoke death callback.
  void on_dead() {
    RAY_CHECK(state_ == State::kDead);
    RAY_LOG(WARNING) << "session is dead, failing all pending requests...";
    while (!queue_.empty()) {
      queue_.front()->fail_callback(ray::Status::IOError("session already dead"));
      queue_.pop();
    }
    RAY_LOG(WARNING) << "session is dead, calling death_callback...";
    death_callback_();
  }

  // (pending_resolve) -> (pending_connect) | kNotConnected
  void on_resolve(beast::error_code ec, tcp::resolver::results_type results) {
    RAY_CHECK(state_ == State::kNotConnected);
    if (ec) {
      RAY_LOG(INFO) << "failed to resolve " << host_ << ", port " << port_ << ": "
                    << ec.what();
      reconnect();
      return;
    }

    // Make the connection on the IP address we get from a lookup
    stream_.async_connect(
        results, beast::bind_front_handler(&Session::on_connect, shared_from_this()));
  }

  // (pending_connect) -> kConnected | kNotConnected
  void on_connect(beast::error_code ec, tcp::resolver::results_type::endpoint_type) {
    RAY_CHECK(state_ == State::kNotConnected);

    if (ec) {
      RAY_LOG(INFO) << "failed to connect " << host_ << ", port " << port_ << ": "
                    << ec.what();
      reconnect();
      return;
    }

    state_ = State::kConnected;
    connect_attempt_start_time_ = std::nullopt;

    if (!queue_.empty()) {
      do_work();
    }
  }

  // kConnected -> kConnected (pending write) | kRequestSent
  // prerequesite: queue is not empty.
  void do_work() {
    RAY_CHECK(state_ == State::kConnected);

    RAY_CHECK(!queue_.empty());
    RAY_CHECK(!current_work_);
    current_work_ = std::move(queue_.front());
    queue_.pop();

    // Send the HTTP request to the remote host
    http::async_write(stream_,
                      current_work_->request,
                      beast::bind_front_handler(&Session::on_write, shared_from_this()));
    state_ = State::kRequestSent;
  }

  // (pending write) ->
  //  kNotConnected (fail), failing the active callback |
  // kRequestSent (pending read)
  void on_write(beast::error_code ec, std::size_t bytes_transferred) {
    boost::ignore_unused(bytes_transferred);
    RAY_CHECK(state_ == State::kRequestSent);

    if (ec) {
      current_work_->fail_callback(ray::Status::IOError("on_write " + ec.what()));
      current_work_ = nullptr;
      state_ = State::kNotConnected;

      RAY_LOG(INFO) << "failed to write: " << ec.what();
      reconnect();
      return;
    }

    buffer_.clear();
    resp_.clear();

    // Receive the HTTP response
    http::async_read(stream_,
                     buffer_,
                     resp_,
                     beast::bind_front_handler(&Session::on_read, shared_from_this()));
  }

  // (pending read) ->
  // kNotConnected (fail), failing the active callback |
  // kConnected, finishing active callback
  void on_read(beast::error_code ec, std::size_t bytes_transferred) {
    boost::ignore_unused(bytes_transferred);
    RAY_CHECK(state_ == State::kRequestSent);

    if (ec) {
      current_work_->fail_callback(ray::Status::IOError("on_read " + ec.what()));
      current_work_ = nullptr;
      state_ = State::kNotConnected;

      RAY_LOG(INFO) << "failed to read: " << ec.what();
      reconnect();

      return;
    }
    current_work_->succ_callback(std::move(resp_));
    current_work_ = nullptr;
    state_ = State::kConnected;

    if (!queue_.empty()) {
      do_work();
    }
  }

  net::io_service::strand strand_;
  tcp::resolver resolver_;
  beast::tcp_stream stream_;
  boost::asio::deadline_timer reconnect_timer_;

  std::string host_;
  std::string port_;

  uint32_t connection_timeout_ms_;
  uint32_t connection_retry_interval_ms_;
  std::function<void()> death_callback_;

  std::optional<std::chrono::steady_clock::time_point> connect_attempt_start_time_;
  beast::flat_buffer buffer_;               // Must clear before async_read()
  http::response<http::string_body> resp_;  // Must clear before async_read()

  enum class State { kNotConnected, kConnected, kRequestSent, kDead };
  State state_ = State::kNotConnected;

  struct Work {
    http::request<http::string_body> request;
    std::function<void(http::response<http::string_body>)> succ_callback;
    std::function<void(ray::Status)> fail_callback;
  };

  std::queue<std::unique_ptr<Work>> queue_;
  // Only exists on kRequestSent.
  std::unique_ptr<Work> current_work_;
};

inline constexpr std::string_view HTTP_PATH_GET_OR_CREATE_RUNTIME_ENV =
    "/get_or_create_runtime_env";
inline constexpr std::string_view HTTP_PATH_DELETE_RUNTIME_ENV_IF_POSSIBLE =
    "/delete_runtime_env_if_possible";

class HttpRuntimeEnvAgentClient : public RuntimeEnvAgentClient {
 public:
  HttpRuntimeEnvAgentClient(instrumented_io_context &io_context,
                            const std::string &address,
                            int port,
                            std::function<std::shared_ptr<boost::asio::deadline_timer>(
                                std::function<void()>, uint32_t delay_ms)> delay_executor,
                            uint32_t agent_register_timeout_ms,
                            uint32_t agent_manager_retry_interval_ms)
      :

        session_(Session::Create(io_context,
                                 address,
                                 std::to_string(port),
                                 agent_register_timeout_ms,
                                 agent_manager_retry_interval_ms,
                                 /*death_callback=*/[this]() { this->Suicide(); })),
        delay_executor_(delay_executor) {}
  ~HttpRuntimeEnvAgentClient() = default;

  template <typename T>
  using SuccCallback = std::function<void(T)>;
  using FailCallback = std::function<void(ray::Status)>;
  template <typename T>
  using TryInvokeOnce = std::function<void(SuccCallback<T>, FailCallback)>;

  void Suicide() {
    RAY_LOG(ERROR)
        << "The raylet exited immediately because the runtime env agent timed out when "
           "Raylet try to connect to it. This can happen because the runtime env agent "
           "was never started, or is listening to the wrong port. Read the log `cat "
           "/tmp/ray/session_latest/logs/runtime_env_agent.log`. You can find the log "
           "file structure here "
           "https://docs.ray.io/en/master/ray-observability/"
           "ray-logging.html#logging-directory-structure.\n";
    ShutdownRayletGracefully();
    // If the process is not terminated within 10 seconds, forcefully kill itself.
    delay_executor_([]() { QuickExit(); }, /*ms*/ 10000);
  }

  // Making HTTP call.
  // POST /get_or_create_runtime_env
  // Body = proto rpc::GetOrCreateRuntimeEnvRequest
  void GetOrCreateRuntimeEnv(const JobID &job_id,
                             const std::string &serialized_runtime_env,
                             const rpc::RuntimeEnvConfig &runtime_env_config,
                             const std::string &serialized_allocated_resource_instances,
                             GetOrCreateRuntimeEnvCallback callback) override {
    rpc::GetOrCreateRuntimeEnvRequest request;
    request.set_job_id(job_id.Hex());
    request.set_serialized_runtime_env(serialized_runtime_env);
    request.mutable_runtime_env_config()->CopyFrom(runtime_env_config);
    request.set_serialized_allocated_resource_instances(
        serialized_allocated_resource_instances);
    std::string payload = request.SerializeAsString();

    std::function<void(ray::Status)> fail_callback = [=](ray::Status status) {
      std::string error_message =
          absl::StrCat("Failed to create runtime env for job ",
                       job_id.Hex(),
                       ", status = ",
                       status.ToString(),
                       ", maybe there are some network problems, will fail the request.");
      RAY_LOG(INFO) << error_message;
      RAY_LOG(DEBUG) << "Serialized runtime env for job " << job_id << ": "
                     << serialized_runtime_env;
      callback(false, "", error_message);
    };

    std::function<void(http::response<http::string_body>)> succ_callback =
        [=](http::response<http::string_body> response) {
          rpc::GetOrCreateRuntimeEnvReply reply;
          if (!reply.ParseFromString(std::move(response).body())) {
            fail_callback(Status::IOError("protobuf parse error"));
            return;
          }
          if (reply.status() != rpc::AGENT_RPC_STATUS_OK) {
            RAY_LOG(INFO) << "Failed to create runtime env for job " << job_id
                          << ", error message: " << reply.error_message();
            RAY_LOG(DEBUG) << "Serialized runtime env for job " << job_id << ": "
                           << serialized_runtime_env;
            callback(false,
                     reply.serialized_runtime_env_context(),
                     /*setup_error_message*/ reply.error_message());
            return;
          }
          RAY_LOG(INFO) << "Created runtime env for job " << job_id;
          callback(true,
                   reply.serialized_runtime_env_context(),
                   /*setup_error_message*/ "");
        };

    session_->post_request(HTTP_PATH_GET_OR_CREATE_RUNTIME_ENV,
                           std::move(payload),
                           succ_callback,
                           fail_callback);
  }

  // Making HTTP call.
  // POST /delete_runtime_env_if_possible
  // Body = proto rpc::DeleteRuntimeEnvIfPossibleRequest
  void DeleteRuntimeEnvIfPossible(const std::string &serialized_runtime_env,
                                  DeleteRuntimeEnvIfPossibleCallback callback) override {
    rpc::DeleteRuntimeEnvIfPossibleRequest request;
    request.set_serialized_runtime_env(serialized_runtime_env);
    request.set_source_process("raylet");
    std::string payload = request.SerializeAsString();

    std::function<void(ray::Status)> fail_callback = [=](ray::Status status) {
      RAY_LOG(WARNING)
          << "Failed to delete runtime env reference, status = " << status
          << ", maybe there are some network problems, will fail the request.";
      RAY_LOG(DEBUG) << "Serialized runtime env: " << serialized_runtime_env;
      callback(false);
    };

    std::function<void(http::response<http::string_body>)> succ_callback =
        [=](http::response<http::string_body> response) {
          rpc::DeleteRuntimeEnvIfPossibleReply reply;
          if (!reply.ParseFromString(std::move(response).body())) {
            fail_callback(Status::IOError("protobuf parse error"));
            return;
          }
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
        };

    session_->post_request(HTTP_PATH_DELETE_RUNTIME_ENV_IF_POSSIBLE,
                           std::move(payload),
                           succ_callback,
                           fail_callback);
  }

 private:
  std::shared_ptr<Session> session_;
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
        std::function<void()>, uint32_t delay_ms)> delay_executor,
    uint32_t agent_register_timeout_ms,
    uint32_t agent_manager_retry_interval_ms) {
  return std::make_shared<HttpRuntimeEnvAgentClient>(io_context,
                                                     address,
                                                     port,
                                                     delay_executor,
                                                     agent_register_timeout_ms,
                                                     agent_manager_retry_interval_ms);
}

}  // namespace raylet
}  // namespace ray
