// Copyright 2023 The Ray Authors.
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

#include "ray/common/asio/asio_util.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/redis_context.h"
#include "ray/stats/metric_defs.h"

namespace ray {
namespace gcs {

std::optional<std::pair<std::string, int>> ParseIffMovedError(
    const std::string &error_msg) {
  std::vector<std::string> parts = absl::StrSplit(error_msg, " ");
  if (parts[0] != "MOVED") {
    return std::nullopt;
  }
  RAY_CHECK_EQ(parts.size(), 3u);
  std::vector<std::string> ip_port = absl::StrSplit(parts[2], ":");
  RAY_CHECK_EQ(ip_port.size(), 2u);
  return std::make_pair(ip_port[0], std::stoi(ip_port[1]));
}

template <typename RedisContextType, typename RedisConnectFunctionType>
std::pair<Status, std::unique_ptr<RedisContextType, RedisContextDeleter>>
ConnectWithoutRetries(const std::string &address,
                      int port,
                      const RedisConnectFunctionType &connect_function) {
  // This currently returns the errorMessage in two different ways,
  // as an output parameter and in the Status::RedisError,
  // because we're not sure whether we'll want to change what this returns.
  RedisContextType *newContext = connect_function(address.c_str(), port);
  if (newContext == nullptr || (newContext)->err) {
    std::ostringstream oss;
    if (newContext == nullptr) {
      oss << "Could not allocate Redis context.";
    } else if (newContext->err) {
      oss << "Could not establish connection to Redis " << address << ":" << port
          << " (context.err = " << newContext->err << ").";
    }
    return std::make_pair(Status::RedisError(oss.str()), nullptr);
  }
  return std::make_pair(Status::OK(),
                        std::unique_ptr<RedisContextType, RedisContextDeleter>(
                            newContext, RedisContextDeleter()));
}

template <typename RedisContextType, typename RedisConnectFunctionType>
std::pair<Status, std::unique_ptr<RedisContextType, RedisContextDeleter>>
ConnectWithRetries(const std::string &address,
                   int port,
                   const RedisConnectFunctionType &connect_function) {
  RAY_LOG(INFO) << "Attempting to connect to address " << address << ":" << port << ".";
  int connection_attempts = 0;
  auto resp = ConnectWithoutRetries<RedisContextType>(address, port, connect_function);
  auto status = resp.first;
  while (!status.ok()) {
    if (connection_attempts >= RayConfig::instance().redis_db_connect_retries()) {
      RAY_LOG(FATAL) << RayConfig::instance().redis_db_connect_retries() << " attempts "
                     << "to connect have all failed. Please check whether the"
                     << " redis storage is alive or not. The last error message was: "
                     << status.ToString();
      break;
    }
    RAY_LOG_EVERY_MS(ERROR, 1000)
        << "Failed to connect to Redis due to: " << status.ToString()
        << ". Will retry in "
        << RayConfig::instance().redis_db_connect_wait_milliseconds() << "ms.";

    // Sleep for a little.
    std::this_thread::sleep_for(std::chrono::milliseconds(
        RayConfig::instance().redis_db_connect_wait_milliseconds()));
    resp = ConnectWithoutRetries<RedisContextType>(address, port, connect_function);
    status = resp.first;
    connection_attempts += 1;
  }
  return resp;
}

RedisAsyncContext *CreateAsyncContext(
    std::unique_ptr<redisAsyncContext, RedisContextDeleter> context) {
  return new RedisAsyncContext(std::move(context));
}

template <typename ConnectType = redisAsyncContext>
void RedisResponseFn(struct redisAsyncContext *async_context,
                     void *raw_reply,
                     void *privdata) {
  auto *request_cxt = static_cast<RedisRequestContext *>(privdata);
  auto redis_reply = reinterpret_cast<redisReply *>(raw_reply);
  // Error happened.
  if (redis_reply == nullptr || redis_reply->type == REDIS_REPLY_ERROR) {
    auto error_msg = redis_reply ? redis_reply->str : async_context->errstr;
    RAY_LOG(ERROR) << "Redis request [" << absl::StrJoin(request_cxt->redis_cmds_, " ")
                   << "]"
                   << " failed due to error " << error_msg << ". "
                   << request_cxt->pending_retries_ << " retries left.";

    // Retry the request after a while.
    auto delay = request_cxt->exp_back_off_.Current();

    // First check if we need to redirect on MOVED.
    if (RayConfig::instance().enable_moved_redirect()) {
      if (auto maybe_ip_port =
              ParseIffMovedError(std::string(redis_reply->str, redis_reply->len));
          maybe_ip_port.has_value()) {
        const auto [ip, port] = maybe_ip_port.value();
        auto resp = ConnectWithRetries<ConnectType>(ip.c_str(), port, redisAsyncConnect);
        if (auto st = resp.first; !st.ok()) {
          // We will ultimately return a MOVED error if we fail to reconnect.
          RAY_LOG(ERROR) << "Failed to connect to the new leader " << ip << ":" << port;
        } else {
          request_cxt->redis_context_.reset(CreateAsyncContext(std::move(resp.second)));
          // Set the context in the longer-lived RedisContext object.
          request_cxt->parent_context_.ResetAsyncContext(request_cxt->redis_context_);
          // TODO(vitsai): do we need to Attach
        }
      } else {
        request_cxt->exp_back_off_.Next();
      }
    } else {
      request_cxt->exp_back_off_.Next();
    }

    execute_after(
        request_cxt->io_service_,
        [request_cxt]() { request_cxt->Run(); },
        std::chrono::milliseconds(delay));
  } else {
    auto reply = std::make_shared<CallbackReply>(redis_reply);
    request_cxt->io_service_.post(
        [reply, callback = std::move(request_cxt->callback_)]() {
          if (callback) {
            callback(std::move(reply));
          }
        },
        "RedisRequestContext.Callback");
    auto end_time = absl::Now();
    ray::stats::GcsLatency().Record((end_time - request_cxt->start_time_) /
                                    absl::Milliseconds(1));
    delete request_cxt;
  }
}

}  // namespace gcs
}  // namespace ray