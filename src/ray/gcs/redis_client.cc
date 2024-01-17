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

#include "ray/gcs/redis_client.h"

#include "ray/common/ray_config.h"
#include "ray/gcs/redis_context.h"

extern "C" {
#include "hiredis/hiredis.h"
}

namespace ray {

namespace gcs {

/// Run redis command using specified context and store the result in `reply`. Return true
/// if the number of attemps didn't reach `redis_db_connect_retries`.
static bool RunRedisCommandWithRetries(
    redisContext *context,
    const char *command,
    redisReply **reply,
    const std::function<bool(const redisReply *)> &condition) {
  int num_attempts = 0;
  while (num_attempts < RayConfig::instance().redis_db_connect_retries()) {
    // Try to execute the command.
    *reply = reinterpret_cast<redisReply *>(redisCommand(context, command));
    if (condition(*reply)) {
      break;
    }

    // Sleep for a little, and try again if the entry isn't there yet.
    freeReplyObject(*reply);
    std::this_thread::sleep_for(std::chrono::milliseconds(
        RayConfig::instance().redis_db_connect_wait_milliseconds()));
    num_attempts++;
  }
  return num_attempts < RayConfig::instance().redis_db_connect_retries();
}

static int DoGetNextJobID(redisContext *context) {
  // This is bad since duplicate logic lives in redis_client
  // and redis_store_client.
  // A refactoring is needed to make things clean.
  // src/ray/gcs/store_client/redis_store_client.cc#L42
  // TODO (iycheng): Unify the way redis key is formated.
  static const std::string kTableSeparator = ":";
  static const std::string kClusterSeparator = "@";
  static std::string key = RayConfig::instance().external_storage_namespace() +
                           kClusterSeparator + kTableSeparator + "JobCounter";
  static std::string cmd =
      "HINCRBY " + RayConfig::instance().external_storage_namespace() + " " + key + " 1";

  redisReply *reply = nullptr;
  bool under_retry_limit = RunRedisCommandWithRetries(
      context, cmd.c_str(), &reply, [](const redisReply *reply) {
        if (reply == nullptr) {
          RAY_LOG(WARNING) << "Didn't get reply for " << cmd;
          return false;
        }
        if (reply->type == REDIS_REPLY_NIL) {
          RAY_LOG(WARNING) << "Got nil reply for " << cmd;
          return false;
        }
        if (reply->type == REDIS_REPLY_ERROR) {
          RAY_LOG(WARNING) << "Got error reply for " << cmd << " Error is " << reply->str;
          return false;
        }
        return true;
      });
  RAY_CHECK(reply);
  RAY_CHECK(under_retry_limit) << "No entry found for JobCounter";
  RAY_CHECK(reply->type == REDIS_REPLY_INTEGER)
      << "Expected integer, found Redis type " << reply->type << " for JobCounter";
  int counter = reply->integer;
  freeReplyObject(reply);
  return counter;
}

RedisClient::RedisClient(const RedisClientOptions &options) : options_(options) {}

Status RedisClient::Connect(instrumented_io_context &io_service) {
  RAY_CHECK(!is_connected_);

  if (options_.server_ip_.empty()) {
    RAY_LOG(ERROR) << "Failed to connect, redis server address is empty.";
    return Status::Invalid("Redis server address is invalid!");
  }

  primary_context_ = std::make_shared<RedisContext>(io_service);

  RAY_CHECK_OK(primary_context_->Connect(options_.server_ip_,
                                         options_.server_port_,
                                         /*password=*/options_.password_,
                                         /*enable_ssl=*/options_.enable_ssl_));

  Attach();

  is_connected_ = true;
  RAY_LOG(DEBUG) << "RedisClient connected.";

  return Status::OK();
}

void RedisClient::Attach() {
  // Take care of sharding contexts.
  RAY_CHECK(!asio_async_auxiliary_client_) << "Attach shall be called only once";
  instrumented_io_context &io_service = primary_context_->io_service();
  asio_async_auxiliary_client_.reset(
      new RedisAsioClient(io_service, primary_context_->async_context()));
}

void RedisClient::Disconnect() {
  RAY_CHECK(is_connected_);
  is_connected_ = false;
  RAY_LOG(DEBUG) << "RedisClient disconnected.";
}

int RedisClient::GetNextJobID() {
  RAY_CHECK(primary_context_);
  return DoGetNextJobID(primary_context_->sync_context());
}

}  // namespace gcs

}  // namespace ray
