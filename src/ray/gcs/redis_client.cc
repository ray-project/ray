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
  redisReply *reply = nullptr;
  bool under_retry_limit = RunRedisCommandWithRetries(
      context, "INCR JobCounter", &reply, [](const redisReply *reply) {
        return reply != nullptr && reply->type != REDIS_REPLY_NIL;
      });
  RAY_CHECK(reply);
  RAY_CHECK(under_retry_limit) << "No entry found for JobCounter";
  RAY_CHECK(reply->type == REDIS_REPLY_INTEGER)
      << "Expected integer, found Redis type " << reply->type << " for JobCounter";
  int counter = reply->integer;
  freeReplyObject(reply);
  return counter;
}

static void GetRedisShards(redisContext *context,
                           std::vector<std::string> *addresses,
                           std::vector<int> *ports) {
  // Get the total number of Redis shards in the system.
  redisReply *reply = nullptr;
  bool under_retry_limit = RunRedisCommandWithRetries(
      context, "GET NumRedisShards", &reply, [](const redisReply *reply) {
        return reply != nullptr && reply->type != REDIS_REPLY_NIL;
      });
  RAY_CHECK(under_retry_limit) << "No entry found for NumRedisShards";
  RAY_CHECK(reply->type == REDIS_REPLY_STRING)
      << "Expected string, found Redis type " << reply->type << " for NumRedisShards";
  int num_redis_shards = atoi(reply->str);
  RAY_CHECK(num_redis_shards >= 1) << "Expected at least one Redis shard, "
                                   << "found " << num_redis_shards;
  freeReplyObject(reply);

  // Get the addresses of all of the Redis shards.
  under_retry_limit = RunRedisCommandWithRetries(
      context,
      "LRANGE RedisShards 0 -1",
      &reply,
      [&num_redis_shards](const redisReply *reply) {
        return static_cast<int>(reply->elements) == num_redis_shards;
      });
  RAY_CHECK(under_retry_limit) << "Expected " << num_redis_shards
                               << " Redis shard addresses, found " << reply->elements;

  // Parse the Redis shard addresses.
  for (size_t i = 0; i < reply->elements; ++i) {
    // Parse the shard addresses and ports.
    RAY_CHECK(reply->element[i]->type == REDIS_REPLY_STRING);
    std::string addr;
    std::stringstream ss(reply->element[i]->str);
    getline(ss, addr, ':');
    addresses->emplace_back(std::move(addr));
    int port;
    ss >> port;
    ports->emplace_back(port);
    RAY_LOG(DEBUG) << "Received Redis shard address " << addr << ":" << port
                   << " from head GCS.";
  }
  freeReplyObject(reply);
}

RedisClient::RedisClient(const RedisClientOptions &options) : options_(options) {}

Status RedisClient::Connect(instrumented_io_context &io_service) {
  std::vector<instrumented_io_context *> io_services;
  io_services.emplace_back(&io_service);
  return Connect(io_services);
}

Status RedisClient::Connect(std::vector<instrumented_io_context *> io_services) {
  RAY_CHECK(!is_connected_);
  RAY_CHECK(!io_services.empty());

  if (options_.server_ip_.empty()) {
    RAY_LOG(ERROR) << "Failed to connect, redis server address is empty.";
    return Status::Invalid("Redis server address is invalid!");
  }

  primary_context_ = std::make_shared<RedisContext>(*io_services[0]);

  RAY_CHECK_OK(primary_context_->Connect(options_.server_ip_,
                                         options_.server_port_,
                                         /*sharding=*/true,
                                         /*password=*/options_.password_,
                                         options_.enable_sync_conn_,
                                         options_.enable_async_conn_,
                                         options_.enable_subscribe_conn_));

  if (options_.enable_sharding_conn_) {
    // Moving sharding into constructor defaultly means that sharding = true.
    // This design decision may worth a look.
    std::vector<std::string> addresses;
    std::vector<int> ports;
    GetRedisShards(primary_context_->sync_context(), &addresses, &ports);
    if (addresses.empty()) {
      RAY_CHECK(ports.empty());
      addresses.push_back(options_.server_ip_);
      ports.push_back(options_.server_port_);
    }

    for (size_t i = 0; i < addresses.size(); ++i) {
      size_t io_service_index = (i + 1) % io_services.size();
      instrumented_io_context &io_service = *io_services[io_service_index];
      // Populate shard_contexts.
      shard_contexts_.push_back(std::make_shared<RedisContext>(io_service));
      // Only async context is used in sharding context, so wen disable the other two.
      RAY_CHECK_OK(shard_contexts_[i]->Connect(addresses[i],
                                               ports[i],
                                               /*sharding=*/true,
                                               /*password=*/options_.password_,
                                               /*enable_sync_conn=*/false,
                                               /*enable_async_conn=*/true,
                                               /*enable_subscribe_conn=*/false));
    }
  } else {
    shard_contexts_.push_back(std::make_shared<RedisContext>(*io_services[0]));
    // Only async context is used in sharding context, so wen disable the other two.
    RAY_CHECK_OK(shard_contexts_[0]->Connect(options_.server_ip_,
                                             options_.server_port_,
                                             /*sharding=*/true,
                                             /*password=*/options_.password_,
                                             /*enable_sync_conn=*/false,
                                             /*enable_async_conn=*/true,
                                             /*enable_subscribe_conn=*/false));
  }

  Attach();

  is_connected_ = true;
  RAY_LOG(DEBUG) << "RedisClient connected.";

  return Status::OK();
}

void RedisClient::Attach() {
  // Take care of sharding contexts.
  RAY_CHECK(shard_asio_async_clients_.empty()) << "Attach shall be called only once";
  for (std::shared_ptr<RedisContext> context : shard_contexts_) {
    instrumented_io_context &io_service = context->io_service();
    shard_asio_async_clients_.emplace_back(
        new RedisAsioClient(io_service, context->async_context()));
  }

  instrumented_io_context &io_service = primary_context_->io_service();
  if (options_.enable_async_conn_) {
    asio_async_auxiliary_client_.reset(
        new RedisAsioClient(io_service, primary_context_->async_context()));
  }
  if (options_.enable_subscribe_conn_) {
    asio_subscribe_auxiliary_client_.reset(
        new RedisAsioClient(io_service, primary_context_->subscribe_context()));
  }
}

void RedisClient::Disconnect() {
  RAY_CHECK(is_connected_);
  is_connected_ = false;
  RAY_LOG(DEBUG) << "RedisClient disconnected.";
}

std::shared_ptr<RedisContext> RedisClient::GetShardContext(const std::string &shard_key) {
  RAY_CHECK(!shard_contexts_.empty());
  static std::hash<std::string> hash;
  size_t index = hash(shard_key) % shard_contexts_.size();
  return shard_contexts_[index];
}

int RedisClient::GetNextJobID() {
  RAY_CHECK(primary_context_);
  return DoGetNextJobID(primary_context_->sync_context());
}

}  // namespace gcs

}  // namespace ray
