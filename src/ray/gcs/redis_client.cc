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

static void GetRedisShards(redisContext *context, std::vector<std::string> *addresses,
                           std::vector<int> *ports) {
  // Get the total number of Redis shards in the system.
  int num_attempts = 0;
  redisReply *reply = nullptr;
  while (num_attempts < RayConfig::instance().redis_db_connect_retries()) {
    // Try to read the number of Redis shards from the primary shard. If the
    // entry is present, exit.
    reply = reinterpret_cast<redisReply *>(redisCommand(context, "GET NumRedisShards"));
    if (reply->type != REDIS_REPLY_NIL) {
      break;
    }

    // Sleep for a little, and try again if the entry isn't there yet.
    freeReplyObject(reply);
    std::this_thread::sleep_for(std::chrono::milliseconds(
        RayConfig::instance().redis_db_connect_wait_milliseconds()));
    num_attempts++;
  }
  RAY_CHECK(num_attempts < RayConfig::instance().redis_db_connect_retries())
      << "No entry found for NumRedisShards";
  RAY_CHECK(reply->type == REDIS_REPLY_STRING)
      << "Expected string, found Redis type " << reply->type << " for NumRedisShards";
  int num_redis_shards = atoi(reply->str);
  RAY_CHECK(num_redis_shards >= 1) << "Expected at least one Redis shard, "
                                   << "found " << num_redis_shards;
  freeReplyObject(reply);

  // Get the addresses of all of the Redis shards.
  num_attempts = 0;
  while (num_attempts < RayConfig::instance().redis_db_connect_retries()) {
    // Try to read the Redis shard locations from the primary shard. If we find
    // that all of them are present, exit.
    reply =
        reinterpret_cast<redisReply *>(redisCommand(context, "LRANGE RedisShards 0 -1"));
    if (static_cast<int>(reply->elements) == num_redis_shards) {
      break;
    }

    // Sleep for a little, and try again if not all Redis shard addresses have
    // been added yet.
    freeReplyObject(reply);
    std::this_thread::sleep_for(std::chrono::milliseconds(
        RayConfig::instance().redis_db_connect_wait_milliseconds()));
    num_attempts++;
  }
  RAY_CHECK(num_attempts < RayConfig::instance().redis_db_connect_retries())
      << "Expected " << num_redis_shards << " Redis shard addresses, found "
      << reply->elements;

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
  }
  freeReplyObject(reply);
}

RedisClient::RedisClient(const RedisClientOptions &options) : options_(options) {}

Status RedisClient::Connect(boost::asio::io_service &io_service) {
  std::vector<boost::asio::io_service *> io_services;
  io_services.emplace_back(&io_service);
  return Connect(io_services);
}

Status RedisClient::Connect(std::vector<boost::asio::io_service *> io_services) {
  RAY_CHECK(!is_connected_);
  RAY_CHECK(!io_services.empty());

  if (options_.server_ip_.empty()) {
    RAY_LOG(ERROR) << "Failed to connect, redis server address is empty.";
    return Status::Invalid("Redis server address is invalid!");
  }

  primary_context_ = std::make_shared<RedisContext>(*io_services[0]);

  RAY_CHECK_OK(primary_context_->Connect(options_.server_ip_, options_.server_port_,
                                         /*sharding=*/true,
                                         /*password=*/options_.password_));

  if (!options_.is_test_client_) {
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
      boost::asio::io_service &io_service = *io_services[io_service_index];
      // Populate shard_contexts.
      shard_contexts_.push_back(std::make_shared<RedisContext>(io_service));
      RAY_CHECK_OK(shard_contexts_[i]->Connect(addresses[i], ports[i], /*sharding=*/true,
                                               /*password=*/options_.password_));
    }
  } else {
    shard_contexts_.push_back(std::make_shared<RedisContext>(*io_services[0]));
    RAY_CHECK_OK(shard_contexts_[0]->Connect(options_.server_ip_, options_.server_port_,
                                             /*sharding=*/true,
                                             /*password=*/options_.password_));
  }

  Attach();

  is_connected_ = true;
  RAY_LOG(INFO) << "RedisClient connected.";

  return Status::OK();
}

void RedisClient::Attach() {
  // Take care of sharding contexts.
  RAY_CHECK(shard_asio_async_clients_.empty()) << "Attach shall be called only once";
  for (std::shared_ptr<RedisContext> context : shard_contexts_) {
    boost::asio::io_service &io_service = context->io_service();
    shard_asio_async_clients_.emplace_back(
        new RedisAsioClient(io_service, context->async_context()));
    shard_asio_subscribe_clients_.emplace_back(
        new RedisAsioClient(io_service, context->subscribe_context()));
  }

  boost::asio::io_service &io_service = primary_context_->io_service();
  asio_async_auxiliary_client_.reset(
      new RedisAsioClient(io_service, primary_context_->async_context()));
  asio_subscribe_auxiliary_client_.reset(
      new RedisAsioClient(io_service, primary_context_->subscribe_context()));
}

void RedisClient::Disconnect() {
  RAY_CHECK(is_connected_);
  is_connected_ = false;
  RAY_LOG(DEBUG) << "RedisClient disconnected.";
}

std::shared_ptr<RedisContext> RedisClient::GetShardContext(const std::string &shard_key) {
  static std::hash<std::string> hash;
  size_t index = hash(shard_key) % shard_contexts_.size();
  return shard_contexts_[index];
}

}  // namespace gcs

}  // namespace ray
