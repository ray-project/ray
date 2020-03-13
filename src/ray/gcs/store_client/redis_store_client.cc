#include "ray/gcs/redis_store_client.h"

#include <functional>
#include "ray/gcs/redis_context.h"

namespace ray {

namespace gcs {

RedisStoreClient::RedisStoreClient(const RedisStoreClientOptions &options) {
  RedisClientOptions redis_options(options.server_ip_, options.server_port_,
                                   options.password_);
  redis_client.reset(new RedisClient(redis_options));
}

RedisStoreClient::~RedisStoreClient() {}

Status RedisStoreClient::Connect(std::shared_ptr<IOServicePool> io_service_pool) {}

void RedisStoreClient::Disconnect() { redis_client->Disconnect(); }

Status RedisStoreClient::AsyncPut(const std::string &table_name, const std::string &key,
                                  const std::string &value,
                                  const StatusCallback &callback) {
  return DoPut(table_name, key, value, /*shard_key*/ key, callback);
}

Status RedisStoreClient::AsyncPut(const std::string &table_name, const std::string &key,
                                  const std::string &index, const std::string &value,
                                  const StatusCallback &callback) {
  auto write_callback = [table_name, key, index, callback](Status status) {
    if (!status.ok()) {
      if (callback != nullptr) {
        callback(status);
      }
      return;
    }

    // Write index to redis.
    std::string index_key = index + key;
    status = AsyncPut(table_name, index_key, key, /*shard_key*/ index, callback);

    if (!status.ok()) {
      if (callback != nullptr) {
        callback(status);
      }
    }
  };

  // Write key && value to redis.
  Status status = DoPut(table_name, key, value, /*shard_key*/ key, write_callback);
  return status;
}

Status RedisStoreClient::DoPut(const std::string &table_name, const std::string &key,
                               const std::string &value, const std::string &shard_key,
                               const StatusCallback &callback) {
  std::string full_key = table_name + key;
  std::vector<std::string> args = {"SET", full_key, value};

  auto write_callback = [callback](std::shared_ptr<CallbackReply> reply) {
    auto status = reply->ReadAsStatus();
    if (callback != nullptr) {
      callback(status);
    }
  };

  // Pick shard context with shard key.
  auto shard_context = GetRedisContext(shard_key);
  Status status = shard_context->RunArgvAsync(args, write_callback);

  return status;
}

Status RedisStoreClient::AsyncGet(const std::string &table_name, const std::string &key,
                                  const OptionalItemCallback<std::string> &callback) {
  RAY_CHECK(callback != nullptr);

  std::string full_key = table_name + key;
  std::vector<std::string> args = {"GET", full_key};

  auto redis_callback = [callback](std::shared_ptr<CallbackReply> reply) {
    boost::optional<std::string> result;
    if (!reply->IsNil()) {
      result = reply->ReadAsString();
    }
    callback(Status::OK(), result);
  };

  auto shard_context = GetRedisContext(key);
  Status status = shard_context->RunArgvAsync(args, redis_callback);

  return status;
}

Status RedisStoreClient::AsyncGetByIndex(const std::string &table_name,
                                         const std::string &index,
                                         const MultiItemCallback<std::string> &callback) {
  RAY_CHECK(callback != nullptr);

  std::string full_key = table_name + index;
  std::vector<std::string> args = {"GET", full_key};

  auto redis_callback = [callback](std::shared_ptr<CallbackReply> reply) {
    boost::optional<std::string> result;
    if (!reply->IsNil()) {
      result = reply->ReadAsString();
    }
    callback(Status::OK(), result);
  };

  auto shard_context = GetRedisContext(index);
  Status status = shard_context->RunArgvAsync(args, redis_callback);

  return status;
}

Status RedisStoreClient::AsyncGetAll(
    const std::string &table_name,
    const ScanCallback<std::string, std::string> &callback) {}

Status RedisStoreClient::AsyncDelete(const std::string &table_name,
                                     const std::string &key,
                                     const StatusCallback &callback) {
  std::string full_key = table_name + key;
  std::vector<std::string> args = {"DEL", full_key};

  auto write_callback = [callback](std::shared_ptr<CallbackReply> reply) {
    auto status = reply->ReadAsStatus();
    if (callback != nullptr) {
      callback(status);
    }
  };

  // Pick shard context with shard key.
  auto shard_context = GetRedisContext(key);
  Status status = shard_context->RunArgvAsync(args, write_callback);

  return status;
}

Status RedisStoreClient::AsyncDeleteByIndex(const std::string &table_name,
                                            const std::string &index,
                                            const StatusCallback &callback) {}

std::shared_ptr<RedisContext> RedisStoreClient::GetRedisContext(const std::string &key) {
  size_t hash = std::hash<std::string>(key);
  auto shard_contexts = redis_client->GetShardContexts();
  size_t index = hash % shard_contexts.size();
  return shard_contexts[index];
}

}  // namespace gcs

}  // namespace ray