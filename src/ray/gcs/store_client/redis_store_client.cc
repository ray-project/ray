#include "ray/gcs/redis_store_client.h"

#include <functional>
#include "ray/gcs/redis_context.h"

namespace ray {

namespace gcs {

RedisStoreClient::RedisStoreClient(const RedisStoreClientOptions &options) {}

RedisStoreClient::~RedisStoreClient() {}

Status RedisStoreClient::Connect(std::shared_ptr<IOServicePool> io_service_pool) {}

void RedisStoreClient::Disconnect() {}

Status RedisStoreClient::AsyncPut(const std::string &table_name, const std::string &key,
                                  const std::string &value,
                                  const StatusCallback &callback) {
  std::string full_key = table_name + key;
  std::vector<std::string> args = {"SET", full_key, value};

  auto redis_callback = [callback](std::shared_ptr<CallbackReply> reply) {
    const auto status =reply->ReadAsStatus();
    if (callback != nullptr) {
      callback(status);
    }
  };

  auto shard_context = GetRedisContext(key);
  Status status = shard_context->RunArgvAsync(args, redis_callback);

  return status;
}

Status RedisStoreClient::AsyncPut(const std::string &table_name, const std::string &key,
                                  const std::string &index, const std::string &value,
                                  const StatusCallback &callback) {
  auto write_key_callback = [table_name, key, index, value, callback](Status status) {
    if (!status.ok()) {
      if (callback != nullptr) {
        callback(status);
      }
      return;
    }

    // Write index to redis.
    std::string index_full_key = index + key;
    status = AsyncPut(table_name, index_full_key, key, callback);

    if (!status.ok()) {
      if (callback != nullptr) {
        callback(status);
      }
    }
  };

  // Write key && value to redis.
  Status status = AsyncPut(table_name, key, value, write_key_callback);
  return status;
}

Status RedisStoreClient::AsyncGet(const std::string &table_name, const std::string &key,
                                  const OptionalItemCallback<std::string> &callback) {}

Status RedisStoreClient::AsyncGetByIndex(const std::string &table_name,
                                         const std::string &index,
                                         const MultiItemCallback<std::string> &callback) {
}

Status RedisStoreClient::AsyncGetAll(
    const std::string &table_name,
    const ScanCallback<std::string, std::string> &callback) {}

Status RedisStoreClient::AsyncDelete(const std::string &table_name,
                                     const std::string &key,
                                     const StatusCallback &callback) {}

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