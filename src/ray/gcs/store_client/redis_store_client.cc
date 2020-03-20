#include "ray/gcs/store_client/redis_store_client.h"

#include <functional>
#include "ray/common/ray_config.h"
#include "ray/gcs/redis_context.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

RedisStoreClient::RedisStoreClient(const StoreClientOptions &options)
    : StoreClient(options) {
  RedisClientOptions redis_client_options(options_.server_ip_, options_.server_port_,
                                          options_.password_, options_.is_test_client_);
  redis_client_.reset(new RedisClient(redis_client_options));
}

RedisStoreClient::~RedisStoreClient() {}

Status RedisStoreClient::Connect(std::shared_ptr<IOServicePool> io_service_pool) {
  io_service_pool_ = io_service_pool;
  Status status = redis_client_->Connect(io_service_pool->GetAll());
  RAY_LOG(INFO) << "RedisStoreClient Connect status " << status.ToString();
  return status;
}

void RedisStoreClient::Disconnect() {
  redis_client_->Disconnect();
  RAY_LOG(INFO) << "RedisStoreClient disconnected.";
}

Status RedisStoreClient::AsyncPut(const std::string &table_name, const std::string &key,
                                  const std::string &value,
                                  const StatusCallback &callback) {
  std::string full_key = table_name + key;
  return DoPut(full_key, value, callback);
}

Status RedisStoreClient::AsyncPut(const std::string &table_name, const std::string &key,
                                  const std::string &index, const std::string &value,
                                  const StatusCallback &callback) {
  auto write_callback = [this, table_name, key, index, callback](Status status) {
    if (!status.ok()) {
      // Run callback if failed.
      if (callback != nullptr) {
        callback(status);
      }
      return;
    }

    // Write index to Redis.
    std::string index_table_key = index + table_name + key;
    status = DoPut(index_table_key, key, callback);

    if (!status.ok()) {
      // Run callback if failed.
      if (callback != nullptr) {
        callback(status);
      }
    }
  };

  // Write data to Redis.
  std::string full_key = table_name + key;
  return DoPut(full_key, value, write_callback);
}

Status RedisStoreClient::DoPut(const std::string &key, const std::string &value,
                               const StatusCallback &callback) {
  std::vector<std::string> args = {"SET", key, value};
  RedisCallback write_callback = nullptr;
  if (callback) {
    write_callback = [callback](std::shared_ptr<CallbackReply> reply) {
      auto status = reply->ReadAsStatus();
      callback(status);
    };
  }

  auto shard_context = redis_client_->GetShardContext(key);
  return shard_context->RunArgvAsync(args, write_callback);
}

Status RedisStoreClient::AsyncGet(const std::string &table_name, const std::string &key,
                                  const OptionalItemCallback<std::string> &callback) {
  RAY_CHECK(callback != nullptr);

  auto redis_callback = [callback](std::shared_ptr<CallbackReply> reply) {
    boost::optional<std::string> result;
    if (!reply->IsNil()) {
      result = reply->ReadAsString();
    }
    callback(Status::OK(), result);
  };

  std::string full_key = table_name + key;
  std::vector<std::string> args = {"GET", full_key};

  auto shard_context = redis_client_->GetShardContext(full_key);
  return shard_context->RunArgvAsync(args, redis_callback);
}

Status RedisStoreClient::AsyncGetByIndex(const std::string &table_name,
                                         const std::string &index,
                                         const MultiItemCallback<std::string> &callback) {
  RAY_CHECK(0) << "Not implemented! Will implement this function in next PR.";
  return Status::OK();
}

Status RedisStoreClient::AsyncGetAll(
    const std::string &table_name,
    const ScanCallback<std::pair<std::string, std::string>> &callback) {
  RAY_CHECK(0) << "Not implemented! Will implement this function in next PR.";
  return Status::OK();
}

Status RedisStoreClient::AsyncDelete(const std::string &table_name,
                                     const std::string &key,
                                     const StatusCallback &callback) {
  RedisCallback delete_callback = nullptr;
  if (callback) {
    delete_callback = [callback](std::shared_ptr<CallbackReply> reply) {
      int64_t deleted_count = reply->ReadAsInteger();
      RAY_LOG(DEBUG) << "Delete done, total delete count " << deleted_count;
      callback(Status::OK());
    };
  }

  std::string full_key = table_name + key;
  std::vector<std::string> args = {"DEL", full_key};

  auto shard_context = redis_client_->GetShardContext(full_key);
  return shard_context->RunArgvAsync(args, delete_callback);
}

Status RedisStoreClient::AsyncDeleteByIndex(const std::string &table_name,
                                            const std::string &index,
                                            const StatusCallback &callback) {
  RAY_CHECK(0) << "Not implemented! Will implement this function in next PR.";
  return Status::OK();
}

}  // namespace gcs

}  // namespace ray
