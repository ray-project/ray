#include "ray/gcs/redis_store_client.h"
#include "ray/gcs/redis_context.h"

namespace ray {

namespace gcs {

RedisStoreClient::RedisStoreClient(const RedisStoreClientOptions &options) {}

RedisStoreClient::~RedisStoreClient() {}

Status RedisStoreClient::Connect(std::shared_ptr<IOServicePool> io_service_pool) {}

void RedisStoreClient::Disconnect() {}

Status RedisStoreClient::AsyncPut(const std::string &table_name, const std::string &key,
                                  const std::string &value,
                                  const StatusCallback &callback) {}

Status RedisStoreClient::AsyncPut(const std::string &table_name, const std::string &key,
                                  const std::string &index, const std::string &value,
                                  const StatusCallback &callback) {}

Status RedisStoreClient::AsyncGet(const std::string &table_name, const std::string &key,
                                  const OptionalItemCallback<std::string> &callback) {}

Status RedisStoreClient::AsyncGetByIndex(const std::string &table_name,
                                         const std::string &index,
                                         const MultiItemCallback &callback) {}

Status RedisStoreClient::AsyncGetAll(const std::string &table_name,
                                     const ScanCallback<std::string> &callback) {}

Status RedisStoreClient::AsyncDelete(const std::string &table_name,
                                     const std::string &key,
                                     const StatusCallback &callback) {}

Status RedisStoreClient::AsyncDeleteByIndex(const std::string &table_name,
                                            const std::string &index,
                                            const StatusCallback &callback) {}

}  // namespace gcs

}  // namespace ray