#ifndef RAY_GCS_STORE_CLIENT_H
#define RAY_GCS_STORE_CLIENT_H

#include "ray/gcs/store_client.h"

namespace ray {

namespace gcs {

class RedisStoreClientOptions {
 public:
  RedisStoreClientOptions(const std::string &ip, int port, const std::string &password)
      : server_ip_(ip), server_port_(port), password_(password) {}

  // Redis server ip.
  std::string server_ip_;
  // Redis server port.
  int server_port_;
  // Redis server password.
  std::string password_;
};

class RedisStoreClient : public StoreClient {
 public:
  RedisStoreClient(const RedisStoreClientOptions &options);

  virtual ~RedisStoreClient();

  Status Connect(std::shared_ptr<IOServicePool> io_service_pool) override;

  void Disconnect() override;

  Status AsyncPut(const std::string &table_name, const std::string &key,
                  const std::string &value, const StatusCallback &callback) override;

  Status AsyncPut(const std::string &table_name, const std::string &key,
                  const std::string &index, const std::string &value,
                  const StatusCallback &callback) override;

  Status AsyncGet(const std::string &table_name, const std::string &key,
                  const OptionalItemCallback<std::string> &callback) override;

  Status AsyncGetByIndex(const std::string &table_name, const std::string &index,
                         const MultiItemCallback<std::string> &callback) override;

  Status AsyncGetAll(const std::string &table_name,
                     const ScanCallback<std::string, std::string> &callback) override;

  Status AsyncDelete(const std::string &table_name, const std::string &key,
                     const StatusCallback &callback) override;

  Status AsyncDeleteByIndex(const std::string &table_name, const std::string &index,
                            const StatusCallback &callback) override;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_STORE_CLIENT_H