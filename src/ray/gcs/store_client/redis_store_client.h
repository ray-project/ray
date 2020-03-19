#ifndef RAY_GCS_STORE_CLIENT_REDIS_STORE_CLIENT_H
#define RAY_GCS_STORE_CLIENT_REDIS_STORE_CLIENT_H

#include <memory>
#include <unordered_set>
#include "ray/gcs/redis_client.h"
#include "ray/gcs/redis_context.h"
#include "ray/gcs/store_client/store_client.h"

namespace ray {

namespace gcs {

class RedisStoreClient : public StoreClient {
 public:
  RedisStoreClient(const StoreClientOptions &options);

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

 private:
  Status DoPut(const std::string &key, const std::string &value,
               const StatusCallback &callback);

  std::shared_ptr<RedisClient> redis_client_;
};

/// \class RedisRangeOpExecutor
/// This class is used for three kind of operations:
/// 1. Get All Data with the same prefix from Redis (GetAll).
/// 2. Get All Data Key with the same index from Redis (GetByIndex).
/// 3. Delete Index and Data Key with the same index from Redis (DeleteByIndex).
/// TODO(micafan) Consider encapsulating three different classes:
/// Scanner, MultiReader, BatchDeleter.
class RedisRangeOpExecutor : public std::enable_shared_from_this<RedisRangeOpExecutor> {
 public:
  RedisRangeOpExecutor(std::shared_ptr<RedisClient> redis_client,
                       const std::string &table_name, const std::string &index,
                       const MultiItemCallback<std::string> &get_by_index_callback);

  RedisRangeOpExecutor(std::shared_ptr<RedisClient> redis_client,
                       const std::string &table_name, const std::string &index,
                       const StatusCallback &delete_by_index_callback);

  RedisRangeOpExecutor(std::shared_ptr<RedisClient> redis_client,
                       const std::string &table_name,
                       const ScanCallback<std::string, std::string> &get_all_callback);

  ~RedisRangeOpExecutor();

  Status Run();

 private:
  void OnFailed();

  void OnDone();

  void DoScan();

  void DoCallback();

  void OnScanCallback(std::shared_ptr<CallbackReply> reply);

  void ProcessScanResult(const std::vector<std::string> &keys);

  void DoParseKeys(const std::vector<std::string> &index_table_keys);

  void DoBatchDelete(const std::vector<std::string> &index_table_keys);

  void DoMultiRead(const std::vector<std::string> &data_table_keys);

  void OnReadCallback(Status status, const boost::optional<std::string> &result,
                      const std::string &data_key);

  void OnDeleteCallback(Status status);

  std::vector<std::string> DeduplicateKeys(const std::vector<std::string> &keys);

 private:
  std::shared_ptr<RedisClient> redis_client_{nullptr};

  std::string table_name_;
  std::string index_;

  MultiItemCallback<std::string> get_by_index_callback_{nullptr};
  std::vector<std::string> get_by_index_result_;

  StatusCallback delete_by_index_callback_{nullptr};
  std::atomic<int> pending_delete_count_{0};

  ScanCallback<std::string, std::string> get_all_callback_{nullptr};
  std::vector<std::pair<std::string, std::string>> get_all_partial_result_;
  std::unordered_set<std::string> pending_read_keys_;

  int cursor_{-1};
  // index && table_name
  std::string index_table_prefix_;
  // table_name
  std::string data_table_prefix_;
  std::string match_pattern_;

  Status status_{Status::OK()};
  std::unordered_set<std::string> keys_returned_by_scan_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_STORE_CLIENT_REDIS_STORE_CLIENT_H
