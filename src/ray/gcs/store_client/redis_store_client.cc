#include "ray/gcs/store_client/redis_store_client.h"

#include <functional>
#include "ray/common/ray_config.h"
#include "ray/gcs/redis_context.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

static Status AsyncGetByKey(std::shared_ptr<RedisContext> redis_context,
                            const std::string &key,
                            const OptionalItemCallback<std::string> &callback) {
  RAY_CHECK(callback != nullptr);

  std::vector<std::string> args = {"GET", key};

  auto redis_callback = [callback](std::shared_ptr<CallbackReply> reply) {
    boost::optional<std::string> result;
    if (!reply->IsNil()) {
      result = reply->ReadAsString();
    }
    callback(Status::OK(), result);
  };

  Status status = redis_context->RunArgvAsync(args, redis_callback);
  return status;
}

static Status AsyncDeleteKeys(std::shared_ptr<RedisContext> redis_context,
                              const std::vector<std::string> &keys,
                              const StatusCallback &callback) {
  std::vector<std::string> args = {"DEL"};
  args.insert(args.end(), keys.begin(), keys.end());

  auto write_callback = [callback](std::shared_ptr<CallbackReply> reply) {
    int64_t deleted_count = reply->ReadAsInteger();
    RAY_LOG(DEBUG) << "Delete done, total delete count " << deleted_count;
    if (callback != nullptr) {
      callback(Status::OK());
    }
  };

  Status status = redis_context->RunArgvAsync(args, write_callback);
  return status;
}

RedisStoreClient::RedisStoreClient(const StoreClientOptions &options)
    : StoreClient(options) {
  RedisClientOptions redis_client_options(options_.server_ip_, options_.server_port_,
                                          options_.password_);
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
  RAY_LOG(INFO) << "RedisStoreClient Disconnect.";
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
  Status status = DoPut(full_key, value, write_callback);
  return status;
}

Status RedisStoreClient::DoPut(const std::string &key, const std::string &value,
                               const StatusCallback &callback) {
  std::vector<std::string> args = {"SET", key, value};

  auto write_callback = [callback](std::shared_ptr<CallbackReply> reply) {
    auto status = reply->ReadAsStatus();
    if (callback != nullptr) {
      callback(status);
    }
  };

  auto shard_context = redis_client_->GetPrimaryContext();
  Status status = shard_context->RunArgvAsync(args, write_callback);

  return status;
}

Status RedisStoreClient::AsyncGet(const std::string &table_name, const std::string &key,
                                  const OptionalItemCallback<std::string> &callback) {
  RAY_CHECK(callback != nullptr);

  std::string full_key = table_name + key;
  auto shard_context = redis_client_->GetPrimaryContext();
  return AsyncGetByKey(shard_context, full_key, callback);
}

Status RedisStoreClient::AsyncGetByIndex(const std::string &table_name,
                                         const std::string &index,
                                         const MultiItemCallback<std::string> &callback) {
  RAY_CHECK(callback != nullptr);
  // TODO(micafan) Confirm semantic of GetByIndex: get key or get value?
  // The semantics of the current implementation is the former.
  auto range_executor =
      std::make_shared<RedisRangeOpExecutor>(redis_client_, table_name, index, callback);
  return range_executor->Run();
}

Status RedisStoreClient::AsyncGetAll(
    const std::string &table_name,
    const ScanCallback<std::string, std::string> &callback) {
  RAY_CHECK(callback != nullptr);

  auto range_executor =
      std::make_shared<RedisRangeOpExecutor>(redis_client_, table_name, callback);
  return range_executor->Run();
}

Status RedisStoreClient::AsyncDelete(const std::string &table_name,
                                     const std::string &key,
                                     const StatusCallback &callback) {
  std::string full_key = table_name + key;

  std::vector<std::string> args = {"DEL", full_key};
  auto shard_context = redis_client_->GetPrimaryContext();
  return AsyncDeleteKeys(shard_context, {full_key}, callback);
}

Status RedisStoreClient::AsyncDeleteByIndex(const std::string &table_name,
                                            const std::string &index,
                                            const StatusCallback &callback) {
  auto delete_callback = [callback](Status status) {
    if (callback) {
      callback(status);
    }
  };

  auto range_executor = std::make_shared<RedisRangeOpExecutor>(redis_client_, table_name,
                                                               index, delete_callback);
  return range_executor->Run();
}

RedisRangeOpExecutor::RedisRangeOpExecutor(
    std::shared_ptr<RedisClient> redis_client, const std::string &table_name,
    const std::string &index, const MultiItemCallback<std::string> &get_by_index_callback)
    : redis_client_(redis_client),
      table_name_(table_name),
      index_(index),
      get_by_index_callback_(get_by_index_callback) {
  index_table_prefix_ = index + table_name;
  match_pattern_ = index_table_prefix_ + "*";

  data_table_prefix_ = table_name;
}

RedisRangeOpExecutor::RedisRangeOpExecutor(std::shared_ptr<RedisClient> redis_client,
                                           const std::string &table_name,
                                           const std::string &index,
                                           const StatusCallback &delete_by_index_callback)
    : redis_client_(redis_client),
      table_name_(table_name),
      index_(index),
      delete_by_index_callback_(delete_by_index_callback) {
  index_table_prefix_ = index + table_name;
  match_pattern_ = index_table_prefix_ + "*";

  data_table_prefix_ = table_name;
}

RedisRangeOpExecutor::RedisRangeOpExecutor(
    std::shared_ptr<RedisClient> redis_client, const std::string &table_name,
    const ScanCallback<std::string, std::string> &get_all_callback)
    : redis_client_(redis_client),
      table_name_(table_name),
      get_all_callback_(get_all_callback) {
  data_table_prefix_ = table_name;
  match_pattern_ = data_table_prefix_ + "*";
}

Status RedisRangeOpExecutor::Run() {
  DoScan();
  return Status::OK();
}

void RedisRangeOpExecutor::DoScan() {
  if (cursor_ == 0) {
    // Scan finishes.
    OnDone();
    return;
  }

  std::shared_ptr<RedisRangeOpExecutor> self = shared_from_this();
  auto redis_callback = [self](std::shared_ptr<CallbackReply> reply) {
    self->OnScanCallback(reply);
  };

  cursor_ = (cursor_ < 0) ? 0 : cursor_;
  // Scan by prefix from Redis.
  size_t batch_count = RayConfig::instance().gcs_service_scan_batch_size();
  std::vector<std::string> args = {"SCAN",  std::to_string(cursor_),
                                   "MATCH", match_pattern_,
                                   "COUNT", std::to_string(batch_count)};

  auto shard_context = redis_client_->GetPrimaryContext();
  status_ = shard_context->RunArgvAsync(args, redis_callback);
  if (!status_.ok()) {
    OnFailed();
  }
}

void RedisRangeOpExecutor::OnScanCallback(std::shared_ptr<CallbackReply> reply) {
  RAY_CHECK_OK(status_);

  if (!reply) {
    status_ = Status::RedisError("Redis error, got empty reply from redis.");
    OnFailed();
    return;
  }

  std::vector<std::string> keys;
  cursor_ = reply->ReadAsScanArray(&keys);
  std::vector<std::string> deduped_keys = DedupeKeys(keys);
  if (!deduped_keys.empty()) {
    ProcessScanResult(deduped_keys);
    return;
  }

  // Continue scan from Redis.
  DoScan();
}

void RedisRangeOpExecutor::OnFailed() {
  RAY_CHECK(!status_.ok());
  RAY_LOG(INFO) << "Execution failed, status " << status_.ToString();
  DoCallback();
}

void RedisRangeOpExecutor::OnDone() {
  RAY_CHECK(status_.ok() && cursor_ == 0);
  DoCallback();
}

void RedisRangeOpExecutor::DoCallback() {
  if (!status_.ok() || cursor_ == 0) {
    // If failed/done, run the callback.
    if (get_by_index_callback_) {
      get_by_index_callback_(status_, get_by_index_result_);
      get_by_index_result_.clear();
      get_by_index_callback_ = nullptr;
      return;
    }

    if (delete_by_index_callback_) {
      delete_by_index_callback_(status_);
      delete_by_index_callback_ = nullptr;
      return;
    }

    if (get_all_callback_) {
      get_all_callback_(status_, /*has_more*/ false, get_all_partial_result_);
      get_all_partial_result_.clear();
      get_all_callback_ = nullptr;
      return;
    }
  }

  if (get_all_callback_ && !get_all_partial_result_.empty()) {
    RAY_CHECK(cursor_ != 0);
    // Callback with partial result.
    get_all_callback_(status_, /*has_more*/ true, get_all_partial_result_);
    get_all_partial_result_.clear();
  }
}

std::vector<std::string> RedisRangeOpExecutor::DedupeKeys(
    const std::vector<std::string> &keys) {
  std::vector<std::string> deduped_keys;
  for (auto &key : keys) {
    auto it = keys_returned_by_scan_.find(key);
    if (it == keys_returned_by_scan_.end()) {
      deduped_keys.emplace_back(key);
      keys_returned_by_scan_.emplace(key);
    }
  }
  return deduped_keys;
}

void RedisRangeOpExecutor::ProcessScanResult(const std::vector<std::string> &keys) {
  // Parse data key from index key for operation: GetByIndex.
  DoParseKeys(keys);
  // Delete data keys and index keys from Redis for operation: DeleteByIndex.
  DoBatchDelete(keys);
  // Read data from Redis for operation: GetAll.
  DoMultiRead(keys);
}

void RedisRangeOpExecutor::DoParseKeys(const std::vector<std::string> &index_table_keys) {
  if (get_by_index_callback_) {
    for (const auto &index_table_key : index_table_keys) {
      // index_table_key contains three parts: index, table, data key (key of data).
      std::string data_key = index_table_key.substr(index_table_prefix_.length());
      RAY_CHECK(!data_key.empty());
      get_by_index_result_.emplace_back(data_key);
    }
  }
  // Trigger next scan.
  DoScan();
}

void RedisRangeOpExecutor::DoBatchDelete(
    const std::vector<std::string> &index_table_keys) {
  if (delete_by_index_callback_) {
    std::shared_ptr<RedisRangeOpExecutor> self = shared_from_this();

    std::vector<std::string> data_table_keys;
    for (const auto &index_table_key : index_table_keys) {
      // data_table_key contains two parts: table, data key (key of data).
      std::string data_table_key =
          table_name_ + index_table_key.substr(index_table_prefix_.length());
      data_table_keys.emplace_back(data_table_key);
    }

    auto delete_callback = [self](Status status) { self->OnDeleteCallback(status); };

    // Delete data from data table.
    auto redis_context = redis_client_->GetPrimaryContext();
    status_ = AsyncDeleteKeys(redis_context, data_table_keys, delete_callback);
    ++pending_delete_count_;
    if (!status_.ok()) {
      OnFailed();
      return;
    }

    // Delete index from index table.
    redis_context = redis_client_->GetPrimaryContext();
    status_ = AsyncDeleteKeys(redis_context, index_table_keys, delete_callback);
    ++pending_delete_count_;
    if (!status_.ok()) {
      OnFailed();
      return;
    }
  }
}

void RedisRangeOpExecutor::DoMultiRead(const std::vector<std::string> &data_table_keys) {
  if (get_all_callback_) {
    std::shared_ptr<RedisRangeOpExecutor> self = shared_from_this();
    auto redis_context = redis_client_->GetPrimaryContext();
    for (const auto &data_table_key : data_table_keys) {
      auto read_callback = [self, data_table_key](
                               Status status,
                               const boost::optional<std::string> &result) {
        self->OnReadCallback(status, result, data_table_key);
      };

      status_ = AsyncGetByKey(redis_context, data_table_key, read_callback);
      pending_read_keys_.emplace(data_table_key);
      if (!status_.ok()) {
        OnFailed();
        return;
      }
    }
    RAY_LOG(DEBUG) << "Current pending_read_keys count " << pending_read_keys_.size()
                   << " total keys_returned_by_scan count "
                   << keys_returned_by_scan_.size();
  }
}

void RedisRangeOpExecutor::OnReadCallback(Status status,
                                          const boost::optional<std::string> &result,
                                          const std::string &data_table_key) {
  if (!status_.ok()) {
    return;
  }

  if (!status.ok()) {
    status_ = status;
    OnFailed();
    return;
  }

  if (result) {
    RAY_CHECK(get_all_callback_);
    // Get data_key by trimming the table prefix from data_table_key.
    std::string data_key = data_table_key.substr(data_table_prefix_.length());
    get_all_partial_result_.emplace_back(data_key, *result);
  }

  pending_read_keys_.erase(data_table_key);
  if (pending_read_keys_.empty()) {
    DoCallback();
    DoScan();
  }
}

void RedisRangeOpExecutor::OnDeleteCallback(Status status) {
  --pending_delete_count_;

  if (!status_.ok()) {
    return;
  }

  if (!status.ok()) {
    status_ = status;
    OnFailed();
    return;
  }

  if (pending_delete_count_ == 0) {
    DoScan();
  }
}

}  // namespace gcs

}  // namespace ray
