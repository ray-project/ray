#ifndef RAY_GCS_STORE_CLIENT_REDIS_SCANNER_H
#define RAY_GCS_STORE_CLIENT_REDIS_SCANNER_H

#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>
#include <vector>
#include "ray/gcs/callback.h"
#include "ray/gcs/redis_client.h"
#include "ray/gcs/redis_context.h"

namespace ray {

namespace gcs {

class ScanRequest {
 public:
  ScanRequest() {}

  void SwapOut(ScanRequest *other);

  enum class ScanType : int8_t {
    kScanAllRows = 0,
    kScanPartialRows = 1,
    kScanAllKeys = 2,
    kScanPartialKeys = 3,
    kUnknown = 4,
  };

  ScanType scan_type_{ScanType::kUnknown};

  /// The scan match pattern.
  std::string match_pattern_;

  /// The callback that will be called after the ScanRows finishes.
  MultiItemCallback <
      std::pair<std::string, std::string> scan_all_rows_callback_{nullptr};
  /// The callback that will be called when ScanPartialRows receving some data from redis.
  /// And the scan may not done.
  SegmentedCallback<std::string, std::string> scan_partial_rows_callback_{nullptr};
  /// The callback that will be called after the ScanKeys finishes.
  MultiItemCallback < std::pair<std::string> scan_all_keys_callback_{nullptr};
  /// The callback that will be called when ScanPartialKeys receving some data from redis.
  /// And the scan may not done.
  SegmentedCallback<std::string> scan_partial_keys_callback_{nullptr};

  /// The scan result in rows.
  /// If the scan type is kScanPartialRows, partial scan result will be saved in this
  /// variable. If the scan type is kScanAllRows, all scan result will be saved in this
  /// variable.
  std::vector<std::pair<std::string, std::string>> rows_;

  /// The scan result in keys.
  /// If the scan type is kScanPartialKeys, partial scan result will be saved in this
  /// variable. If the scan type is kScanAllKeys, all scan result will be saved in this
  /// variable.
  std::vector<std::string> keys_;
};

/// \class RedisScanner
/// This class is used to scan data from Redis.
///
/// Call method `ScanKeys` if you want to scan all keys at once.
/// Call method `ScanPartialKeys` if you want to scan partial keys at one time.
/// Call method `ScanRows` if you want to scan all rows at once.
/// Call method `ScanPartialRows` if you want to scan partial rows at one time.
///
/// If you called one method, should never call the other methods.
/// Otherwise it will disturb the status of the RedisScanner.
class RedisScanner {
 public:
  /// Constructor of RedisScanner.
  ///
  /// \param redis_client The redis client that used to access redis.
  /// \param match_pattern The scan match pattern that used for scan.
  RedisScanner(std::shared_ptr<RedisClient> redis_client,
               const std::string &match_pattern);

  ~RedisScanner();

  /// Start scan keys. Will callback after the scan finishes(receiving all data from
  /// redis).
  ///
  /// This function is non-thread safe.
  ///
  /// \param callback The callback will be called after scan finishes.
  /// All result will be returned.
  /// \return Status
  Status ScanKeys(const MultiItemCallback<std::string> &callback);

  /// Start or continue scan keys. Will callback immediately after receiving some data
  /// from redis. Should call this method again if you want scan the rest data. Should not
  /// call other methods once you call this method.
  ///
  /// This function is non-thread safe.
  ///
  /// If the callback return `has_more == true`, means there has more data
  /// to be received, the scan is not finish.
  /// Otherwise, the scan finishes.
  ///
  /// \param callback The callback will be called when receiving some data.
  /// \return Status
  Status ScanPartialKeys(const SegmentedCallback<std::string> &callback);

  /// Start scan rows. Will callback after the scan finishes(receiving all data from
  /// redis).
  ///
  /// This function is non-thread safe.
  ///
  /// \param callback The callback will be called after scan finishes.
  /// All result will be returned.
  /// \return Status
  Status ScanRows(const MultiItemCallback < std::pair<std::string, std::string> &
                  callback);

  /// Start or continue scan rows. Will callback immediately after receiving some data
  /// from redis. Should call this method again if you want scan the rest data. Should not
  /// call other methods once you call this method.
  ///
  /// This function is non-thread safe.
  /// If the callback return `has_more == true`, means there has more data
  /// to be received, the scan is not finish.
  /// Otherwise, the scan finishes.
  ///
  /// \param callback The callback will be called when receiving some data.
  /// \return Status
  Status ScanPartialRows(const SegmentedCallback<std::string, std::string> &callback);

 private:
  /// Scan from Redis.
  void DoScan();

  /// Process scan done.
  void OnDone();

  /// Process when scan Redis callback.
  void OnScanCallback(size_t shard_index, std::shared_ptr<CallbackReply> reply);

  /// Process scan result.
  ///
  /// \param shard_index The index of the shard which execute the scan command.
  /// \param cousor The scan cousor of the shard.
  /// \param scan_result The keys returned by scan command.
  /// \param pending_done Whether all pending scan is finishes.
  void ProcessScanResult(size_t shard_index, size_t cousor,
                         const std::vector<std::string> &scan_result, bool pending_done);

  /// Run callback for partial scan.
  void DoPartialCallback();

  /// Deduplicate the keys returned by scan command.
  ///
  /// \return Return deduplicated keys.
  std::vector<std::string> Deduplicate(const std::vector<std::string> &scan_result);

  /// Execute multi read command.
  void DoMultiRead();

  /// Process read result of read command.
  ///
  /// \param status Read command execution status.
  /// \param read_result The result of read command.
  void OnReadCallback(
      Status status, const std::vector<std::pair<std::string, std::string>> &read_result);

  /// Update result keys.
  ///
  /// \return The size of result.
  size_t UpdateResult(const std::vector<std::string> &keys);

  /// Update result rows.
  ///
  /// \return The size of result.
  size_t UpdateResult(const std::vector<std::pair<std::string, std::string>> &rows);

 private:
  /// Redis client.
  std::shared_ptr<RedisClient> redis_client_;
  std::vector<std::shared_ptr<RedisContext>> shard_contexts_;

  mutable std::Mutex mutex_;

  ScanRequest scan_request_;

  /// Whether the scan is failed.
  std::atomic<bool> is_failed_{false};
  /// Whether the scan is finishes.
  std::atomic<bool> is_scan_done_{false};

  /// The pending shard scan count.
  std::atomic<size_t> pending_request_count_{0};

  /// The scan cursor for each shard.
  std::unordered_map<size_t, size_t> shard_to_cursor_;

  /// All keys that received from redis.
  std::unordered_set<std::string> all_received_keys_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_STORE_CLIENT_REDIS_SCANNER_H