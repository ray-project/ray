#ifndef RAY_GCS_STORE_CLIENT_REDIS_MULTI_READER_H
#define RAY_GCS_STORE_CLIENT_REDIS_MULTI_READER_H

#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>
#include <vector>
#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/redis_client.h"
#include "ray/gcs/redis_context.h"

namespace ray {

namespace gcs {

class RedisMultiReader {
 public:
  /// Constructor of RedisMultiReader.
  ///
  /// \param redis_client The redis client that used to access redis.
  /// \param keys The keys that will be read from redis.
  RedisMultiReader(std::shared_ptr<RedisClient> redis_client,
                   const std::vector<std::string> &keys);

  ~RedisMultiReader();

  /// Start read from redis. Will callback when the read finishes.
  /// RedisMultiReader can't be reuse, event after `Read` finishes.
  ///
  /// \param callback The callback that will be called when the read finished.
  /// \return Status
  Status Read(const MultiItemCallback<std::pair<std::string, std::string>> &callback);

 private:
  /// Run callback when multi read finishes.
  void OnDone() NO_THREAD_SAFETY_ANALYSIS;

  /// Process single read result.
  void OnReadCallback(const std::string &key, const std::shared_ptr<CallbackReply> &reply)
      LOCKS_EXCLUDED(mutex_);

 private:
  /// Redis client.
  std::shared_ptr<RedisClient> redis_client_;
  /// Keys need to be read from redis.
  std::vector<std::string> keys_;
  /// Multi read callback.
  MultiItemCallback<std::pair<std::string, std::string>> multi_read_callback_{nullptr};

  /// Total pending read request count.
  std::atomic<size_t> pending_read_count_{0};

  /// Whether multi read failed.
  std::atomic<bool> is_failed_{false};

  mutable absl::Mutex mutex_;

  /// The result that redis returns.
  std::vector<std::pair<std::string, std::string>> read_result_ GUARDED_BY(mutex_);
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_STORE_CLIENT_REDIS_MULTI_READER_H