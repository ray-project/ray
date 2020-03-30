#include "ray/gcs/store_client/redis_multi_reader.h"

namespace ray {

namespace gcs {

RedisMultiReader::RedisMultiReader(std::shared_ptr<RedisClient> redis_client,
                                   const std::vector<std::string> &keys)
    : redis_client_(std::move(redis_client)), keys_(keys) {
  RAY_CHECK(!keys_.empty());
}

RedisMultiReader::~RedisMultiReader() {}

Status RedisMultiReader::Read(
    const MultiItemCallback<std::pair<std::string, std::string>> &callback) {
  RAY_CHECK(callback);
  RAY_CHECK(multi_read_callback_ == nullptr);
  multi_read_callback_ = callback;

  for (const auto &key : keys) {
    ++pending_read_count_;
    Status status = SingleRead(key);
    if (!status.ok()) {
      is_failed_ = true;
      if (--pending_read_count_ == 0) {
        OnDone();
      }
      RAY_LOG(INFO) << "Read key " << key << " failed, status " << status.ToString();
      return;
    }
  }
}

Status RedisMultiReader::SingleRead(const std::string &key,
                                    const OptionalItemCallback<std::string> &callback) {
  std::vector<std::string> args = {"GET", key};
  auto read_callback = [this, key](std::shared_ptr<CallbackReply> reply) {
    OnReadCallback(key, reply);
  };

  auto shard_context = redis_client_->GetShardContext(key);
  return shard_context->RunArgvAsync(args, callback);
}

void RedisMultiReader::OnReadCallback(const std::string &key,
                                      const std::shared_ptr<CallbackReply> &reply) {
  std::string value;
  if (!reply->IsNil()) {
    value = reply->ReadAsString();

    {
      std::lock_guard<std::mutex> lock(&mutex_);
      read_result_.emplace_back(key, value);
    }
  }

  if (--pending_read_count_ == 0) {
    OnDone();
  }
}

void RedisMultiReader::OnDone() {
  if (!is_failed_) {
    std::vector<std::pair<std::string, std::string>> result;
    {
      std::lock_guard<std::mutex> lock(&mutex_);
      result.swap(read_result_);
    }
    multi_read_callback_(Status::OK(), result);
  } else {
    multi_read_callback_(Status::RedisError(), {});
  }
}

}  // namespace gcs

}  // namespace ray
