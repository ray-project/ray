// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

  for (const auto &key : keys_) {
    ++pending_read_count_;

    std::vector<std::string> args = {"GET", key};
    auto read_callback = [this, key](std::shared_ptr<CallbackReply> reply) {
      OnReadCallback(key, reply);
    };

    auto shard_context = redis_client_->GetShardContext(key);
    Status status = shard_context->RunArgvAsync(args, read_callback);
    if (!status.ok()) {
      RAY_LOG(INFO) << "Read key " << key << " failed, status " << status.ToString();
      is_failed_ = true;
      if (--pending_read_count_ == 0) {
        return status;
      }  // Else, waiting pending read done, then callback.
    }
  }
  return Status::OK();
}

void RedisMultiReader::OnReadCallback(const std::string &key,
                                      const std::shared_ptr<CallbackReply> &reply) {
  std::string value;
  if (!reply->IsNil()) {
    value = reply->ReadAsString();

    {
      absl::MutexLock lock(&mutex_);
      read_result_.emplace_back(key, value);
    }
  }

  if (--pending_read_count_ == 0) {
    OnDone();
  }
}

void RedisMultiReader::OnDone() {
  if (!is_failed_) {
    multi_read_callback_(Status::OK(), read_result_);
  } else {
    multi_read_callback_(Status::RedisError("Redis return failed."), {});
  }
}

}  // namespace gcs

}  // namespace ray
