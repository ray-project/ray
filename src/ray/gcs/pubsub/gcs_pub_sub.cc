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

#include "gcs_pub_sub.h"

namespace ray {
namespace gcs {

Status GcsPubSub::Publish(const std::string &channel, const std::string &id,
                          const std::string &data, const StatusCallback &done) {
  rpc::PubSubMessage message;
  message.set_id(id);
  message.set_data(data);

  auto on_done = [done](std::shared_ptr<CallbackReply> reply) {
    if (done) {
      done(Status::OK());
    }
  };

  return redis_client_->GetPrimaryContext()->PublishAsync(
      GenChannelPattern(channel, boost::optional<std::string>(id)),
      message.SerializeAsString(), on_done);
}

Status GcsPubSub::Subscribe(const std::string &channel, const std::string &id,
                            const Callback &subscribe, const StatusCallback &done) {
  return SubscribeInternal(channel, subscribe, done, boost::optional<std::string>(id));
}

Status GcsPubSub::SubscribeAll(const std::string &channel, const Callback &subscribe,
                               const StatusCallback &done) {
  return SubscribeInternal(channel, subscribe, done);
}

Status GcsPubSub::Unsubscribe(const std::string &channel, const std::string &id) {
  return redis_client_->GetPrimaryContext()->PUnsubscribeAsync(
      GenChannelPattern(channel, boost::optional<std::string>(id)));
}

Status GcsPubSub::SubscribeInternal(const std::string &channel, const Callback &subscribe,
                                    const StatusCallback &done,
                                    const boost::optional<std::string> &id) {
  std::string pattern = GenChannelPattern(channel, id);
  auto callback = [this, pattern, subscribe](std::shared_ptr<CallbackReply> reply) {
    if (!reply->IsNil()) {
      if (reply->IsUnsubscribeCallback()) {
        absl::MutexLock lock(&mutex_);
        ray::gcs::RedisCallbackManager::instance().remove(
            subscribe_callback_index_[pattern]);
        subscribe_callback_index_.erase(pattern);
      } else {
        const auto reply_data = reply->ReadAsPubsubData();
        if (!reply_data.empty()) {
          rpc::PubSubMessage message;
          message.ParseFromString(reply_data);
          subscribe(message.id(), message.data());
        }
      }
    }
  };

  int64_t out_callback_index;
  auto status = redis_client_->GetPrimaryContext()->PSubscribeAsync(pattern, callback,
                                                                    &out_callback_index);
  if (id) {
    absl::MutexLock lock(&mutex_);
    subscribe_callback_index_[pattern] = out_callback_index;
  }
  if (done) {
    done(status);
  }
  return status;
}

std::string GcsPubSub::GenChannelPattern(const std::string &channel,
                                         const boost::optional<std::string> &id) {
  std::stringstream pattern;
  pattern << channel << ":";
  if (id) {
    pattern << *id;
  } else {
    pattern << "*";
  }
  return pattern.str();
}

}  // namespace gcs
}  // namespace ray
