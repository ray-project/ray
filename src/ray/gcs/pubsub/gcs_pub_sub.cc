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

#include "ray/gcs/pubsub/gcs_pub_sub.h"

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
  return SubscribeInternal(channel, subscribe, done, false,
                           boost::optional<std::string>(id));
}

Status GcsPubSub::SubscribeAll(const std::string &channel, const Callback &subscribe,
                               const StatusCallback &done) {
  return SubscribeInternal(channel, subscribe, done, true);
}

Status GcsPubSub::Unsubscribe(const std::string &channel_name, const std::string &id) {
  std::string pattern = GenChannelPattern(channel_name, id);

  absl::MutexLock lock(&mutex_);
  // Add the UNSUBSCRIBE command to the queue.
  auto channel = channels_.find(pattern);
  RAY_CHECK(channel != channels_.end());
  channel->second.command_queue.push_back(Command());
  total_commands_queued_++;

  // Process the first command on the queue, if possible.
  return ExecuteCommandIfPossible(channel->first, channel->second);
}

Status GcsPubSub::SubscribeInternal(const std::string &channel_name,
                                    const Callback &subscribe, const StatusCallback &done,
                                    bool is_sub_or_unsub_all,
                                    const boost::optional<std::string> &id) {
  std::string pattern = GenChannelPattern(channel_name, id);

  absl::MutexLock lock(&mutex_);
  auto channel = channels_.find(pattern);
  if (channel == channels_.end()) {
    // There were no pending commands for this channel and we were not already
    // subscribed.
    channel = channels_.emplace(pattern, Channel()).first;
  }

  // Add the SUBSCRIBE command to the queue.
  channel->second.command_queue.push_back(Command(subscribe, done, is_sub_or_unsub_all));
  total_commands_queued_++;

  // Process the first command on the queue, if possible.
  return ExecuteCommandIfPossible(channel->first, channel->second);
}

Status GcsPubSub::ExecuteCommandIfPossible(const std::string &channel_key,
                                           GcsPubSub::Channel &channel) {
  // Process the first command on the queue, if possible.
  Status status;
  auto &command = channel.command_queue.front();
  if (command.is_subscribe && channel.callback_index == -1) {
    // The next command is SUBSCRIBE and we are currently unsubscribed, so we
    // can execute the command.
    int64_t callback_index =
        ray::gcs::RedisCallbackManager::instance().AllocateCallbackIndex();
    const auto &command_done_callback = command.done_callback;
    const auto &command_subscribe_callback = command.subscribe_callback;
    auto callback = [this, channel_key, command_done_callback, command_subscribe_callback,
                     callback_index](std::shared_ptr<CallbackReply> reply) {
      if (reply->IsNil()) {
        return;
      }
      if (reply->IsUnsubscribeCallback()) {
        // Unset the callback index.
        absl::MutexLock lock(&mutex_);
        auto channel = channels_.find(channel_key);
        RAY_CHECK(channel != channels_.end());
        ray::gcs::RedisCallbackManager::instance().RemoveCallback(
            channel->second.callback_index);
        channel->second.callback_index = -1;
        channel->second.pending_reply = false;

        if (channel->second.command_queue.empty()) {
          // We are unsubscribed and there are no more commands to process.
          // Delete the channel.
          channels_.erase(channel);
        } else {
          // Process the next item in the queue.
          RAY_CHECK(channel->second.command_queue.front().is_subscribe);
          RAY_CHECK_OK(ExecuteCommandIfPossible(channel_key, channel->second));
        }
      } else if (reply->IsSubscribeCallback()) {
        {
          // Set the callback index.
          absl::MutexLock lock(&mutex_);
          auto channel = channels_.find(channel_key);
          RAY_CHECK(channel != channels_.end());
          channel->second.callback_index = callback_index;
          channel->second.pending_reply = false;
          // Process the next item in the queue, if any.
          if (!channel->second.command_queue.empty()) {
            RAY_CHECK(!channel->second.command_queue.front().is_subscribe);
            RAY_CHECK_OK(ExecuteCommandIfPossible(channel_key, channel->second));
          }
        }

        if (command_done_callback) {
          command_done_callback(Status::OK());
        }
      } else {
        const auto reply_data = reply->ReadAsPubsubData();
        if (!reply_data.empty()) {
          rpc::PubSubMessage message;
          message.ParseFromString(reply_data);
          command_subscribe_callback(message.id(), message.data());
        }
      }
    };

    if (command.is_sub_or_unsub_all) {
      status = redis_client_->GetPrimaryContext()->PSubscribeAsync(channel_key, callback,
                                                                   callback_index);
    } else {
      status = redis_client_->GetPrimaryContext()->SubscribeAsync(channel_key, callback,
                                                                  callback_index);
    }
    channel.pending_reply = true;
    channel.command_queue.pop_front();
    total_commands_queued_--;
  } else if (!command.is_subscribe && channel.callback_index != -1) {
    // The next command is UNSUBSCRIBE and we are currently subscribed, so we
    // can execute the command. The reply for will be received through the
    // SUBSCRIBE command's callback.
    if (command.is_sub_or_unsub_all) {
      status = redis_client_->GetPrimaryContext()->PUnsubscribeAsync(channel_key);
    } else {
      status = redis_client_->GetPrimaryContext()->UnsubscribeAsync(channel_key);
    }
    channel.pending_reply = true;
    channel.command_queue.pop_front();
    total_commands_queued_--;
  } else if (!channel.pending_reply) {
    // There is no in-flight command, but the next command to execute is not
    // runnable. The caller must have sent a command out-of-order.
    // TODO(swang): This can cause a fatal error if the GCS server restarts and
    // the client attempts to subscribe again.
    RAY_LOG(FATAL) << "Caller attempted a duplicate subscribe or unsubscribe to channel "
                   << channel_key;
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

bool GcsPubSub::IsUnsubscribed(const std::string &channel, const std::string &id) {
  std::string pattern = GenChannelPattern(channel, id);

  absl::MutexLock lock(&mutex_);
  return !channels_.contains(pattern);
}

std::string GcsPubSub::DebugString() const {
  absl::MutexLock lock(&mutex_);
  std::ostringstream stream;
  stream << "GcsPubSub:";
  stream << "\n- num channels subscribed to: " << channels_.size();
  stream << "\n- total commands queued: " << total_commands_queued_;
  return stream.str();
}

}  // namespace gcs
}  // namespace ray
