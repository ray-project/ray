// Copyright 2021 The Ray Authors.
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

#include "ray/gcs/pubsub_handler.h"

#include <string>
#include <utility>

namespace ray {
namespace gcs {

InternalPubSubHandler::InternalPubSubHandler(instrumented_io_context &io_service,
                                             pubsub::GcsPublisher &gcs_publisher)
    : io_service_(io_service), gcs_publisher_(gcs_publisher) {}

void InternalPubSubHandler::HandleGcsPublish(rpc::GcsPublishRequest request,
                                             rpc::GcsPublishReply *reply,
                                             rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "received publish request: " << request.DebugString();
  for (auto &&msg : std::move(*request.mutable_pub_messages())) {
    gcs_publisher_.GetPublisher().Publish(std::move(msg));
  }
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

// Needs to use rpc::GcsSubscriberPollRequest and rpc::GcsSubscriberPollReply here,
// and convert the reply to rpc::PubsubLongPollingReply because GCS RPC services are
// required to have the `status` field in replies.
void InternalPubSubHandler::HandleGcsSubscriberPoll(
    rpc::GcsSubscriberPollRequest request,
    rpc::GcsSubscriberPollReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  rpc::PubsubLongPollingRequest pubsub_req;
  pubsub_req.set_subscriber_id(std::move(*request.mutable_subscriber_id()));
  pubsub_req.set_publisher_id(std::move(*request.mutable_publisher_id()));
  pubsub_req.set_max_processed_sequence_id(request.max_processed_sequence_id());
  gcs_publisher_.GetPublisher().ConnectToSubscriber(pubsub_req,
                                                    reply->mutable_publisher_id(),
                                                    reply->mutable_pub_messages(),
                                                    std::move(send_reply_callback));
}

// Similar for HandleGcsSubscriberPoll() above, needs to use
// rpc::GcsSubscriberCommandBatchReply as reply type instead of using
// rpc::GcsSubscriberCommandBatchReply directly.
void InternalPubSubHandler::HandleGcsSubscriberCommandBatch(
    rpc::GcsSubscriberCommandBatchRequest request,
    rpc::GcsSubscriberCommandBatchReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const auto subscriber_id = UniqueID::FromBinary(request.subscriber_id());

  // If the sender_id field is not set, subscriber_id will be used instead.
  auto sender_id = request.sender_id();
  if (sender_id.empty()) {
    sender_id = request.subscriber_id();
  }

  auto iter = sender_to_subscribers_.find(sender_id);
  if (iter == sender_to_subscribers_.end()) {
    iter = sender_to_subscribers_.insert({sender_id, {}}).first;
  }

  for (const auto &command : request.commands()) {
    if (command.has_unsubscribe_message()) {
      gcs_publisher_.GetPublisher().UnregisterSubscription(
          command.channel_type(),
          subscriber_id,
          command.key_id().empty() ? std::nullopt : std::make_optional(command.key_id()));
      iter->second.erase(subscriber_id);
    } else if (command.has_subscribe_message()) {
      gcs_publisher_.GetPublisher().RegisterSubscription(
          command.channel_type(),
          subscriber_id,
          command.key_id().empty() ? std::nullopt : std::make_optional(command.key_id()));
      iter->second.insert(subscriber_id);
    } else {
      RAY_LOG(FATAL) << "Invalid command has received, "
                     << static_cast<int>(command.command_message_one_of_case())
                     << ". If you see this message, please file an issue to Ray Github.";
    }
  }
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void InternalPubSubHandler::AsyncRemoveSubscriberFrom(const std::string &sender_id) {
  io_service_.post(
      [this, sender_id]() {
        auto iter = sender_to_subscribers_.find(sender_id);
        if (iter == sender_to_subscribers_.end()) {
          return;
        }
        for (auto &subscriber_id : iter->second) {
          gcs_publisher_.GetPublisher().UnregisterSubscriber(subscriber_id);
        }
        sender_to_subscribers_.erase(iter);
      },
      "RemoveSubscriberFrom");
}

}  // namespace gcs
}  // namespace ray
