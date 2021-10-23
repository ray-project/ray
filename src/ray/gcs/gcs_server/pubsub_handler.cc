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

#include "ray/gcs/gcs_server/pubsub_handler.h"

namespace ray {
namespace gcs {

void InternalPubSubHandler::HandleGcsSubscriberPoll(
    const rpc::GcsSubscriberPollRequest &request, rpc::GcsSubscriberPollReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const auto subscriber_id = UniqueID::FromBinary(request.subscriber_id());
  rpc::PubsubLongPollingReply pubsub_reply;
  gcs_publisher_->GetPublisher()->ConnectToSubscriber(subscriber_id, &pubsub_reply,
                                                      std::move(send_reply_callback));
  reply->mutable_pub_messages()->Swap(pubsub_reply.mutable_pub_messages());
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void InternalPubSubHandler::HandleGcsSubscriberCommandBatch(
    const rpc::GcsSubscriberCommandBatchRequest &request,
    rpc::GcsSubscriberCommandBatchReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const auto subscriber_id = UniqueID::FromBinary(request.subscriber_id());
  for (const auto &command : request.commands()) {
    if (command.has_unsubscribe_message()) {
      gcs_publisher_->GetPublisher()->UnregisterSubscription(
          command.channel_type(), subscriber_id, command.key_id());
    } else if (command.has_subscribe_message()) {
      gcs_publisher_->GetPublisher()->RegisterSubscription(
          command.channel_type(), subscriber_id, command.key_id());
    } else {
      RAY_LOG(FATAL) << "Invalid command has received, "
                     << static_cast<int>(command.command_message_one_of_case())
                     << ". If you see this message, please "
                        "report to Ray Github.";
    }
  }
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

}  // namespace gcs
}  // namespace ray