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

#include "absl/strings/str_format.h"
#include "ray/common/id.h"

namespace ray {
namespace gcs {

PubSubHandlerBase::PubSubHandlerBase(instrumented_io_context &io_service,
                                     pubsub::PublisherInterface &publisher)
    : io_service_(io_service), publisher_(publisher) {}

void PubSubHandlerBase::HandleGcsPublish(rpc::GcsPublishRequest request,
                                         rpc::GcsPublishReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "received publish request: " << request.DebugString();
  for (auto &&msg : std::move(*request.mutable_pub_messages())) {
    publisher_.Publish(std::move(msg));
  }
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

// Needs to use rpc::GcsSubscriberPollRequest and rpc::GcsSubscriberPollReply here,
// and convert the reply to rpc::PubsubLongPollingReply because GCS RPC services are
// required to have the `status` field in replies.
void PubSubHandlerBase::HandleGcsSubscriberPoll(
    rpc::GcsSubscriberPollRequest request,
    rpc::GcsSubscriberPollReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  rpc::PubsubLongPollingRequest pubsub_req;
  pubsub_req.set_subscriber_id(std::move(*request.mutable_subscriber_id()));
  pubsub_req.set_publisher_id(std::move(*request.mutable_publisher_id()));
  pubsub_req.set_max_processed_sequence_id(request.max_processed_sequence_id());
  publisher_.ConnectToSubscriber(pubsub_req,
                                 reply->mutable_publisher_id(),
                                 reply->mutable_pub_messages(),
                                 std::move(send_reply_callback));
}

// Similar for HandleGcsSubscriberPoll() above, needs to use
// rpc::GcsSubscriberCommandBatchReply as reply type instead of using
// rpc::GcsSubscriberCommandBatchReply directly.
void PubSubHandlerBase::HandleGcsSubscriberCommandBatch(
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

  Status status = Status::OK();
  for (const auto &command : request.commands()) {
    if (!command.has_unsubscribe_message() && !command.has_subscribe_message()) {
      send_reply_callback(Status::InvalidArgument(absl::StrFormat(
                              "Unexpected pubsub command has been received: %s."
                              "Expected either unsubscribe or subscribe message",
                              command.DebugString())),
                          nullptr,
                          nullptr);
      return;
    }

    if (command.has_unsubscribe_message()) {
      publisher_.UnregisterSubscription(
          command.channel_type(),
          subscriber_id,
          command.key_id().empty() ? std::nullopt : std::make_optional(command.key_id()));
      iter->second.erase(subscriber_id);
    } else {  // subscribe_message case
      StatusSet<StatusT::InvalidArgument> result = publisher_.RegisterSubscription(
          command.channel_type(),
          subscriber_id,
          command.key_id().empty() ? std::nullopt : std::make_optional(command.key_id()));
      if (result.has_error()) {
        send_reply_callback(
            Status::InvalidArgument(
                std::get<StatusT::InvalidArgument>(result.error()).message()),
            nullptr,
            nullptr);
        return;
      }
      iter->second.insert(subscriber_id);
    }
  }
  send_reply_callback(status, nullptr, nullptr);
}

void PubSubHandlerBase::AsyncRemoveSubscriberFrom(const std::string &sender_id) {
  io_service_.post(
      [this, sender_id]() {
        auto iter = sender_to_subscribers_.find(sender_id);
        if (iter == sender_to_subscribers_.end()) {
          return;
        }
        for (auto &subscriber_id : iter->second) {
          publisher_.UnregisterSubscriber(subscriber_id);
        }
        sender_to_subscribers_.erase(iter);
      },
      "RemoveSubscriberFrom");
}

InternalPubSubHandler::InternalPubSubHandler(instrumented_io_context &io_service,
                                             pubsub::GcsPublisher &gcs_publisher)
    : PubSubHandlerBase(io_service, gcs_publisher.GetPublisher()) {}

ObservabilityPubSubHandler::ObservabilityPubSubHandler(
    instrumented_io_context &io_service,
    pubsub::ObservabilityPublisher &observability_publisher)
    : PubSubHandlerBase(io_service, observability_publisher.GetPublisher()),
      observability_publisher_(observability_publisher) {}

void ObservabilityPubSubHandler::HandleReportJobError(
    rpc::ReportJobErrorRequest request,
    rpc::ReportJobErrorReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const auto &job_id_binary = request.job_error().job_id();
  if (job_id_binary.size() != JobID::Size()) {
    send_reply_callback(Status::InvalidArgument(absl::StrFormat(
                            "Invalid job_id: expected length %zu bytes, got %zu",
                            JobID::Size(),
                            job_id_binary.size())),
                        nullptr,
                        nullptr);
    return;
  }
  const auto job_id = JobID::FromBinary(job_id_binary);
  observability_publisher_.PublishError(job_id.Hex(),
                                        std::move(*request.mutable_job_error()));
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

}  // namespace gcs
}  // namespace ray
