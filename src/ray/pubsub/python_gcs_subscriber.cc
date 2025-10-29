// Copyright 2025 The Ray Authors.
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

#include "ray/pubsub/python_gcs_subscriber.h"

#include <grpcpp/grpcpp.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ray/gcs_rpc_client/rpc_client.h"
#include "ray/rpc/authentication/authentication_token_loader.h"

namespace ray {
namespace pubsub {

std::vector<std::string> PythonGetLogBatchLines(rpc::LogBatch log_batch) {
  return std::vector<std::string>(
      std::make_move_iterator(log_batch.mutable_lines()->begin()),
      std::make_move_iterator(log_batch.mutable_lines()->end()));
}

PythonGcsSubscriber::PythonGcsSubscriber(const std::string &gcs_address,
                                         int gcs_port,
                                         rpc::ChannelType channel_type,
                                         std::string subscriber_id,
                                         std::string worker_id)
    : channel_(rpc::GcsRpcClient::CreateGcsChannel(gcs_address, gcs_port)),
      pubsub_stub_(rpc::InternalPubSubGcsService::NewStub(channel_)),
      channel_type_(channel_type),
      subscriber_id_(std::move(subscriber_id)),
      worker_id_(std::move(worker_id)) {}

Status PythonGcsSubscriber::Subscribe() {
  absl::MutexLock lock(&mu_);

  if (closed_) {
    return Status::OK();
  }

  grpc::ClientContext context;
  SetAuthenticationToken(context);

  rpc::GcsSubscriberCommandBatchRequest request;
  request.set_subscriber_id(subscriber_id_);
  request.set_sender_id(worker_id_);
  auto *command = request.add_commands();
  command->set_channel_type(channel_type_);
  command->mutable_subscribe_message();

  rpc::GcsSubscriberCommandBatchReply reply;
  grpc::Status status =
      pubsub_stub_->GcsSubscriberCommandBatch(&context, request, &reply);

  if (status.ok()) {
    return Status::OK();
  } else {
    return Status::RpcError(status.error_message(), status.error_code());
  }
}

Status PythonGcsSubscriber::DoPoll(int64_t timeout_ms, rpc::PubMessage *message) {
  absl::MutexLock lock(&mu_);

  while (queue_.empty()) {
    if (closed_) {
      return Status::OK();
    }
    current_polling_context_ = std::make_shared<grpc::ClientContext>();
    SetAuthenticationToken(*current_polling_context_);
    if (timeout_ms != -1) {
      current_polling_context_->set_deadline(std::chrono::system_clock::now() +
                                             std::chrono::milliseconds(timeout_ms));
    }
    rpc::GcsSubscriberPollRequest request;
    request.set_subscriber_id(subscriber_id_);
    request.set_max_processed_sequence_id(max_processed_sequence_id_);
    request.set_publisher_id(publisher_id_);

    rpc::GcsSubscriberPollReply reply;
    auto context = current_polling_context_;
    // Drop the lock while in RPC
    mu_.Unlock();
    grpc::Status status = pubsub_stub_->GcsSubscriberPoll(context.get(), request, &reply);
    mu_.Lock();

    if (status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED ||
        status.error_code() == grpc::StatusCode::UNAVAILABLE) {
      return Status::OK();
    }
    if (status.error_code() == grpc::StatusCode::CANCELLED) {
      // This channel was shut down via Close()
      return Status::OK();
    }
    if (status.error_code() != grpc::StatusCode::OK) {
      return Status::Invalid(status.error_message());
    }

    if (publisher_id_ != reply.publisher_id()) {
      if (publisher_id_ != "") {
        RAY_LOG(DEBUG) << "Replied publisher_id " << reply.publisher_id()
                       << " different from " << publisher_id_
                       << ", this should only happen"
                       << " during GCS failover.";
      }
      publisher_id_ = reply.publisher_id();
      max_processed_sequence_id_ = 0;
    }
    last_batch_size_ = reply.pub_messages().size();
    for (auto &cur_pub_msg : *reply.mutable_pub_messages()) {
      if (cur_pub_msg.sequence_id() <= max_processed_sequence_id_) {
        RAY_LOG(WARNING) << "Ignoring out of order message " << cur_pub_msg.sequence_id();
        continue;
      }
      max_processed_sequence_id_ = cur_pub_msg.sequence_id();
      if (cur_pub_msg.channel_type() != channel_type_) {
        RAY_LOG(WARNING) << "Ignoring message from unsubscribed channel "
                         << cur_pub_msg.channel_type();
        continue;
      }
      queue_.emplace_back(std::move(cur_pub_msg));
    }
  }

  *message = std::move(queue_.front());
  queue_.pop_front();

  return Status::OK();
}

Status PythonGcsSubscriber::PollError(std::string *key_id,
                                      int64_t timeout_ms,
                                      rpc::ErrorTableData *data) {
  rpc::PubMessage message;
  RAY_RETURN_NOT_OK(DoPoll(timeout_ms, &message));
  *key_id = std::move(*message.mutable_key_id());
  *data = std::move(*message.mutable_error_info_message());
  return Status::OK();
}

Status PythonGcsSubscriber::PollLogs(std::string *key_id,
                                     int64_t timeout_ms,
                                     rpc::LogBatch *data) {
  rpc::PubMessage message;
  RAY_RETURN_NOT_OK(DoPoll(timeout_ms, &message));
  *key_id = std::move(*message.mutable_key_id());
  *data = std::move(*message.mutable_log_batch_message());
  return Status::OK();
}

Status PythonGcsSubscriber::Close() {
  std::shared_ptr<grpc::ClientContext> current_polling_context;
  {
    absl::MutexLock lock(&mu_);
    if (closed_) {
      return Status::OK();
    }
    closed_ = true;
    current_polling_context = current_polling_context_;
  }
  if (current_polling_context) {
    current_polling_context->TryCancel();
  }

  grpc::ClientContext context;
  SetAuthenticationToken(context);

  rpc::GcsSubscriberCommandBatchRequest request;
  request.set_subscriber_id(subscriber_id_);
  auto *command = request.add_commands();
  command->set_channel_type(channel_type_);
  command->mutable_unsubscribe_message();
  rpc::GcsSubscriberCommandBatchReply reply;
  grpc::Status status =
      pubsub_stub_->GcsSubscriberCommandBatch(&context, request, &reply);

  if (!status.ok()) {
    RAY_LOG(WARNING) << "Error while unregistering the subscriber: "
                     << status.error_message() << " [code " << status.error_code() << "]";
  }
  return Status::OK();
}

int64_t PythonGcsSubscriber::last_batch_size() {
  absl::MutexLock lock(&mu_);
  return last_batch_size_;
}

void PythonGcsSubscriber::SetAuthenticationToken(grpc::ClientContext &context) {
  auto auth_token = ray::rpc::AuthenticationTokenLoader::instance().GetToken();
  if (auth_token.has_value() && !auth_token->empty()) {
    auth_token->SetMetadata(context);
  }
}

}  // namespace pubsub
}  // namespace ray
