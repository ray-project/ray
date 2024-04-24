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

#include "absl/strings/str_cat.h"
#include "ray/rpc/gcs_server/gcs_rpc_client.h"
#include "ray/rpc/grpc_client.h"

namespace ray {
namespace gcs {

Status GcsPublisher::PublishActor(const ActorID &id,
                                  const rpc::ActorTableData &message,
                                  const StatusCallback &done) {
  rpc::PubMessage msg;
  msg.set_channel_type(rpc::ChannelType::GCS_ACTOR_CHANNEL);
  msg.set_key_id(id.Binary());
  *msg.mutable_actor_message() = message;
  publisher_->Publish(std::move(msg));
  if (done != nullptr) {
    done(Status::OK());
  }
  return Status::OK();
}

Status GcsPublisher::PublishJob(const JobID &id,
                                const rpc::JobTableData &message,
                                const StatusCallback &done) {
  rpc::PubMessage msg;
  msg.set_channel_type(rpc::ChannelType::GCS_JOB_CHANNEL);
  msg.set_key_id(id.Binary());
  *msg.mutable_job_message() = message;
  publisher_->Publish(std::move(msg));
  if (done != nullptr) {
    done(Status::OK());
  }
  return Status::OK();
}

Status GcsPublisher::PublishNodeInfo(const NodeID &id,
                                     const rpc::GcsNodeInfo &message,
                                     const StatusCallback &done) {
  rpc::PubMessage msg;
  msg.set_channel_type(rpc::ChannelType::GCS_NODE_INFO_CHANNEL);
  msg.set_key_id(id.Binary());
  *msg.mutable_node_info_message() = message;
  publisher_->Publish(std::move(msg));
  if (done != nullptr) {
    done(Status::OK());
  }
  return Status::OK();
}

Status GcsPublisher::PublishWorkerFailure(const WorkerID &id,
                                          const rpc::WorkerDeltaData &message,
                                          const StatusCallback &done) {
  rpc::PubMessage msg;
  msg.set_channel_type(rpc::ChannelType::GCS_WORKER_DELTA_CHANNEL);
  msg.set_key_id(id.Binary());
  *msg.mutable_worker_delta_message() = message;
  publisher_->Publish(std::move(msg));
  if (done != nullptr) {
    done(Status::OK());
  }
  return Status::OK();
}

Status GcsPublisher::PublishError(const std::string &id,
                                  const rpc::ErrorTableData &message,
                                  const StatusCallback &done) {
  rpc::PubMessage msg;
  msg.set_channel_type(rpc::ChannelType::RAY_ERROR_INFO_CHANNEL);
  msg.set_key_id(id);
  *msg.mutable_error_info_message() = message;
  publisher_->Publish(std::move(msg));
  if (done != nullptr) {
    done(Status::OK());
  }
  return Status::OK();
}

std::string GcsPublisher::DebugString() const { return "GcsPublisher {}"; }

Status GcsSubscriber::SubscribeAllJobs(
    const SubscribeCallback<JobID, rpc::JobTableData> &subscribe,
    const StatusCallback &done) {
  // GCS subscriber.
  auto subscribe_item_callback = [subscribe](const rpc::PubMessage &msg) {
    RAY_CHECK(msg.channel_type() == rpc::ChannelType::GCS_JOB_CHANNEL);
    const JobID id = JobID::FromBinary(msg.key_id());
    subscribe(id, msg.job_message());
  };
  auto subscription_failure_callback = [](const std::string &, const Status &status) {
    RAY_LOG(WARNING) << "Subscription to Job channel failed: " << status.ToString();
  };
  // Ignore if the subscription already exists, because the resubscription is intentional.
  RAY_UNUSED(subscriber_->SubscribeChannel(
      std::make_unique<rpc::SubMessage>(),
      rpc::ChannelType::GCS_JOB_CHANNEL,
      gcs_address_,
      [done](Status status) {
        if (done != nullptr) {
          done(status);
        }
      },
      std::move(subscribe_item_callback),
      std::move(subscription_failure_callback)));
  return Status::OK();
}

Status GcsSubscriber::SubscribeActor(
    const ActorID &id,
    const SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
    const StatusCallback &done) {
  // GCS subscriber.
  auto subscription_callback = [id, subscribe](const rpc::PubMessage &msg) {
    RAY_CHECK(msg.channel_type() == rpc::ChannelType::GCS_ACTOR_CHANNEL);
    RAY_CHECK(msg.key_id() == id.Binary());
    subscribe(id, msg.actor_message());
  };
  auto subscription_failure_callback = [id](const std::string &failed_id,
                                            const Status &status) {
    RAY_CHECK(failed_id == id.Binary());
    RAY_LOG(WARNING) << "Subscription to Actor " << id.Hex()
                     << " failed: " << status.ToString();
  };
  // Ignore if the subscription already exists, because the resubscription is intentional.
  RAY_UNUSED(subscriber_->Subscribe(
      std::make_unique<rpc::SubMessage>(),
      rpc::ChannelType::GCS_ACTOR_CHANNEL,
      gcs_address_,
      id.Binary(),
      [done](Status status) {
        if (done != nullptr) {
          done(status);
        }
      },
      std::move(subscription_callback),
      std::move(subscription_failure_callback)));
  return Status::OK();
}

Status GcsSubscriber::UnsubscribeActor(const ActorID &id) {
  subscriber_->Unsubscribe(
      rpc::ChannelType::GCS_ACTOR_CHANNEL, gcs_address_, id.Binary());
  return Status::OK();
}

bool GcsSubscriber::IsActorUnsubscribed(const ActorID &id) {
  return !subscriber_->IsSubscribed(
      rpc::ChannelType::GCS_ACTOR_CHANNEL, gcs_address_, id.Binary());
}

Status GcsSubscriber::SubscribeAllNodeInfo(
    const ItemCallback<rpc::GcsNodeInfo> &subscribe, const StatusCallback &done) {
  // GCS subscriber.
  auto subscribe_item_callback = [subscribe](const rpc::PubMessage &msg) {
    RAY_CHECK(msg.channel_type() == rpc::ChannelType::GCS_NODE_INFO_CHANNEL);
    subscribe(msg.node_info_message());
  };
  auto subscription_failure_callback = [](const std::string &, const Status &status) {
    RAY_LOG(WARNING) << "Subscription to NodeInfo channel failed: " << status.ToString();
  };
  // Ignore if the subscription already exists, because the resubscription is intentional.
  RAY_UNUSED(subscriber_->SubscribeChannel(
      std::make_unique<rpc::SubMessage>(),
      rpc::ChannelType::GCS_NODE_INFO_CHANNEL,
      gcs_address_,
      [done](Status status) {
        if (done != nullptr) {
          done(status);
        }
      },
      std::move(subscribe_item_callback),
      std::move(subscription_failure_callback)));
  return Status::OK();
}

Status GcsSubscriber::SubscribeAllWorkerFailures(
    const ItemCallback<rpc::WorkerDeltaData> &subscribe, const StatusCallback &done) {
  auto subscribe_item_callback = [subscribe](const rpc::PubMessage &msg) {
    RAY_CHECK(msg.channel_type() == rpc::ChannelType::GCS_WORKER_DELTA_CHANNEL);
    subscribe(msg.worker_delta_message());
  };
  auto subscription_failure_callback = [](const std::string &, const Status &status) {
    RAY_LOG(WARNING) << "Subscription to WorkerDelta channel failed: "
                     << status.ToString();
  };
  // Ignore if the subscription already exists, because the resubscription is intentional.
  RAY_UNUSED(subscriber_->SubscribeChannel(
      std::make_unique<rpc::SubMessage>(),
      rpc::ChannelType::GCS_WORKER_DELTA_CHANNEL,
      gcs_address_,
      /*subscribe_done_callback=*/
      [done](Status status) {
        if (done != nullptr) {
          done(status);
        }
      },
      std::move(subscribe_item_callback),
      std::move(subscription_failure_callback)));
  return Status::OK();
}

std::vector<std::string> PythonGetLogBatchLines(const rpc::LogBatch &log_batch) {
  return std::vector<std::string>(log_batch.lines().begin(), log_batch.lines().end());
}

PythonGcsPublisher::PythonGcsPublisher(const std::string &gcs_address) {
  std::vector<std::string> address = absl::StrSplit(gcs_address, ':');
  RAY_LOG(DEBUG) << "Connect to gcs server via address: " << gcs_address;
  RAY_CHECK(address.size() == 2);
  gcs_address_ = address[0];
  gcs_port_ = std::stoi(address[1]);
}

Status PythonGcsPublisher::Connect() {
  channel_ = rpc::GcsRpcClient::CreateGcsChannel(gcs_address_, gcs_port_);
  pubsub_stub_ = rpc::InternalPubSubGcsService::NewStub(channel_);
  return Status::OK();
}

constexpr int MAX_GCS_PUBLISH_RETRIES = 60;

Status PythonGcsPublisher::DoPublishWithRetries(const rpc::GcsPublishRequest &request,
                                                int64_t num_retries,
                                                int64_t timeout_ms) {
  int count = num_retries == -1 ? MAX_GCS_PUBLISH_RETRIES : num_retries;
  rpc::GcsPublishReply reply;
  grpc::Status status;
  while (count > 0) {
    grpc::ClientContext context;
    if (timeout_ms != -1) {
      context.set_deadline(std::chrono::system_clock::now() +
                           std::chrono::milliseconds(timeout_ms));
    }
    status = pubsub_stub_->GcsPublish(&context, request, &reply);
    if (status.error_code() == grpc::StatusCode::OK) {
      if (reply.status().code() != static_cast<int>(StatusCode::OK)) {
        return Status::Invalid(reply.status().message());
      }
      return Status::OK();
    } else if (status.error_code() == grpc::StatusCode::UNAVAILABLE ||
               status.error_code() == grpc::StatusCode::UNKNOWN) {
      // This is the case in which we will retry
      count -= 1;
      std::this_thread::sleep_for(std::chrono::seconds(1));
      continue;
    } else {
      return Status::Invalid(status.error_message());
    }
  }
  return Status::TimedOut("Failed to publish after retries: " + status.error_message());
}

Status PythonGcsPublisher::PublishError(const std::string &key_id,
                                        const rpc::ErrorTableData &error_info,
                                        int64_t num_retries) {
  rpc::GcsPublishRequest request;
  auto *message = request.add_pub_messages();
  message->set_channel_type(rpc::RAY_ERROR_INFO_CHANNEL);
  message->set_key_id(key_id);
  message->mutable_error_info_message()->MergeFrom(error_info);
  return DoPublishWithRetries(request, num_retries, 1000);
}

Status PythonGcsPublisher::PublishLogs(const std::string &key_id,
                                       const rpc::LogBatch &log_batch) {
  rpc::GcsPublishRequest request;
  auto *message = request.add_pub_messages();
  message->set_channel_type(rpc::RAY_LOG_CHANNEL);
  message->set_key_id(key_id);
  message->mutable_log_batch_message()->MergeFrom(log_batch);
  return DoPublishWithRetries(request, -1, -1);
}

PythonGcsSubscriber::PythonGcsSubscriber(const std::string &gcs_address,
                                         int gcs_port,
                                         rpc::ChannelType channel_type,
                                         const std::string &subscriber_id,
                                         const std::string &worker_id)
    : channel_type_(channel_type),
      subscriber_id_(subscriber_id),
      publisher_id_(""),
      worker_id_(worker_id),
      max_processed_sequence_id_(0),
      closed_(false) {
  channel_ = rpc::GcsRpcClient::CreateGcsChannel(gcs_address, gcs_port);
  pubsub_stub_ = rpc::InternalPubSubGcsService::NewStub(channel_);
}

Status PythonGcsSubscriber::Subscribe() {
  absl::MutexLock lock(&mu_);

  if (closed_) {
    return Status::OK();
  }

  grpc::ClientContext context;

  rpc::GcsSubscriberCommandBatchRequest request;
  request.set_subscriber_id(subscriber_id_);
  request.set_sender_id(worker_id_);
  auto *cmd = request.add_commands();
  cmd->set_channel_type(channel_type_);
  cmd->mutable_subscribe_message();

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
    } else if (status.error_code() == grpc::StatusCode::CANCELLED) {
      // This channel was shut down via Close()
      return Status::OK();
    } else if (status.error_code() != grpc::StatusCode::OK) {
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
    for (auto &message : reply.pub_messages()) {
      if (message.sequence_id() <= max_processed_sequence_id_) {
        RAY_LOG(WARNING) << "Ignoring out of order message " << message.sequence_id();
        continue;
      }
      max_processed_sequence_id_ = message.sequence_id();
      if (message.channel_type() != channel_type_) {
        RAY_LOG(WARNING) << "Ignoring message from unsubscribed channel "
                         << message.channel_type();
        continue;
      }
      queue_.emplace_back(std::move(message));
    }
  }

  *message = queue_.front();
  queue_.pop_front();

  return Status::OK();
}

Status PythonGcsSubscriber::PollError(std::string *key_id,
                                      int64_t timeout_ms,
                                      rpc::ErrorTableData *data) {
  rpc::PubMessage message;
  RAY_RETURN_NOT_OK(DoPoll(timeout_ms, &message));
  *key_id = message.key_id();
  *data = message.error_info_message();
  return Status::OK();
}

Status PythonGcsSubscriber::PollLogs(std::string *key_id,
                                     int64_t timeout_ms,
                                     rpc::LogBatch *data) {
  rpc::PubMessage message;
  RAY_RETURN_NOT_OK(DoPoll(timeout_ms, &message));
  *key_id = message.key_id();
  *data = message.log_batch_message();
  return Status::OK();
}

Status PythonGcsSubscriber::PollActor(std::string *key_id,
                                      int64_t timeout_ms,
                                      rpc::ActorTableData *data) {
  rpc::PubMessage message;
  RAY_RETURN_NOT_OK(DoPoll(timeout_ms, &message));
  *key_id = message.key_id();
  *data = message.actor_message();
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

  rpc::GcsSubscriberCommandBatchRequest request;
  request.set_subscriber_id(subscriber_id_);
  request.set_sender_id(worker_id_);
  auto *cmd = request.add_commands();
  cmd->set_channel_type(channel_type_);
  cmd->mutable_unsubscribe_message();

  rpc::GcsSubscriberCommandBatchReply reply;
  grpc::Status status =
      pubsub_stub_->GcsSubscriberCommandBatch(&context, request, &reply);

  if (!status.ok()) {
    RAY_LOG(DEBUG) << "Error while closing the subscriber: " << status.error_message()
                   << " [code " << status.error_code() << "]";
  }
  return Status::OK();
}

int64_t PythonGcsSubscriber::last_batch_size() {
  absl::MutexLock lock(&mu_);
  return last_batch_size_;
}

}  // namespace gcs
}  // namespace ray
