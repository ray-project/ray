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

grpc::ChannelArguments PythonGrpcChannelArguments() {
  grpc::ChannelArguments arguments;
  arguments.SetInt(GRPC_ARG_MAX_MESSAGE_LENGTH, 512 * 1024 * 1024);
  arguments.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 60 * 1000);
  arguments.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 60 * 1000);
  return arguments;
}

PythonGcsPublisher::PythonGcsPublisher(const std::string &gcs_address) {
  std::vector<std::string> address = absl::StrSplit(gcs_address, ':');
  RAY_LOG(DEBUG) << "Connect to gcs server via address: " << gcs_address;
  RAY_CHECK(address.size() == 2);
  gcs_address_ = address[0];
  gcs_port_ = std::stoi(address[1]);
}

Status PythonGcsPublisher::Connect() {
  auto arguments = PythonGrpcChannelArguments();
  channel_ = rpc::BuildChannel(gcs_address_, gcs_port_, arguments);
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

Status PythonGcsPublisher::PublishFunctionKey(
    const rpc::PythonFunction &python_function) {
  rpc::GcsPublishRequest request;
  auto *message = request.add_pub_messages();
  message->set_channel_type(rpc::RAY_PYTHON_FUNCTION_CHANNEL);
  message->mutable_python_function_message()->MergeFrom(python_function);
  return DoPublishWithRetries(request, -1, -1);
}

}  // namespace gcs
}  // namespace ray
