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

#include "ray/pubsub/gcs_publisher.h"

#include <string>
#include <utility>

namespace ray {
namespace pubsub {

void GcsPublisher::PublishActor(const ActorID &id, rpc::ActorTableData message) {
  rpc::PubMessage msg;
  msg.set_channel_type(rpc::ChannelType::GCS_ACTOR_CHANNEL);
  msg.set_key_id(id.Binary());
  *msg.mutable_actor_message() = std::move(message);
  publisher_->Publish(std::move(msg));
}

void GcsPublisher::PublishJob(const JobID &id, rpc::JobTableData message) {
  rpc::PubMessage msg;
  msg.set_channel_type(rpc::ChannelType::GCS_JOB_CHANNEL);
  msg.set_key_id(id.Binary());
  *msg.mutable_job_message() = std::move(message);
  publisher_->Publish(std::move(msg));
}

void GcsPublisher::PublishNodeInfo(const NodeID &id, rpc::GcsNodeInfo message) {
  rpc::PubMessage msg;
  msg.set_channel_type(rpc::ChannelType::GCS_NODE_INFO_CHANNEL);
  msg.set_key_id(id.Binary());
  *msg.mutable_node_info_message() = std::move(message);
  publisher_->Publish(std::move(msg));
}

void GcsPublisher::PublishWorkerFailure(const WorkerID &id,
                                        rpc::WorkerDeltaData message) {
  rpc::PubMessage msg;
  msg.set_channel_type(rpc::ChannelType::GCS_WORKER_DELTA_CHANNEL);
  msg.set_key_id(id.Binary());
  *msg.mutable_worker_delta_message() = std::move(message);
  publisher_->Publish(std::move(msg));
}

void GcsPublisher::PublishError(std::string id, rpc::ErrorTableData message) {
  rpc::PubMessage msg;
  msg.set_channel_type(rpc::ChannelType::RAY_ERROR_INFO_CHANNEL);
  msg.set_key_id(std::move(id));
  *msg.mutable_error_info_message() = std::move(message);
  publisher_->Publish(std::move(msg));
}

std::string GcsPublisher::DebugString() const { return publisher_->DebugString(); }

}  // namespace pubsub
}  // namespace ray
