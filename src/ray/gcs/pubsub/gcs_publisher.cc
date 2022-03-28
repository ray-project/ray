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

#include "ray/gcs/pubsub/gcs_publisher.h"

namespace ray {
namespace gcs {

Status GrpcBasedGcsPublisher::PublishActor(const ActorID &id,
                                           const rpc::ActorTableData &message,
                                           const StatusCallback &done) {
  rpc::PubMessage msg;
  msg.set_channel_type(rpc::ChannelType::GCS_ACTOR_CHANNEL);
  msg.set_key_id(id.Binary());
  *msg.mutable_actor_message() = message;
  publisher_->Publish(msg);
  if (done != nullptr) {
    done(Status::OK());
  }
  return Status::OK();
}

Status GrpcBasedGcsPublisher::PublishJob(const JobID &id,
                                         const rpc::JobTableData &message,
                                         const StatusCallback &done) {
  rpc::PubMessage msg;
  msg.set_channel_type(rpc::ChannelType::GCS_JOB_CHANNEL);
  msg.set_key_id(id.Binary());
  *msg.mutable_job_message() = message;
  publisher_->Publish(msg);
  if (done != nullptr) {
    done(Status::OK());
  }
  return Status::OK();
}

Status GrpcBasedGcsPublisher::PublishNodeInfo(const NodeID &id,
                                              const rpc::GcsNodeInfo &message,
                                              const StatusCallback &done) {
  rpc::PubMessage msg;
  msg.set_channel_type(rpc::ChannelType::GCS_NODE_INFO_CHANNEL);
  msg.set_key_id(id.Binary());
  *msg.mutable_node_info_message() = message;
  publisher_->Publish(msg);
  if (done != nullptr) {
    done(Status::OK());
  }
  return Status::OK();
}

Status GrpcBasedGcsPublisher::PublishNodeResource(const NodeID &id,
                                                  const rpc::NodeResourceChange &message,
                                                  const StatusCallback &done) {
  rpc::PubMessage msg;
  msg.set_channel_type(rpc::ChannelType::GCS_NODE_RESOURCE_CHANNEL);
  msg.set_key_id(id.Binary());
  *msg.mutable_node_resource_message() = message;
  publisher_->Publish(msg);
  if (done != nullptr) {
    done(Status::OK());
  }
  return Status::OK();
}

Status GrpcBasedGcsPublisher::PublishWorkerFailure(const WorkerID &id,
                                                   const rpc::WorkerDeltaData &message,
                                                   const StatusCallback &done) {
  rpc::PubMessage msg;
  msg.set_channel_type(rpc::ChannelType::GCS_WORKER_DELTA_CHANNEL);
  msg.set_key_id(id.Binary());
  *msg.mutable_worker_delta_message() = message;
  publisher_->Publish(msg);
  if (done != nullptr) {
    done(Status::OK());
  }
  return Status::OK();
}

Status GrpcBasedGcsPublisher::PublishError(const std::string &id,
                                           const rpc::ErrorTableData &message,
                                           const StatusCallback &done) {
  rpc::PubMessage msg;
  msg.set_channel_type(rpc::ChannelType::RAY_ERROR_INFO_CHANNEL);
  msg.set_key_id(id);
  *msg.mutable_error_info_message() = message;
  publisher_->Publish(msg);
  if (done != nullptr) {
    done(Status::OK());
  }
  return Status::OK();
}

std::string GrpcBasedGcsPublisher::DebugString() const { return "GcsPublisher {}"; }

Status RedisBasedGcsPublisher::PublishActor(const ActorID &id,
                                            const rpc::ActorTableData &message,
                                            const StatusCallback &done) {
  return pubsub_->Publish(ACTOR_CHANNEL, id.Hex(), message.SerializeAsString(), done);
}

Status RedisBasedGcsPublisher::PublishJob(const JobID &id,
                                          const rpc::JobTableData &message,
                                          const StatusCallback &done) {
  return pubsub_->Publish(JOB_CHANNEL, id.Hex(), message.SerializeAsString(), done);
}

Status RedisBasedGcsPublisher::PublishNodeInfo(const NodeID &id,
                                               const rpc::GcsNodeInfo &message,
                                               const StatusCallback &done) {
  return pubsub_->Publish(NODE_CHANNEL, id.Hex(), message.SerializeAsString(), done);
}

Status RedisBasedGcsPublisher::PublishNodeResource(const NodeID &id,
                                                   const rpc::NodeResourceChange &message,
                                                   const StatusCallback &done) {
  return pubsub_->Publish(
      NODE_RESOURCE_CHANNEL, id.Hex(), message.SerializeAsString(), done);
}

Status RedisBasedGcsPublisher::PublishWorkerFailure(const WorkerID &id,
                                                    const rpc::WorkerDeltaData &message,
                                                    const StatusCallback &done) {
  return pubsub_->Publish(WORKER_CHANNEL, id.Hex(), message.SerializeAsString(), done);
}

Status RedisBasedGcsPublisher::PublishError(const std::string &id,
                                            const rpc::ErrorTableData &message,
                                            const StatusCallback &done) {
  return pubsub_->Publish(ERROR_INFO_CHANNEL, id, message.SerializeAsString(), done);
}

std::string RedisBasedGcsPublisher::DebugString() const { return pubsub_->DebugString(); }

}  // namespace gcs
}  // namespace ray