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

#include "ray/gcs/gcs_client/gcs_client.h"

#include <utility>

#include "ray/common/ray_config.h"
#include "ray/gcs/gcs_client/accessor.h"
#include "ray/pubsub/subscriber.h"

namespace ray {
namespace gcs {
namespace {

/// Adapts GcsRpcClient to SubscriberClientInterface for making RPC calls. Thread safe.
class GcsSubscriberClient final : public pubsub::SubscriberClientInterface {
 public:
  explicit GcsSubscriberClient(const std::shared_ptr<rpc::GcsRpcClient> &rpc_client)
      : rpc_client_(rpc_client) {}

  ~GcsSubscriberClient() final = default;

  void PubsubLongPolling(
      const rpc::PubsubLongPollingRequest &request,
      const rpc::ClientCallback<rpc::PubsubLongPollingReply> &callback) final;

  void PubsubCommandBatch(
      const rpc::PubsubCommandBatchRequest &request,
      const rpc::ClientCallback<rpc::PubsubCommandBatchReply> &callback) final;

 private:
  const std::shared_ptr<rpc::GcsRpcClient> rpc_client_;
};

void GcsSubscriberClient::PubsubLongPolling(
    const rpc::PubsubLongPollingRequest &request,
    const rpc::ClientCallback<rpc::PubsubLongPollingReply> &callback) {
  rpc::GcsSubscriberPollRequest req;
  req.set_subscriber_id(request.subscriber_id());
  rpc_client_->GcsSubscriberPoll(
      req,
      [callback](const Status &status, const rpc::GcsSubscriberPollReply &poll_reply) {
        rpc::PubsubLongPollingReply reply;
        *reply.mutable_pub_messages() = poll_reply.pub_messages();
        callback(status, reply);
      });
}

void GcsSubscriberClient::PubsubCommandBatch(
    const rpc::PubsubCommandBatchRequest &request,
    const rpc::ClientCallback<rpc::PubsubCommandBatchReply> &callback) {
  rpc::GcsSubscriberCommandBatchRequest req;
  req.set_subscriber_id(request.subscriber_id());
  *req.mutable_commands() = request.commands();
  rpc_client_->GcsSubscriberCommandBatch(
      req,
      [callback](const Status &status,
                 const rpc::GcsSubscriberCommandBatchReply &batch_reply) {
        rpc::PubsubCommandBatchReply reply;
        callback(status, reply);
      });
}

}  // namespace

GcsClient::GcsClient(const GcsClientOptions &options) : options_(options) {}

Status GcsClient::Connect(instrumented_io_context &io_service) {
  // Connect to gcs service.
  client_call_manager_ = std::make_unique<rpc::ClientCallManager>(io_service);
  gcs_rpc_client_ = std::make_shared<rpc::GcsRpcClient>(
      options_.gcs_address_, options_.gcs_port_, *client_call_manager_);

  resubscribe_func_ = [this]() {
    job_accessor_->AsyncResubscribe();
    actor_accessor_->AsyncResubscribe();
    node_accessor_->AsyncResubscribe();
    node_resource_accessor_->AsyncResubscribe();
    worker_accessor_->AsyncResubscribe();
  };

  rpc::Address gcs_address;
  gcs_address.set_ip_address(options_.gcs_address_);
  gcs_address.set_port(options_.gcs_port_);
  /// TODO(mwtian): refactor pubsub::Subscriber to avoid faking worker ID.
  gcs_address.set_worker_id(UniqueID::FromRandom().Binary());

  auto subscriber = std::make_unique<pubsub::Subscriber>(
      /*subscriber_id=*/gcs_client_id_,
      /*channels=*/
      std::vector<rpc::ChannelType>{rpc::ChannelType::GCS_ACTOR_CHANNEL,
                                    rpc::ChannelType::GCS_JOB_CHANNEL,
                                    rpc::ChannelType::GCS_NODE_INFO_CHANNEL,
                                    rpc::ChannelType::GCS_WORKER_DELTA_CHANNEL},
      /*max_command_batch_size*/ RayConfig::instance().max_command_batch_size(),
      /*get_client=*/
      [this](const rpc::Address &) {
        return std::make_shared<GcsSubscriberClient>(gcs_rpc_client_);
      },
      /*callback_service*/ &io_service);

  // Init GCS subscriber instance.
  gcs_subscriber_ = std::make_unique<GcsSubscriber>(gcs_address, std::move(subscriber));

  job_accessor_ = std::make_unique<JobInfoAccessor>(this);
  actor_accessor_ = std::make_unique<ActorInfoAccessor>(this);
  node_accessor_ = std::make_unique<NodeInfoAccessor>(this);
  node_resource_accessor_ = std::make_unique<NodeResourceInfoAccessor>(this);
  error_accessor_ = std::make_unique<ErrorInfoAccessor>(this);
  worker_accessor_ = std::make_unique<WorkerInfoAccessor>(this);
  placement_group_accessor_ = std::make_unique<PlacementGroupInfoAccessor>(this);
  internal_kv_accessor_ = std::make_unique<InternalKVAccessor>(this);
  task_accessor_ = std::make_unique<TaskInfoAccessor>(this);

  RAY_LOG(DEBUG) << "GcsClient connected.";
  return Status::OK();
}

void GcsClient::Disconnect() {
  if (gcs_rpc_client_) {
    gcs_rpc_client_->Shutdown();
  }
}

std::pair<std::string, int> GcsClient::GetGcsServerAddress() const {
  return gcs_rpc_client_->GetAddress();
}

}  // namespace gcs
}  // namespace ray
