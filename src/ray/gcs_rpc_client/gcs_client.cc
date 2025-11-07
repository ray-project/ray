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

#include "ray/gcs_rpc_client/gcs_client.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "ray/common/asio/asio_util.h"
#include "ray/common/ray_config.h"
#include "ray/gcs_rpc_client/accessor.h"
#include "ray/pubsub/subscriber.h"
#include "ray/util/network_util.h"

namespace ray {
namespace gcs {
namespace {

/// Adapts GcsRpcClient to SubscriberClientInterface for making RPC calls. Thread safe.
class GcsSubscriberClient final : public pubsub::SubscriberClientInterface {
 public:
  explicit GcsSubscriberClient(const std::shared_ptr<rpc::GcsRpcClient> &rpc_client)
      : rpc_client_(rpc_client) {}

  void PubsubLongPolling(
      rpc::PubsubLongPollingRequest &&request,
      const rpc::ClientCallback<rpc::PubsubLongPollingReply> &callback) final {
    rpc::GcsSubscriberPollRequest req;
    req.set_subscriber_id(std::move(*request.mutable_subscriber_id()));
    req.set_max_processed_sequence_id(request.max_processed_sequence_id());
    req.set_publisher_id(std::move(*request.mutable_publisher_id()));
    rpc_client_->GcsSubscriberPoll(
        std::move(req),
        [callback](const Status &status, rpc::GcsSubscriberPollReply &&poll_reply) {
          rpc::PubsubLongPollingReply reply;
          reply.mutable_pub_messages()->Swap(poll_reply.mutable_pub_messages());
          *reply.mutable_publisher_id() = std::move(*poll_reply.mutable_publisher_id());
          callback(status, std::move(reply));
        });
  }

  void PubsubCommandBatch(
      rpc::PubsubCommandBatchRequest &&request,
      const rpc::ClientCallback<rpc::PubsubCommandBatchReply> &callback) final {
    rpc::GcsSubscriberCommandBatchRequest req;
    req.set_subscriber_id(std::move(*request.mutable_subscriber_id()));
    *req.mutable_commands() = std::move(*request.mutable_commands());
    rpc_client_->GcsSubscriberCommandBatch(
        std::move(req),
        [callback](const Status &status,
                   rpc::GcsSubscriberCommandBatchReply &&batch_reply) {
          rpc::PubsubCommandBatchReply reply;
          callback(status, std::move(reply));
        });
  }

 private:
  const std::shared_ptr<rpc::GcsRpcClient> rpc_client_;
};

}  // namespace

bool GcsClientOptions::ShouldFetchClusterId(ClusterID cluster_id,
                                            bool allow_cluster_id_nil,
                                            bool fetch_cluster_id_if_nil) {
  RAY_CHECK(!((!allow_cluster_id_nil) && fetch_cluster_id_if_nil))
      << " invalid config combination: if allow_cluster_id_nil == false, "
         "fetch_cluster_id_if_nil "
         "must false";
  if (!cluster_id.IsNil()) {
    // ClusterID non nil is always good.
    return false;
  }
  RAY_CHECK(allow_cluster_id_nil) << "Unexpected nil Cluster ID.";
  if (fetch_cluster_id_if_nil) {
    return true;
  } else {
    RAY_LOG(INFO) << "GcsClient has no Cluster ID set, and won't fetch from GCS.";
    return false;
  }
}

GcsClient::GcsClient(GcsClientOptions options,
                     std::string local_address,
                     UniqueID gcs_client_id)
    : options_(std::move(options)),
      gcs_client_id_(gcs_client_id),
      local_address_(std::move(local_address)) {}

Status GcsClient::Connect(instrumented_io_context &io_service, int64_t timeout_ms) {
  if (timeout_ms < 0) {
    timeout_ms = RayConfig::instance().gcs_rpc_server_connect_timeout_s() * 1000;
  }
  // Connect to gcs service.
  client_call_manager_ = std::make_unique<rpc::ClientCallManager>(io_service,
                                                                  /*record_stats=*/false,
                                                                  local_address_,
                                                                  options_.cluster_id_);
  gcs_rpc_client_ = std::make_shared<rpc::GcsRpcClient>(
      options_.gcs_address_, options_.gcs_port_, *client_call_manager_);

  resubscribe_func_ = [this]() {
    RAY_LOG(INFO) << "Resubscribing to GCS tables.";
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
      std::vector<rpc::ChannelType>{
          rpc::ChannelType::GCS_ACTOR_CHANNEL,
          rpc::ChannelType::GCS_JOB_CHANNEL,
          rpc::ChannelType::GCS_NODE_INFO_CHANNEL,
          rpc::ChannelType::GCS_NODE_ADDRESS_AND_LIVENESS_CHANNEL,
          rpc::ChannelType::GCS_WORKER_DELTA_CHANNEL},
      /*max_command_batch_size*/ RayConfig::instance().max_command_batch_size(),
      /*get_client=*/
      [this](const rpc::Address &) {
        return std::make_shared<GcsSubscriberClient>(gcs_rpc_client_);
      },
      /*callback_service*/ &io_service);

  // Init GCS subscriber instance.
  gcs_subscriber_ =
      std::make_unique<pubsub::GcsSubscriber>(gcs_address, std::move(subscriber));

  job_accessor_ = std::make_unique<JobInfoAccessor>(this);
  actor_accessor_ = std::make_unique<ActorInfoAccessor>(this);
  node_accessor_ = std::make_unique<NodeInfoAccessor>(this);
  node_resource_accessor_ = std::make_unique<NodeResourceInfoAccessor>(this);
  error_accessor_ = std::make_unique<ErrorInfoAccessor>(this);
  worker_accessor_ = std::make_unique<WorkerInfoAccessor>(this);
  placement_group_accessor_ = std::make_unique<PlacementGroupInfoAccessor>(this);
  internal_kv_accessor_ = std::make_unique<InternalKVAccessor>(this);
  task_accessor_ = std::make_unique<TaskInfoAccessor>(this);
  runtime_env_accessor_ = std::make_unique<RuntimeEnvAccessor>(this);
  autoscaler_state_accessor_ = std::make_unique<AutoscalerStateAccessor>(this);
  publisher_accessor_ = std::make_unique<PublisherAccessor>(this);

  RAY_LOG(DEBUG) << "GcsClient connected "
                 << BuildAddress(options_.gcs_address_, options_.gcs_port_);

  if (options_.should_fetch_cluster_id_) {
    RAY_RETURN_NOT_OK(FetchClusterId(timeout_ms));
  }
  return Status::OK();
}

Status GcsClient::FetchClusterId(int64_t timeout_ms) {
  if (!GetClusterId().IsNil()) {
    return Status::OK();
  }
  rpc::GetClusterIdRequest request;
  rpc::GetClusterIdReply reply;
  RAY_LOG(DEBUG) << "Cluster ID is nil, getting cluster ID from GCS server.";

  Status s = gcs_rpc_client_->SyncGetClusterId(std::move(request), &reply, timeout_ms);
  if (!s.ok()) {
    RAY_LOG(WARNING) << "Failed to get cluster ID from GCS server: " << s;
    gcs_rpc_client_.reset();
    client_call_manager_.reset();
    return s;
  }
  const auto reply_cluster_id = ClusterID::FromBinary(reply.cluster_id());
  RAY_LOG(DEBUG) << "Retrieved cluster ID from GCS server: " << reply_cluster_id;
  client_call_manager_->SetClusterId(reply_cluster_id);
  return Status::OK();
}

void GcsClient::Disconnect() {
  if (gcs_rpc_client_) {
    gcs_rpc_client_.reset();
  }
}

std::pair<std::string, int> GcsClient::GetGcsServerAddress() const {
  return gcs_rpc_client_->GetAddress();
}

ClusterID GcsClient::GetClusterId() const {
  ClusterID cluster_id = client_call_manager_->GetClusterId();
  return cluster_id;
}

std::unordered_map<std::string, double> PythonGetResourcesTotal(
    const rpc::GcsNodeInfo &node_info) {
  return std::unordered_map<std::string, double>(node_info.resources_total().begin(),
                                                 node_info.resources_total().end());
}

std::unordered_map<std::string, std::string> PythonGetNodeLabels(
    const rpc::GcsNodeInfo &node_info) {
  return std::unordered_map<std::string, std::string>(node_info.labels().begin(),
                                                      node_info.labels().end());
}

Status ConnectOnSingletonIoContext(GcsClient &gcs_client, int64_t timeout_ms) {
  static InstrumentedIOContextWithThread io_context("gcs_client_io_service");
  instrumented_io_context &io_service = io_context.GetIoService();
  return gcs_client.Connect(io_service, timeout_ms);
}

}  // namespace gcs
}  // namespace ray
