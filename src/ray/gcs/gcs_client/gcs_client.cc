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

#include <chrono>
#include <thread>
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
  req.set_max_processed_sequence_id(request.max_processed_sequence_id());
  req.set_publisher_id(request.publisher_id());
  rpc_client_->GcsSubscriberPoll(
      req,
      [callback](const Status &status, const rpc::GcsSubscriberPollReply &poll_reply) {
        rpc::PubsubLongPollingReply reply;
        *reply.mutable_pub_messages() = poll_reply.pub_messages();
        *reply.mutable_publisher_id() = poll_reply.publisher_id();
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

// Waits for the channel to be ready.
// The wait time takes a fraction of the total timeout of an RPC, so we limit out wait
// time to min(rpc total timeout, configured connect timeout).
Status WaitForChannelReady(grpc::Channel &channel, int64_t rpc_timeout_ms) {
  auto timeout_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::seconds(::RayConfig::instance().gcs_rpc_server_connect_timeout_s()));
  if (rpc_timeout_ms > 0) {
    timeout_duration =
        std::min(timeout_duration, std::chrono::milliseconds(rpc_timeout_ms));
  }
  auto deadline = std::chrono::system_clock::now() + timeout_duration;
  if (channel.WaitForConnected(deadline)) {
    return Status::OK();
  }
  return Status::RpcError("GCS is not connected in WaitForChannelReady",
                          grpc::StatusCode::UNAVAILABLE);
}

}  // namespace

GcsClient::GcsClient(const GcsClientOptions &options, UniqueID gcs_client_id)
    : options_(options), gcs_client_id_(gcs_client_id) {}

Status GcsClient::Connect(instrumented_io_context &io_service,
                          const ClusterID &cluster_id) {
  // Connect to gcs service.
  client_call_manager_ = std::make_unique<rpc::ClientCallManager>(io_service, cluster_id);
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

  RAY_LOG(DEBUG) << "GcsClient connected " << options_.gcs_address_ << ":"
                 << options_.gcs_port_;
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

PythonGcsClient::PythonGcsClient(const GcsClientOptions &options) : options_(options) {}

namespace {
Status HandleGcsError(rpc::GcsStatus status) {
  RAY_CHECK_NE(status.code(), static_cast<int>(StatusCode::OK));
  return Status::Invalid(status.message() +
                         " [GCS status code: " + std::to_string(status.code()) + "]");
}
}  // namespace

Status PythonGcsClient::Connect(const ClusterID &cluster_id,
                                int64_t timeout_ms,
                                size_t num_retries) {
  channel_ =
      rpc::GcsRpcClient::CreateGcsChannel(options_.gcs_address_, options_.gcs_port_);
  kv_stub_ = rpc::InternalKVGcsService::NewStub(channel_);
  runtime_env_stub_ = rpc::RuntimeEnvGcsService::NewStub(channel_);
  node_info_stub_ = rpc::NodeInfoGcsService::NewStub(channel_);
  job_info_stub_ = rpc::JobInfoGcsService::NewStub(channel_);
  node_resource_info_stub_ = rpc::NodeResourceInfoGcsService::NewStub(channel_);
  autoscaler_stub_ = rpc::autoscaler::AutoscalerStateService::NewStub(channel_);
  if (cluster_id.IsNil()) {
    size_t tries = num_retries + 1;
    RAY_CHECK(tries > 0) << "Expected positive retries, but got " << tries;

    RAY_LOG(DEBUG) << "Retrieving cluster ID from GCS server.";
    rpc::GetClusterIdRequest request;
    rpc::GetClusterIdReply reply;

    Status connect_status;
    for (; tries > 0; tries--) {
      grpc::ClientContext context;
      PrepareContext(context, timeout_ms);
      RAY_RETURN_NOT_OK(WaitForChannelReady(*channel_, timeout_ms));
      connect_status =
          GrpcStatusToRayStatus(node_info_stub_->GetClusterId(&context, request, &reply));

      if (connect_status.ok()) {
        cluster_id_ = ClusterID::FromBinary(reply.cluster_id());
        RAY_LOG(DEBUG) << "Received cluster ID from GCS server: " << cluster_id_;
        RAY_CHECK(!cluster_id_.IsNil());
        break;
      } else if (!connect_status.IsGrpcError()) {
        return HandleGcsError(reply.status());
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    RAY_RETURN_NOT_OK(connect_status);
  } else {
    cluster_id_ = cluster_id;
    RAY_LOG(DEBUG) << "Client initialized with provided cluster ID: " << cluster_id_;
  }

  RAY_CHECK(!cluster_id_.IsNil()) << "Unexpected nil cluster ID.";
  return Status::OK();
}

Status PythonGcsClient::CheckAlive(const std::vector<std::string> &raylet_addresses,
                                   int64_t timeout_ms,
                                   std::vector<bool> &result) {
  grpc::ClientContext context;
  PrepareContext(context, timeout_ms);
  RAY_RETURN_NOT_OK(WaitForChannelReady(*channel_, timeout_ms));

  rpc::CheckAliveRequest request;
  for (const auto &address : raylet_addresses) {
    request.add_raylet_address(address);
  }

  rpc::CheckAliveReply reply;
  grpc::Status status = node_info_stub_->CheckAlive(&context, request, &reply);

  if (status.ok()) {
    if (reply.status().code() == static_cast<int>(StatusCode::OK)) {
      result =
          std::vector<bool>(reply.raylet_alive().begin(), reply.raylet_alive().end());
      return Status::OK();
    }
    return HandleGcsError(reply.status());
  }
  return Status::RpcError(status.error_message(), status.error_code());
}

Status PythonGcsClient::InternalKVGet(const std::string &ns,
                                      const std::string &key,
                                      int64_t timeout_ms,
                                      std::string &value) {
  grpc::ClientContext context;
  PrepareContext(context, timeout_ms);
  RAY_RETURN_NOT_OK(WaitForChannelReady(*channel_, timeout_ms));

  rpc::InternalKVGetRequest request;
  request.set_namespace_(ns);
  request.set_key(key);

  rpc::InternalKVGetReply reply;

  grpc::Status status = kv_stub_->InternalKVGet(&context, request, &reply);
  if (status.ok()) {
    if (reply.status().code() == static_cast<int>(StatusCode::OK)) {
      value = reply.value();
      return Status::OK();
    } else if (reply.status().code() == static_cast<int>(StatusCode::NotFound)) {
      return Status::KeyError(key);
    }
    return HandleGcsError(reply.status());
  }
  return Status::RpcError(status.error_message(), status.error_code());
}

Status PythonGcsClient::InternalKVMultiGet(
    const std::string &ns,
    const std::vector<std::string> &keys,
    int64_t timeout_ms,
    std::unordered_map<std::string, std::string> &result) {
  grpc::ClientContext context;
  PrepareContext(context, timeout_ms);
  RAY_RETURN_NOT_OK(WaitForChannelReady(*channel_, timeout_ms));

  rpc::InternalKVMultiGetRequest request;
  request.set_namespace_(ns);
  request.mutable_keys()->Add(keys.begin(), keys.end());

  rpc::InternalKVMultiGetReply reply;

  grpc::Status status = kv_stub_->InternalKVMultiGet(&context, request, &reply);
  if (status.ok()) {
    result.clear();
    if (reply.status().code() == static_cast<int>(StatusCode::OK)) {
      for (const auto &entry : reply.results()) {
        result[entry.key()] = entry.value();
      }
      return Status::OK();
    } else if (reply.status().code() == static_cast<int>(StatusCode::NotFound)) {
      // result has already been cleared above
      return Status::OK();
    }
    return HandleGcsError(reply.status());
  }
  return Status::RpcError(status.error_message(), status.error_code());
}

Status PythonGcsClient::InternalKVPut(const std::string &ns,
                                      const std::string &key,
                                      const std::string &value,
                                      bool overwrite,
                                      int64_t timeout_ms,
                                      int &added_num) {
  grpc::ClientContext context;
  PrepareContext(context, timeout_ms);
  RAY_RETURN_NOT_OK(WaitForChannelReady(*channel_, timeout_ms));

  rpc::InternalKVPutRequest request;
  request.set_namespace_(ns);
  request.set_key(key);
  request.set_value(value);
  request.set_overwrite(overwrite);

  rpc::InternalKVPutReply reply;

  grpc::Status status = kv_stub_->InternalKVPut(&context, request, &reply);
  if (status.ok()) {
    if (reply.status().code() == static_cast<int>(StatusCode::OK)) {
      added_num = reply.added_num();
      return Status::OK();
    }
    return HandleGcsError(reply.status());
  }
  return Status::RpcError(status.error_message(), status.error_code());
}

Status PythonGcsClient::InternalKVDel(const std::string &ns,
                                      const std::string &key,
                                      bool del_by_prefix,
                                      int64_t timeout_ms,
                                      int &deleted_num) {
  grpc::ClientContext context;
  PrepareContext(context, timeout_ms);
  RAY_RETURN_NOT_OK(WaitForChannelReady(*channel_, timeout_ms));

  rpc::InternalKVDelRequest request;
  request.set_namespace_(ns);
  request.set_key(key);
  request.set_del_by_prefix(del_by_prefix);

  rpc::InternalKVDelReply reply;

  grpc::Status status = kv_stub_->InternalKVDel(&context, request, &reply);
  if (status.ok()) {
    if (reply.status().code() == static_cast<int>(StatusCode::OK)) {
      deleted_num = reply.deleted_num();
      return Status::OK();
    }
    return HandleGcsError(reply.status());
  }
  return Status::RpcError(status.error_message(), status.error_code());
}

Status PythonGcsClient::InternalKVKeys(const std::string &ns,
                                       const std::string &prefix,
                                       int64_t timeout_ms,
                                       std::vector<std::string> &results) {
  grpc::ClientContext context;
  PrepareContext(context, timeout_ms);
  RAY_RETURN_NOT_OK(WaitForChannelReady(*channel_, timeout_ms));

  rpc::InternalKVKeysRequest request;
  request.set_namespace_(ns);
  request.set_prefix(prefix);

  rpc::InternalKVKeysReply reply;

  grpc::Status status = kv_stub_->InternalKVKeys(&context, request, &reply);
  if (status.ok()) {
    if (reply.status().code() == static_cast<int>(StatusCode::OK)) {
      results = std::vector<std::string>(reply.results().begin(), reply.results().end());
      return Status::OK();
    }
    return HandleGcsError(reply.status());
  }
  return Status::RpcError(status.error_message(), status.error_code());
}

Status PythonGcsClient::InternalKVExists(const std::string &ns,
                                         const std::string &key,
                                         int64_t timeout_ms,
                                         bool &exists) {
  grpc::ClientContext context;
  PrepareContext(context, timeout_ms);
  RAY_RETURN_NOT_OK(WaitForChannelReady(*channel_, timeout_ms));

  rpc::InternalKVExistsRequest request;
  request.set_namespace_(ns);
  request.set_key(key);

  rpc::InternalKVExistsReply reply;

  grpc::Status status = kv_stub_->InternalKVExists(&context, request, &reply);
  if (status.ok()) {
    if (reply.status().code() == static_cast<int>(StatusCode::OK)) {
      exists = reply.exists();
      return Status::OK();
    }
    return HandleGcsError(reply.status());
  }
  return Status::RpcError(status.error_message(), status.error_code());
}

Status PythonGcsClient::PinRuntimeEnvUri(const std::string &uri,
                                         int expiration_s,
                                         int64_t timeout_ms) {
  grpc::ClientContext context;
  PrepareContext(context, timeout_ms);
  RAY_RETURN_NOT_OK(WaitForChannelReady(*channel_, timeout_ms));

  rpc::PinRuntimeEnvURIRequest request;
  request.set_uri(uri);
  request.set_expiration_s(expiration_s);

  rpc::PinRuntimeEnvURIReply reply;

  grpc::Status status = runtime_env_stub_->PinRuntimeEnvURI(&context, request, &reply);
  if (status.ok()) {
    if (reply.status().code() == static_cast<int>(StatusCode::OK)) {
      return Status::OK();
    } else if (reply.status().code() == static_cast<int>(StatusCode::GrpcUnavailable)) {
      std::string msg =
          "Failed to pin URI reference for " + uri + " due to the GCS being " +
          "unavailable, most likely it has crashed: " + reply.status().message() + ".";
      return Status::GrpcUnavailable(msg);
    }
    std::string msg = "Failed to pin URI reference for " + uri +
                      " due to unexpected error " + reply.status().message() + ".";
    return Status::GrpcUnknown(msg);
  }
  return Status::RpcError(status.error_message(), status.error_code());
}

Status PythonGcsClient::GetAllNodeInfo(int64_t timeout_ms,
                                       std::vector<rpc::GcsNodeInfo> &result) {
  grpc::ClientContext context;
  PrepareContext(context, timeout_ms);
  RAY_RETURN_NOT_OK(WaitForChannelReady(*channel_, timeout_ms));

  rpc::GetAllNodeInfoRequest request;
  rpc::GetAllNodeInfoReply reply;

  grpc::Status status = node_info_stub_->GetAllNodeInfo(&context, request, &reply);
  if (status.ok()) {
    if (reply.status().code() == static_cast<int>(StatusCode::OK)) {
      result = std::vector<rpc::GcsNodeInfo>(reply.node_info_list().begin(),
                                             reply.node_info_list().end());
      return Status::OK();
    }
    return HandleGcsError(reply.status());
  }
  return Status::RpcError(status.error_message(), status.error_code());
}

Status PythonGcsClient::GetAllJobInfo(int64_t timeout_ms,
                                      std::vector<rpc::JobTableData> &result) {
  grpc::ClientContext context;
  PrepareContext(context, timeout_ms);
  RAY_RETURN_NOT_OK(WaitForChannelReady(*channel_, timeout_ms));

  rpc::GetAllJobInfoRequest request;
  rpc::GetAllJobInfoReply reply;

  grpc::Status status = job_info_stub_->GetAllJobInfo(&context, request, &reply);
  if (status.ok()) {
    if (reply.status().code() == static_cast<int>(StatusCode::OK)) {
      result = std::vector<rpc::JobTableData>(reply.job_info_list().begin(),
                                              reply.job_info_list().end());
      return Status::OK();
    }
    return HandleGcsError(reply.status());
  }
  return Status::RpcError(status.error_message(), status.error_code());
}

Status PythonGcsClient::GetAllResourceUsage(int64_t timeout_ms,
                                            std::string &serialized_reply) {
  grpc::ClientContext context;
  PrepareContext(context, timeout_ms);
  RAY_RETURN_NOT_OK(WaitForChannelReady(*channel_, timeout_ms));

  rpc::GetAllResourceUsageRequest request;
  rpc::GetAllResourceUsageReply reply;

  grpc::Status status =
      node_resource_info_stub_->GetAllResourceUsage(&context, request, &reply);
  if (status.ok()) {
    if (reply.status().code() == static_cast<int>(StatusCode::OK)) {
      serialized_reply = reply.SerializeAsString();
      return Status::OK();
    }
    return HandleGcsError(reply.status());
  }
  return Status::RpcError(status.error_message(), status.error_code());
}

Status PythonGcsClient::RequestClusterResourceConstraint(
    int64_t timeout_ms,
    const std::vector<std::unordered_map<std::string, double>> &bundles,
    const std::vector<int64_t> &count_array) {
  grpc::ClientContext context;
  PrepareContext(context, timeout_ms);
  RAY_RETURN_NOT_OK(WaitForChannelReady(*channel_, timeout_ms));

  rpc::autoscaler::RequestClusterResourceConstraintRequest request;
  rpc::autoscaler::RequestClusterResourceConstraintReply reply;
  RAY_CHECK(bundles.size() == count_array.size());
  for (size_t i = 0; i < bundles.size(); ++i) {
    const auto &bundle = bundles[i];
    auto count = count_array[i];

    auto new_resource_requests_by_count =
        request.mutable_cluster_resource_constraint()->add_min_bundles();

    new_resource_requests_by_count->mutable_request()->mutable_resources_bundle()->insert(
        bundle.begin(), bundle.end());
    new_resource_requests_by_count->set_count(count);
  }

  grpc::Status status =
      autoscaler_stub_->RequestClusterResourceConstraint(&context, request, &reply);

  if (status.ok()) {
    return Status::OK();
  }
  return Status::RpcError(status.error_message(), status.error_code());
}

Status PythonGcsClient::GetClusterStatus(int64_t timeout_ms,
                                         std::string &serialized_reply) {
  rpc::autoscaler::GetClusterStatusRequest request;
  rpc::autoscaler::GetClusterStatusReply reply;
  grpc::ClientContext context;
  PrepareContext(context, timeout_ms);
  RAY_RETURN_NOT_OK(WaitForChannelReady(*channel_, timeout_ms));

  grpc::Status status = autoscaler_stub_->GetClusterStatus(&context, request, &reply);

  if (status.ok()) {
    if (!reply.SerializeToString(&serialized_reply)) {
      return Status::IOError("Failed to serialize GetClusterStatusReply");
    }
    return Status::OK();
  }
  return Status::RpcError(status.error_message(), status.error_code());
}

Status PythonGcsClient::DrainNode(const std::string &node_id,
                                  int32_t reason,
                                  const std::string &reason_message,
                                  int64_t timeout_ms,
                                  bool &is_accepted) {
  rpc::autoscaler::DrainNodeRequest request;
  request.set_node_id(NodeID::FromHex(node_id).Binary());
  request.set_reason(static_cast<rpc::autoscaler::DrainNodeReason>(reason));
  request.set_reason_message(reason_message);

  rpc::autoscaler::DrainNodeReply reply;

  grpc::ClientContext context;
  PrepareContext(context, timeout_ms);
  RAY_RETURN_NOT_OK(WaitForChannelReady(*channel_, timeout_ms));

  grpc::Status status = autoscaler_stub_->DrainNode(&context, request, &reply);

  if (status.ok()) {
    is_accepted = reply.is_accepted();
    return Status::OK();
  }
  return Status::RpcError(status.error_message(), status.error_code());
}

Status PythonGcsClient::DrainNodes(const std::vector<std::string> &node_ids,
                                   int64_t timeout_ms,
                                   std::vector<std::string> &drained_node_ids) {
  grpc::ClientContext context;
  PrepareContext(context, timeout_ms);
  RAY_RETURN_NOT_OK(WaitForChannelReady(*channel_, timeout_ms));

  rpc::DrainNodeRequest request;
  for (const std::string &node_id : node_ids) {
    request.add_drain_node_data()->set_node_id(node_id);
  }

  rpc::DrainNodeReply reply;

  grpc::Status status = node_info_stub_->DrainNode(&context, request, &reply);
  if (status.ok()) {
    if (reply.status().code() == static_cast<int>(StatusCode::OK)) {
      drained_node_ids.clear();
      drained_node_ids.reserve(reply.drain_node_status().size());
      for (const auto &node_status : reply.drain_node_status()) {
        drained_node_ids.push_back(node_status.node_id());
      }
      return Status::OK();
    }
    return HandleGcsError(reply.status());
  }
  return Status::RpcError(status.error_message(), status.error_code());
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

Status PythonCheckGcsHealth(const std::string &gcs_address,
                            const int gcs_port,
                            const int64_t timeout_ms,
                            const std::string &ray_version,
                            const bool skip_version_check,
                            bool &is_healthy) {
  auto channel = rpc::GcsRpcClient::CreateGcsChannel(gcs_address, gcs_port);
  auto stub = rpc::NodeInfoGcsService::NewStub(channel);
  grpc::ClientContext context;
  if (timeout_ms != -1) {
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(timeout_ms));
  }
  rpc::CheckAliveRequest request;
  rpc::CheckAliveReply reply;
  grpc::Status status = stub->CheckAlive(&context, request, &reply);
  if (!status.ok()) {
    is_healthy = false;
    return Status::RpcError(status.error_message(), status.error_code());
  }
  if (reply.status().code() != static_cast<int>(StatusCode::OK)) {
    is_healthy = false;
    return HandleGcsError(reply.status());
  }
  if (!skip_version_check) {
    // Check for Ray version match
    if (reply.ray_version() != ray_version) {
      is_healthy = false;
      std::ostringstream ss;
      ss << "Ray cluster at " << gcs_address << ":" << gcs_port << " has version "
         << reply.ray_version() << ", but this process"
         << "is running Ray version " << ray_version << ".";
      return Status::Invalid(ss.str());
    }
  }
  is_healthy = true;
  return Status::OK();
}

}  // namespace gcs
}  // namespace ray
