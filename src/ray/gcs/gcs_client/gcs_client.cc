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
      req, [callback](const Status &status, rpc::GcsSubscriberPollReply &&poll_reply) {
        rpc::PubsubLongPollingReply reply;
        reply.mutable_pub_messages()->Swap(poll_reply.mutable_pub_messages());
        *reply.mutable_publisher_id() = std::move(*poll_reply.mutable_publisher_id());
        callback(status, std::move(reply));
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
                 rpc::GcsSubscriberCommandBatchReply &&batch_reply) {
        rpc::PubsubCommandBatchReply reply;
        callback(status, std::move(reply));
      });
}

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

GcsClient::GcsClient(const GcsClientOptions &options, UniqueID gcs_client_id)
    : options_(options), gcs_client_id_(gcs_client_id) {}

Status GcsClient::Connect(instrumented_io_context &io_service, int64_t timeout_ms) {
  if (timeout_ms < 0) {
    timeout_ms = RayConfig::instance().gcs_rpc_server_connect_timeout_s() * 1000;
  }
  // Connect to gcs service.
  client_call_manager_ =
      std::make_unique<rpc::ClientCallManager>(io_service, options_.cluster_id_);
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
  runtime_env_accessor_ = std::make_unique<RuntimeEnvAccessor>(this);
  autoscaler_state_accessor_ = std::make_unique<AutoscalerStateAccessor>(this);

  RAY_LOG(DEBUG) << "GcsClient connected " << options_.gcs_address_ << ":"
                 << options_.gcs_port_;

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

  Status s = gcs_rpc_client_->SyncGetClusterId(request, &reply, timeout_ms);
  if (!s.ok()) {
    RAY_LOG(WARNING) << "Failed to get cluster ID from GCS server: " << s;
    gcs_rpc_client_->Shutdown();
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
    gcs_rpc_client_->Shutdown();
  }
}

std::pair<std::string, int> GcsClient::GetGcsServerAddress() const {
  return gcs_rpc_client_->GetAddress();
}

ClusterID GcsClient::GetClusterId() const {
  ClusterID cluster_id = client_call_manager_->GetClusterId();
  return cluster_id;
}

PythonGcsClient::PythonGcsClient(const GcsClientOptions &options)
    : options_(options), cluster_id_(options.cluster_id_) {}

namespace {
Status HandleGcsError(rpc::GcsStatus status) {
  RAY_CHECK_NE(status.code(), static_cast<int>(StatusCode::OK));
  return Status::Invalid(status.message() +
                         " [GCS status code: " + std::to_string(status.code()) + "]");
}
}  // namespace

Status PythonGcsClient::Connect(int64_t timeout_ms, size_t num_retries) {
  absl::WriterMutexLock lock(&mutex_);
  channel_ =
      rpc::GcsRpcClient::CreateGcsChannel(options_.gcs_address_, options_.gcs_port_);
  node_info_stub_ = rpc::NodeInfoGcsService::NewStub(channel_);
  // cluster_id_ may already be fetched from a last Connect() call.
  if (cluster_id_.IsNil() && options_.should_fetch_cluster_id_) {
    size_t tries = num_retries + 1;
    RAY_CHECK(tries > 0) << "Expected positive retries, but got " << tries;

    RAY_LOG(DEBUG) << "Retrieving cluster ID from GCS server.";
    rpc::GetClusterIdRequest request;
    rpc::GetClusterIdReply reply;

    Status connect_status;
    for (; tries > 0; tries--) {
      grpc::ClientContext context;
      PrepareContext(context, timeout_ms);
      connect_status =
          GrpcStatusToRayStatus(node_info_stub_->GetClusterId(&context, request, &reply));

      // On RpcError: retry
      // On GCS side error: return error
      // On success: set cluster_id_ and break
      if (connect_status.ok()) {
        if (reply.status().code() == static_cast<int>(StatusCode::OK)) {
          cluster_id_ = ClusterID::FromBinary(reply.cluster_id());
          RAY_LOG(DEBUG) << "Received cluster ID from GCS server: " << cluster_id_;
          RAY_CHECK(!cluster_id_.IsNil());
          break;
        } else {
          return HandleGcsError(reply.status());
        }
      } else if (!connect_status.IsRpcError()) {
        return connect_status;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
      channel_ =
          rpc::GcsRpcClient::CreateGcsChannel(options_.gcs_address_, options_.gcs_port_);
      node_info_stub_ = rpc::NodeInfoGcsService::NewStub(channel_);
    }
    RAY_RETURN_NOT_OK(connect_status);
  }

  RAY_CHECK(!cluster_id_.IsNil()) << "Unexpected nil cluster ID.";
  kv_stub_ = rpc::InternalKVGcsService::NewStub(channel_);
  runtime_env_stub_ = rpc::RuntimeEnvGcsService::NewStub(channel_);
  job_info_stub_ = rpc::JobInfoGcsService::NewStub(channel_);
  node_resource_info_stub_ = rpc::NodeResourceInfoGcsService::NewStub(channel_);
  autoscaler_stub_ = rpc::autoscaler::AutoscalerStateService::NewStub(channel_);
  return Status::OK();
}

Status PythonGcsClient::CheckAlive(const std::vector<std::string> &raylet_addresses,
                                   int64_t timeout_ms,
                                   std::vector<bool> &result) {
  grpc::ClientContext context;
  PrepareContext(context, timeout_ms);

  rpc::CheckAliveRequest request;
  for (const auto &address : raylet_addresses) {
    request.add_raylet_address(address);
  }

  absl::ReaderMutexLock lock(&mutex_);
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

  rpc::InternalKVGetRequest request;
  request.set_namespace_(ns);
  request.set_key(key);

  absl::ReaderMutexLock lock(&mutex_);
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

  rpc::InternalKVMultiGetRequest request;
  request.set_namespace_(ns);
  request.mutable_keys()->Add(keys.begin(), keys.end());

  absl::ReaderMutexLock lock(&mutex_);
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

  rpc::InternalKVPutRequest request;
  request.set_namespace_(ns);
  request.set_key(key);
  request.set_value(value);
  request.set_overwrite(overwrite);

  absl::ReaderMutexLock lock(&mutex_);
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

  rpc::InternalKVDelRequest request;
  request.set_namespace_(ns);
  request.set_key(key);
  request.set_del_by_prefix(del_by_prefix);

  absl::ReaderMutexLock lock(&mutex_);
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

  rpc::InternalKVKeysRequest request;
  request.set_namespace_(ns);
  request.set_prefix(prefix);

  absl::ReaderMutexLock lock(&mutex_);
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

  rpc::InternalKVExistsRequest request;
  request.set_namespace_(ns);
  request.set_key(key);

  absl::ReaderMutexLock lock(&mutex_);
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

  rpc::PinRuntimeEnvURIRequest request;
  request.set_uri(uri);
  request.set_expiration_s(expiration_s);

  absl::ReaderMutexLock lock(&mutex_);
  rpc::PinRuntimeEnvURIReply reply;

  grpc::Status status = runtime_env_stub_->PinRuntimeEnvURI(&context, request, &reply);
  if (status.ok()) {
    if (reply.status().code() == static_cast<int>(StatusCode::OK)) {
      return Status::OK();
    } else {
      return Status(StatusCode(reply.status().code()), reply.status().message());
    }
  }
  return Status::RpcError(status.error_message(), status.error_code());
}

Status PythonGcsClient::GetAllNodeInfo(int64_t timeout_ms,
                                       std::vector<rpc::GcsNodeInfo> &result) {
  grpc::ClientContext context;
  PrepareContext(context, timeout_ms);

  absl::ReaderMutexLock lock(&mutex_);
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

Status PythonGcsClient::GetAllJobInfo(
    const std::optional<std::string> &job_or_submission_id,
    bool skip_submission_job_info_field,
    bool skip_is_running_tasks_field,
    int64_t timeout_ms,
    std::vector<rpc::JobTableData> &result) {
  grpc::ClientContext context;
  PrepareContext(context, timeout_ms);

  absl::ReaderMutexLock lock(&mutex_);
  rpc::GetAllJobInfoRequest request;
  request.set_skip_submission_job_info_field(skip_submission_job_info_field);
  request.set_skip_is_running_tasks_field(skip_is_running_tasks_field);
  if (job_or_submission_id.has_value()) {
    request.set_job_or_submission_id(job_or_submission_id.value());
  }
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

  absl::ReaderMutexLock lock(&mutex_);
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

  absl::ReaderMutexLock lock(&mutex_);
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

Status PythonGcsClient::GetClusterResourceState(int64_t timeout_ms,
                                                std::string &serialized_reply) {
  rpc::autoscaler::GetClusterResourceStateRequest request;
  rpc::autoscaler::GetClusterResourceStateReply reply;
  grpc::ClientContext context;
  PrepareContext(context, timeout_ms);

  absl::ReaderMutexLock lock(&mutex_);
  grpc::Status status =
      autoscaler_stub_->GetClusterResourceState(&context, request, &reply);

  if (status.ok()) {
    if (!reply.SerializeToString(&serialized_reply)) {
      return Status::IOError("Failed to serialize GetClusterResourceState");
    }
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

  absl::ReaderMutexLock lock(&mutex_);
  grpc::Status status = autoscaler_stub_->GetClusterStatus(&context, request, &reply);

  if (status.ok()) {
    if (!reply.SerializeToString(&serialized_reply)) {
      return Status::IOError("Failed to serialize GetClusterStatusReply");
    }
    return Status::OK();
  }
  return Status::RpcError(status.error_message(), status.error_code());
}

Status PythonGcsClient::ReportAutoscalingState(int64_t timeout_ms,
                                               const std::string &serialized_state) {
  rpc::autoscaler::ReportAutoscalingStateRequest request;
  rpc::autoscaler::ReportAutoscalingStateReply reply;
  rpc::autoscaler::AutoscalingState state;
  grpc::ClientContext context;
  PrepareContext(context, timeout_ms);

  absl::ReaderMutexLock lock(&mutex_);
  if (!state.ParseFromString(serialized_state)) {
    return Status::IOError("Failed to parse ReportAutoscalingState");
  }
  request.mutable_autoscaling_state()->CopyFrom(state);
  grpc::Status status =
      autoscaler_stub_->ReportAutoscalingState(&context, request, &reply);

  if (status.ok()) {
    return Status::OK();
  }
  return Status::RpcError(status.error_message(), status.error_code());
}

Status PythonGcsClient::DrainNode(const std::string &node_id,
                                  int32_t reason,
                                  const std::string &reason_message,
                                  int64_t deadline_timestamp_ms,
                                  int64_t timeout_ms,
                                  bool &is_accepted,
                                  std::string &rejection_reason_message) {
  rpc::autoscaler::DrainNodeRequest request;
  request.set_node_id(NodeID::FromHex(node_id).Binary());
  request.set_reason(static_cast<rpc::autoscaler::DrainNodeReason>(reason));
  request.set_reason_message(reason_message);
  request.set_deadline_timestamp_ms(deadline_timestamp_ms);

  rpc::autoscaler::DrainNodeReply reply;

  grpc::ClientContext context;
  PrepareContext(context, timeout_ms);

  absl::ReaderMutexLock lock(&mutex_);
  grpc::Status status = autoscaler_stub_->DrainNode(&context, request, &reply);

  if (status.ok()) {
    is_accepted = reply.is_accepted();
    if (!is_accepted) {
      rejection_reason_message = reply.rejection_reason_message();
    }
    return Status::OK();
  }
  return GrpcStatusToRayStatus(status);
}

Status PythonGcsClient::DrainNodes(const std::vector<std::string> &node_ids,
                                   int64_t timeout_ms,
                                   std::vector<std::string> &drained_node_ids) {
  grpc::ClientContext context;
  PrepareContext(context, timeout_ms);

  rpc::DrainNodeRequest request;
  for (const std::string &node_id : node_ids) {
    request.add_drain_node_data()->set_node_id(node_id);
  }

  absl::ReaderMutexLock lock(&mutex_);
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

/// Creates a singleton thread that runs an io_service.
/// All ConnectToGcsStandalone calls will share this io_service.
class SingletonIoContext {
 public:
  static SingletonIoContext &Instance() {
    static SingletonIoContext instance;
    return instance;
  }

  instrumented_io_context &GetIoService() { return io_service_; }

 private:
  SingletonIoContext() : work_(io_service_) {
    io_thread_ = std::thread([this] {
      SetThreadName("singleton_io_context.gcs_client");
      io_service_.run();
    });
  }
  ~SingletonIoContext() {
    io_service_.stop();
    if (io_thread_.joinable()) {
      io_thread_.join();
    }
  }

  instrumented_io_context io_service_;
  boost::asio::io_service::work work_;  // to keep io_service_ running
  std::thread io_thread_;
};

Status ConnectOnSingletonIoContext(GcsClient &gcs_client, int64_t timeout_ms) {
  instrumented_io_context &io_service = SingletonIoContext::Instance().GetIoService();
  return gcs_client.Connect(io_service, timeout_ms);
}

}  // namespace gcs
}  // namespace ray
