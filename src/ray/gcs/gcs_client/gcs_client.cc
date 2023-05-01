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

}  // namespace

GcsClient::GcsClient(const GcsClientOptions &options, UniqueID gcs_client_id)
    : options_(options), gcs_client_id_(gcs_client_id) {}

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

PythonGcsClient::PythonGcsClient(const GcsClientOptions &options) : options_(options) {}

Status PythonGcsClient::Connect() {
  grpc::ChannelArguments arguments;
  arguments.SetInt(GRPC_ARG_MAX_MESSAGE_LENGTH, 512 * 1024 * 1024);
  arguments.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 60 * 1000);
  arguments.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 60 * 1000);
  channel_ = rpc::BuildChannel(options_.gcs_address_, options_.gcs_port_, arguments);
  kv_stub_ = rpc::InternalKVGcsService::NewStub(channel_);
  runtime_env_stub_ = rpc::RuntimeEnvGcsService::NewStub(channel_);
  node_info_stub_ = rpc::NodeInfoGcsService::NewStub(channel_);
  job_info_stub_ = rpc::JobInfoGcsService::NewStub(channel_);
  return Status::OK();
}

Status HandleGcsError(rpc::GcsStatus status) {
  RAY_CHECK(status.code() != static_cast<int>(StatusCode::OK));
  return Status::Invalid(status.message() +
                         " [GCS status code: " + std::to_string(status.code()) + "]");
}

void GrpcClientContextWithTimeoutMs(grpc::ClientContext &context, int64_t timeout_ms) {
  if (timeout_ms != -1) {
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(timeout_ms));
  }
}

Status PythonGcsClient::InternalKVGet(const std::string &ns,
                                      const std::string &key,
                                      int64_t timeout_ms,
                                      std::string &value) {
  grpc::ClientContext context;
  GrpcClientContextWithTimeoutMs(context, timeout_ms);

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
  GrpcClientContextWithTimeoutMs(context, timeout_ms);

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
  GrpcClientContextWithTimeoutMs(context, timeout_ms);

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
  GrpcClientContextWithTimeoutMs(context, timeout_ms);

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
  GrpcClientContextWithTimeoutMs(context, timeout_ms);

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
  GrpcClientContextWithTimeoutMs(context, timeout_ms);

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
  GrpcClientContextWithTimeoutMs(context, timeout_ms);

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
  GrpcClientContextWithTimeoutMs(context, timeout_ms);

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
  GrpcClientContextWithTimeoutMs(context, timeout_ms);

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

std::unordered_map<std::string, double> PythonGetResourcesTotal(
    const rpc::GcsNodeInfo &node_info) {
  return std::unordered_map<std::string, double>(node_info.resources_total().begin(),
                                                 node_info.resources_total().end());
}

}  // namespace gcs
}  // namespace ray
