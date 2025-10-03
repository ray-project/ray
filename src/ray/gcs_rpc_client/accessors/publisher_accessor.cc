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

#include "ray/gcs_rpc_client/accessors/publisher_accessor.h"

#include "ray/gcs_rpc_client/rpc_client.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

PublisherAccessor::PublisherAccessor(GcsClientContext *context) : context_(context) {}

Status PublisherAccessor::PublishError(std::string key_id,
                                       rpc::ErrorTableData data,
                                       int64_t timeout_ms) {
  rpc::GcsPublishRequest request;
  auto *pub_message = request.add_pub_messages();
  pub_message->set_channel_type(rpc::RAY_ERROR_INFO_CHANNEL);
  pub_message->set_key_id(std::move(key_id));
  *(pub_message->mutable_error_info_message()) = std::move(data);
  rpc::GcsPublishReply reply;
  return context_->GetGcsRpcClient().SyncGcsPublish(
      std::move(request), &reply, timeout_ms);
}

Status PublisherAccessor::PublishLogs(std::string key_id,
                                      rpc::LogBatch data,
                                      int64_t timeout_ms) {
  rpc::GcsPublishRequest request;
  auto *pub_message = request.add_pub_messages();
  pub_message->set_channel_type(rpc::RAY_LOG_CHANNEL);
  pub_message->set_key_id(std::move(key_id));
  *(pub_message->mutable_log_batch_message()) = std::move(data);
  rpc::GcsPublishReply reply;
  return context_->GetGcsRpcClient().SyncGcsPublish(
      std::move(request), &reply, timeout_ms);
}

void PublisherAccessor::AsyncPublishNodeResourceUsage(
    std::string key_id,
    std::string node_resource_usage_json,
    const StatusCallback &done) {
  rpc::GcsPublishRequest request;
  auto *pub_message = request.add_pub_messages();
  pub_message->set_channel_type(rpc::RAY_NODE_RESOURCE_USAGE_CHANNEL);
  pub_message->set_key_id(std::move(key_id));
  pub_message->mutable_node_resource_usage_message()->set_json(
      std::move(node_resource_usage_json));
  context_->GetGcsRpcClient().GcsPublish(
      std::move(request),
      [done](const Status &status, rpc::GcsPublishReply &&reply) { done(status); });
}

}  // namespace gcs
}  // namespace ray
