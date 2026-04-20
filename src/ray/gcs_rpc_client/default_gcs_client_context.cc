// Copyright 2025 The Ray Authors.
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

#include "ray/gcs_rpc_client/default_gcs_client_context.h"

#include "ray/gcs_rpc_client/rpc_client.h"
#include "ray/pubsub/gcs_subscriber.h"

namespace ray {
namespace gcs {
pubsub::GcsSubscriber &DefaultGcsClientContext::GetGcsSubscriber() {
  return *subscriber_;
}

rpc::GcsRpcClient &DefaultGcsClientContext::GetGcsRpcClient() { return *client_; }

rpc::ObservabilityPubSubGcsRpcClient &
DefaultGcsClientContext::GetObservabilityPubSubGcsRpcClient() {
  return *observability_pubsub_client_;
}

bool DefaultGcsClientContext::IsInitialized() const { return client_ != nullptr; }

void DefaultGcsClientContext::SetGcsRpcClient(std::shared_ptr<rpc::GcsRpcClient> client) {
  client_ = client;
}

void DefaultGcsClientContext::SetObservabilityPubSubGcsRpcClient(
    std::shared_ptr<rpc::ObservabilityPubSubGcsRpcClient> client) {
  observability_pubsub_client_ = std::move(client);
}
void DefaultGcsClientContext::SetGcsSubscriber(
    std::unique_ptr<pubsub::GcsSubscriber> subscriber) {
  subscriber_ = std::move(subscriber);
}

void DefaultGcsClientContext::Disconnect() {
  if (observability_pubsub_client_) {
    observability_pubsub_client_.reset();
  }
  if (client_) {
    client_.reset();
  }
};

}  // namespace gcs
}  // namespace ray
