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

#pragma once
#include <memory>

#include "ray/gcs_rpc_client/gcs_client_context.h"
#include "ray/gcs_rpc_client/rpc_client.h"
#include "ray/pubsub/gcs_subscriber.h"

namespace ray {
namespace gcs {

class DefaultGcsClientContext : public GcsClientContext {
 public:
  DefaultGcsClientContext() = default;
  ~DefaultGcsClientContext() override = default;

  /**
   Get the GCS subscriber for pubsub operations.
  */
  pubsub::GcsSubscriber &GetGcsSubscriber() override;

  /**
   Get the GCS RPC client for making RPC calls.
  */
  rpc::GcsRpcClient &GetGcsRpcClient() override;

  /**
   Check if the RPC client has been initialized
  */
  bool IsInitialized() const override;

  /**
   Set the GCS RPC client for making RPC calls.
  */
  void SetGcsRpcClient(std::shared_ptr<rpc::GcsRpcClient> client) override;

  /**
   Set the GCS subscriber for pubsub operations.
  */
  void SetGcsSubscriber(std::unique_ptr<pubsub::GcsSubscriber> subscriber) override;

  void Disconnect() override;

 private:
  std::shared_ptr<rpc::GcsRpcClient> client_ = nullptr;
  std::unique_ptr<pubsub::GcsSubscriber> subscriber_ = nullptr;
};

}  // namespace gcs
}  // namespace ray
