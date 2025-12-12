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

namespace ray {

namespace pubsub {
class GcsSubscriber;
}

namespace rpc {
class GcsRpcClient;
}

namespace gcs {

/**
@class GcsClientContext
Minimal interface providing access to RPC client and subscriber. This allows accessor
implementations to access GCS services without depending on the full GcsClient class,
breaking circular dependencies.
*/
class GcsClientContext {
 public:
  virtual ~GcsClientContext() = default;

  /**
   Get the GCS subscriber for pubsub operations.
  */
  virtual pubsub::GcsSubscriber &GetGcsSubscriber() = 0;

  /**
   Get the GCS RPC client for making RPC calls.
  */
  virtual rpc::GcsRpcClient &GetGcsRpcClient() = 0;

  /**
   Check if the RPC client has been initialized
  */
  virtual bool IsInitialized() const = 0;

  /**
   Set the GCS RPC client for making RPC calls.
  */
  virtual void SetGcsRpcClient(std::shared_ptr<rpc::GcsRpcClient> client) = 0;
  /**
   Set the GCS subscriber for pubsub operations.
  */
  virtual void SetGcsSubscriber(std::unique_ptr<pubsub::GcsSubscriber> subscriber) = 0;

  virtual void Disconnect() = 0;
};

}  // namespace gcs
}  // namespace ray
