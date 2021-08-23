// Copyright 2020 The Ray Authors.
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

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "ray/gcs/gcs_client.h"
#include "ray/rpc/object_manager/object_manager_client.h"

using absl::optional;
using std::shared_ptr;

namespace ray {

using ObjectManagerClientFactoryFn =
    std::function<std::shared_ptr<rpc::ObjectManagerClient>(const rpc::Address &)>;

class IObjectManagerClientPool {
 public:
  virtual ~IObjectManagerClientPool() {}

  /// Return an existing ObjectManagerClient if exists, and connect to one if it does
  /// not. The returned pointer is borrowed, and expected to be used briefly.
  virtual optional<shared_ptr<rpc::ObjectManagerClient>> GetOrConnectByID(
      ray::NodeID id) = 0;

  /// Return an existing ObjectManagerClient if exists, and connect to one if it does
  /// not. The returned pointer is borrowed, and expected to be used briefly.
  virtual shared_ptr<rpc::ObjectManagerClient> GetOrConnectByAddress(
      const rpc::Address &address) = 0;

  /// Removes a connection to the worker from the pool, if one exists. Since the
  /// shared pointer will no longer be retained in the pool, the connection will
  /// be open until it's no longer used, at which time it will disconnect.
  virtual void Disconnect(ray::NodeID id) = 0;

  /// Get all available object manager clients in this cluster.
  virtual std::vector<shared_ptr<rpc::ObjectManagerClient>>
  GetAllObjectManagerClients() = 0;
};

class ObjectManagerClientPool : public IObjectManagerClientPool {
 public:
  optional<shared_ptr<rpc::ObjectManagerClient>> GetOrConnectByID(
      ray::NodeID id) override;

  shared_ptr<rpc::ObjectManagerClient> GetOrConnectByAddress(
      const rpc::Address &address) override;

  void Disconnect(ray::NodeID id) override;

  std::vector<shared_ptr<rpc::ObjectManagerClient>> GetAllObjectManagerClients() override;

  ObjectManagerClientPool(rpc::ClientCallManager &ccm, gcs::GcsClient *gcs_client)
      : client_factory_(defaultClientFactory(ccm)), gcs_client_(gcs_client){};

  virtual ~ObjectManagerClientPool() {}

  RAY_DISALLOW_COPY_AND_ASSIGN(ObjectManagerClientPool);

 private:
  /// Provides the default client factory function. Providing this function to the
  /// construtor aids migration but is ultimately a thing that should be
  /// deprecated and brought internal to the pool, so this is our bridge.
  ObjectManagerClientFactoryFn defaultClientFactory(rpc::ClientCallManager &ccm) const {
    return [&](const rpc::Address &addr) {
      auto object_manager_client =
          std::make_shared<rpc::ObjectManagerClient>(addr.ip_address(), addr.port(), ccm);
      return object_manager_client;
    };
  };

  /// This factory function does the connection to ObjectManagerClient, and is
  /// provided by the constructor (either the default implementation, above, or a
  /// provided one)
  ObjectManagerClientFactoryFn client_factory_;

  /// A client connection to the GCS.
  gcs::GcsClient *gcs_client_;

  absl::Mutex mu_;

  /// A pool of open connections by host:port. Clients can reuse the connection
  /// objects in this pool by requesting them
  absl::flat_hash_map<ray::NodeID, shared_ptr<rpc::ObjectManagerClient>> client_map_
      GUARDED_BY(mu_);
};

}  // namespace ray
