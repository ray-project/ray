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

#include <list>
#include <memory>
#include <utility>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "ray/core_worker_rpc_client/core_worker_client_interface.h"
#include "ray/gcs_rpc_client/gcs_client.h"
#include "ray/raylet_rpc_client/raylet_client_interface.h"
#include "ray/raylet_rpc_client/raylet_client_pool.h"

namespace ray {
namespace rpc {
using CoreWorkerClientFactoryFn =
    std::function<std::shared_ptr<CoreWorkerClientInterface>(const rpc::Address &)>;
class CoreWorkerClientPool {
 public:
  CoreWorkerClientPool() = delete;

  /// Creates a CoreWorkerClientPool by a given connection function.
  explicit CoreWorkerClientPool(CoreWorkerClientFactoryFn client_factory)
      : core_worker_client_factory_(std::move(client_factory)){};

  /// Default unavailable_timeout_callback for retryable rpc's used by client factories on
  /// core worker.
  static std::function<void()> GetDefaultUnavailableTimeoutCallback(
      gcs::GcsClient *gcs_client,
      rpc::CoreWorkerClientPool *worker_client_pool,
      rpc::RayletClientPool *raylet_client_pool,
      const rpc::Address &addr);

  /// Returns an open CoreWorkerClientInterface if one exists, and connect to one
  /// if it does not. The returned pointer is expected to be used
  /// briefly.
  std::shared_ptr<CoreWorkerClientInterface> GetOrConnect(const Address &addr_proto);

  /// Removes a connection to the worker from the pool, if one exists. Since the
  /// shared pointer will no longer be retained in the pool, the connection will
  /// be open until it's no longer used, at which time it will disconnect.
  void Disconnect(const WorkerID &id);

  /// Removes connections to all workers on a node.
  void Disconnect(const NodeID &node_id);

 private:
  friend void AssertID(WorkerID worker_id,
                       CoreWorkerClientPool &client_pool,
                       bool contains);

  /// Try to remove some idle clients to free memory.
  /// It doesn't go through the entire list and remove all idle clients.
  /// Instead, it tries to remove idle clients from the end of the list
  /// and stops when it finds the first non-idle client.
  /// However, it's guaranteed that all idle clients will eventually be
  /// removed as long as the method will be called repeatedly.
  void RemoveIdleClients() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Erases a single entry from node_clients_map_.
  void EraseFromNodeClientMap(const NodeID &node_id, const WorkerID &worker_id)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// This factory function does the connection to CoreWorkerClient, and is
  /// provided by the constructor (either the default implementation, above, or a
  /// provided one)
  CoreWorkerClientFactoryFn core_worker_client_factory_;

  absl::Mutex mu_;

  struct CoreWorkerClientEntry {
   public:
    CoreWorkerClientEntry() = default;
    CoreWorkerClientEntry(WorkerID worker_id,
                          NodeID node_id,
                          std::shared_ptr<CoreWorkerClientInterface> core_worker_client)
        : worker_id_(std::move(worker_id)),
          node_id_(std::move(node_id)),
          core_worker_client_(std::move(core_worker_client)) {}

    WorkerID worker_id_;
    NodeID node_id_;
    std::shared_ptr<CoreWorkerClientInterface> core_worker_client_;
  };

  /// A list of open connections from the most recent accessed to the least recent
  /// accessed. This is used to check and remove idle connections.
  std::list<CoreWorkerClientEntry> client_list_ ABSL_GUARDED_BY(mu_);

  using WorkerIdClientMap =
      absl::flat_hash_map<WorkerID, std::list<CoreWorkerClientEntry>::iterator>;

  /// A pool of open connections by WorkerID. Clients can reuse the connection
  /// objects in this pool by requesting them.
  absl::flat_hash_map<WorkerID, std::list<CoreWorkerClientEntry>::iterator>
      worker_client_map_ ABSL_GUARDED_BY(mu_);

  /// Map from NodeID to map of workerid -> client iterators. Used to disconnect all
  /// workers on a node.
  absl::flat_hash_map<NodeID, WorkerIdClientMap> node_clients_map_ ABSL_GUARDED_BY(mu_);
};

}  // namespace rpc
}  // namespace ray
