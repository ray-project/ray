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
#include "ray/rpc/worker/core_worker_client.h"

namespace ray {
namespace rpc {

class CoreWorkerClientPool {
 public:
  CoreWorkerClientPool() = delete;

  /// Creates a CoreWorkerClientPool by a given connection function.
  explicit CoreWorkerClientPool(CoreWorkerClientFactoryFn client_factory)
      : core_worker_client_factory_(std::move(client_factory)){};

  /// Returns an open CoreWorkerClientInterface if one exists, and connect to one
  /// if it does not. The returned pointer is borrowed, and expected to be used
  /// briefly.
  std::shared_ptr<CoreWorkerClientInterface> GetOrConnect(const Address &addr_proto);

  /// Removes a connection to the worker from the pool, if one exists. Since the
  /// shared pointer will no longer be retained in the pool, the connection will
  /// be open until it's no longer used, at which time it will disconnect.
  void Disconnect(ray::WorkerID id);

  /// For testing.
  size_t Size() {
    absl::MutexLock lock(&mu_);
    RAY_CHECK_EQ(client_list_.size(), client_map_.size());
    return client_list_.size();
  }

 private:
  /// Try to remove some idle clients to free memory.
  /// It doesn't go through the entire list and remove all idle clients.
  /// Instead, it tries to remove idle clients from the end of the list
  /// and stops when it finds the first non-idle client.
  /// However, it's guaranteed that all idle clients will eventually be
  /// removed as long as the method will be called repeatedly.
  void RemoveIdleClients() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// This factory function does the connection to CoreWorkerClient, and is
  /// provided by the constructor (either the default implementation, above, or a
  /// provided one)
  CoreWorkerClientFactoryFn core_worker_client_factory_;

  absl::Mutex mu_;

  struct CoreWorkerClientEntry {
   public:
    CoreWorkerClientEntry() {}
    CoreWorkerClientEntry(ray::WorkerID worker_id,
                          std::shared_ptr<CoreWorkerClientInterface> core_worker_client)
        : worker_id(std::move(worker_id)),
          core_worker_client(std::move(core_worker_client)) {}

    ray::WorkerID worker_id;
    std::shared_ptr<CoreWorkerClientInterface> core_worker_client;
  };

  /// A list of open connections from the most recent accessed to the least recent
  /// accessed. This is used to check and remove idle connections.
  std::list<CoreWorkerClientEntry> client_list_ ABSL_GUARDED_BY(mu_);
  /// A pool of open connections by WorkerID. Clients can reuse the connection
  /// objects in this pool by requesting them.
  absl::flat_hash_map<ray::WorkerID, std::list<CoreWorkerClientEntry>::iterator>
      client_map_ ABSL_GUARDED_BY(mu_);
};

}  // namespace rpc
}  // namespace ray
