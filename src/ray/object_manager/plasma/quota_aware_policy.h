// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <list>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "ray/object_manager/plasma/common.h"
#include "ray/object_manager/plasma/eviction_policy.h"
#include "ray/object_manager/plasma/plasma.h"

namespace plasma {

/// Reserve this fraction of memory for shared usage. Attempts to set client
/// quotas that would cause the global LRU memory fraction to fall below this
/// value will be rejected.
constexpr double kGlobalLruReserveFraction = 0.3;

/// Extends the basic eviction policy to implement per-client memory quotas.
/// This effectively gives each client its own LRU queue, which caps its
/// memory usage and protects this memory from being evicted by other clients.
///
/// The quotas are enforced when objects are first created, by evicting the
/// necessary number of objects from the client's own LRU queue to cap its
/// memory usage. Once that is done, allocation is handled by the normal
/// eviction policy. This may result in the eviction of objects from the
/// global LRU queue, if not enough memory can be allocated even after the
/// evictions from the client's own LRU queue.
///
/// Some special cases:
/// - When a pinned object is "evicted" from a per-client queue, it is
/// instead transferred into the global LRU queue.
/// - When a client disconnects, its LRU queue is merged into the head of the
/// global LRU queue.
class QuotaAwarePolicy : public EvictionPolicy {
 public:
  /// Construct a quota-aware eviction policy.
  ///
  /// \param store_info Information about the Plasma store that is exposed
  ///        to the eviction policy.
  /// \param max_size Max size in bytes total of objects to store.
  explicit QuotaAwarePolicy(PlasmaStoreInfo* store_info, int64_t max_size);
  void ObjectCreated(const ObjectID& object_id, Client* client, bool is_create) override;
  bool SetClientQuota(Client* client, int64_t output_memory_quota) override;
  bool EnforcePerClientQuota(Client* client, int64_t size, bool is_create,
                             std::vector<ObjectID>* objects_to_evict) override;
  void ClientDisconnected(Client* client) override;
  void BeginObjectAccess(const ObjectID& object_id) override;
  void EndObjectAccess(const ObjectID& object_id) override;
  void RemoveObject(const ObjectID& object_id) override;
  void RefreshObjects(const std::vector<ObjectID>& object_ids) override;
  std::string DebugString() const override;

 private:
  /// Returns whether we are enforcing memory quotas for an operation.
  bool HasQuota(Client* client, bool is_create);

  /// Per-client LRU caches, if quota is enabled.
  std::unordered_map<Client*, std::unique_ptr<LRUCache>> per_client_cache_;
  /// Tracks which client created which object. This only applies to clients
  /// that have a memory quota set.
  std::unordered_map<ObjectID, Client*> owned_by_client_;
  /// Tracks which objects are mapped for read and hence can't be evicted.
  /// However these objects are still tracked within the client caches.
  std::unordered_set<ObjectID> shared_for_read_;
};

}  // namespace plasma
