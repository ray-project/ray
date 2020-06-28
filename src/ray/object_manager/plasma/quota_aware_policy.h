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
#include "ray/object_manager/plasma/plasma.h"

namespace plasma {

// Declare the class here to avoid recursive header import.
class ObjectDirectory;

class LRUCache {
 public:
  LRUCache(const std::string& name, int64_t size)
      : name_(name),
        original_capacity_(size),
        capacity_(size),
        used_capacity_(0),
        num_evictions_total_(0),
        bytes_evicted_total_(0) {}

  void Add(const ObjectID& key, int64_t size);

  int64_t Remove(const ObjectID& key);

  int64_t ChooseObjectsToEvict(int64_t num_bytes_required,
                               std::vector<ObjectID>* objects_to_evict);

  int64_t OriginalCapacity() const;

  int64_t Capacity() const;

  int64_t RemainingCapacity() const;

  void AdjustCapacity(int64_t delta);

  void Foreach(std::function<void(const ObjectID&)>);

  std::string DebugString() const;

 private:
  /// A doubly-linked list containing the items in the cache and
  /// their sizes in LRU order.
  typedef std::list<std::pair<ObjectID, int64_t>> ItemList;
  ItemList item_list_;
  /// A hash table mapping the object ID of an object in the cache to its
  /// location in the doubly linked list item_list_.
  std::unordered_map<ObjectID, ItemList::iterator> item_map_;

  /// The name of this cache, used for debugging purposes only.
  const std::string name_;
  /// The original (max) capacity of this cache in bytes.
  const int64_t original_capacity_;
  /// The current capacity, which must be <= the original capacity.
  int64_t capacity_;
  /// The number of bytes used of the available capacity.
  int64_t used_capacity_;
  /// The number of objects evicted from this cache.
  int64_t num_evictions_total_;
  /// The number of bytes evicted from this cache.
  int64_t bytes_evicted_total_;
};

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
class QuotaAwarePolicy {
 public:
  /// Construct a quota-aware eviction policy.
  ///
  /// \param store_info Information about the Plasma store that is exposed
  ///        to the eviction policy.
  /// \param max_size Max size in bytes total of objects to store.
  explicit QuotaAwarePolicy(ObjectDirectory* store_info, int64_t max_size);

  /// This method will be called when the Plasma store needs more space, perhaps
  /// to create a new object. When this method is called, the eviction
  /// policy will assume that the objects chosen to be evicted will in fact be
  /// evicted from the Plasma store by the caller.
  ///
  /// \param size The size in bytes of the new object, including both data and
  ///        metadata.
  /// \param objects_to_evict The object IDs that were chosen for eviction will
  ///        be stored into this vector.
  /// \return True if enough space can be freed and false otherwise.
  bool RequireSpace(int64_t size, std::vector<ObjectID>* objects_to_evict);

  /// Choose some objects to evict from the Plasma store. When this method is
  /// called, the eviction policy will assume that the objects chosen to be
  /// evicted will in fact be evicted from the Plasma store by the caller.
  ///
  /// @note This method is not part of the API. It is exposed in the header file
  /// only for testing.
  ///
  /// \param num_bytes_required The number of bytes of space to try to free up.
  /// \param objects_to_evict The object IDs that were chosen for eviction will
  ///        be stored into this vector.
  /// \return The total number of bytes of space chosen to be evicted.
  int64_t ChooseObjectsToEvict(int64_t num_bytes_required,
                               std::vector<ObjectID>* objects_to_evict);

  /// This method will be called whenever an object is first created in order to
  /// add it to the LRU cache. This is done so that the first time, the Plasma
  /// store calls begin_object_access, we can remove the object from the LRU
  /// cache.
  ///
  /// \param object_id The object ID of the object that was created.
  /// \param client The pointer to the client.
  /// \param is_create Whether we are creating a new object (vs reading an object).
  void ObjectCreated(const ObjectID& object_id, Client* client, bool is_create);

  /// Set quota for a client.
  ///
  /// \param client The pointer to the client.
  /// \param output_memory_quota Set the quota for this client. This can only be
  ///        called once per client. This is effectively the equivalent of giving
  ///        the client its own LRU cache instance. The memory for this is taken
  ///        out of the capacity of the global LRU cache for the client lifetime.
  ///
  /// \return True if enough space can be reserved for the given client quota.
  bool SetClientQuota(Client* client, int64_t output_memory_quota);

  /// Determine what objects need to be evicted to enforce the given client's quota.
  ///
  /// \param client The pointer to the client creating the object.
  /// \param size The size of the object to create.
  /// \param is_create Whether we are creating a new object (vs reading an object).
  /// \param objects_to_evict The object IDs that were chosen for eviction will
  ///        be stored into this vector.
  ///
  /// \return True if enough space could be freed and false otherwise.
  bool EnforcePerClientQuota(Client* client, int64_t size, bool is_create,
                             std::vector<ObjectID>* objects_to_evict);

  /// Called to clean up any resources allocated by this client. This merges any
  /// per-client LRU queue created by SetClientQuota into the global LRU queue.
  ///
  /// \param client The pointer to the client.
  void ClientDisconnected(Client* client);

  /// This method will be called whenever an unused object in the Plasma store
  /// starts to be used. When this method is called, the eviction policy will
  /// assume that the objects chosen to be evicted will in fact be evicted from
  /// the Plasma store by the caller.
  ///
  /// \param object_id The ID of the object that is now being used.
  void BeginObjectAccess(const ObjectID& object_id);

  /// This method will be called whenever an object in the Plasma store that was
  /// being used is no longer being used. When this method is called, the
  /// eviction policy will assume that the objects chosen to be evicted will in
  /// fact be evicted from the Plasma store by the caller.
  ///
  /// \param object_id The ID of the object that is no longer being used.
  void EndObjectAccess(const ObjectID& object_id);

  /// This method will be called when an object is going to be removed
  ///
  /// \param object_id The ID of the object that is now being used.
  void RemoveObject(const ObjectID& object_id);

  void RefreshObjects(const std::vector<ObjectID>& object_ids);

  /// Returns debugging information for this eviction policy.
  std::string DebugString() const;

 private:
  /// Returns whether we are enforcing memory quotas for an operation.
  bool HasQuota(Client* client, bool is_create);

  /// The number of bytes pinned by applications.
  int64_t pinned_memory_bytes_;

  /// Pointer to the plasma store info.
  ObjectDirectory* store_info_;

  /// Datastructure for the LRU cache.
  LRUCache cache_;

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
