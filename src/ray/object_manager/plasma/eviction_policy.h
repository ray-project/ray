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

// ==== The eviction policy ====
//
// This file contains declaration for all functions and data structures that
// need to be provided if you want to implement a new eviction algorithm for the
// Plasma store.

#pragma once

#include <functional>
#include <list>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "ray/object_manager/plasma/common.h"
#include "ray/object_manager/plasma/lifecycle_event_subscriber.h"
#include "ray/object_manager/plasma/lifecycle_meta_store.h"
#include "ray/object_manager/plasma/object_store.h"
#include "ray/object_manager/plasma/plasma.h"
#include "ray/object_manager/plasma/plasma_allocator.h"

namespace plasma {

/// The eviction policy interface.
class IEvictionPolicy : public ILifecycleEventSubscriber {
 public:
  virtual ~IEvictionPolicy() = default;

  /// This method will be called when the Plasma store needs more space, perhaps
  /// to create a new object. When this method is called, the eviction
  /// policy will assume that the objects chosen to be evicted will in fact be
  /// evicted from the Plasma store by the caller.
  ///
  /// \param size The size in bytes of the new object, including both data and
  ///        metadata.
  /// \param objects_to_evict The object IDs that were chosen for eviction will
  ///        be stored into this vector.
  /// \return The number of bytes of space that is still needed, if
  /// any. If negative, then the required space has been made.
  virtual int64_t RequireSpace(int64_t size, std::vector<ObjectID> &objects_to_evict) = 0;

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
  virtual int64_t ChooseObjectsToEvict(int64_t num_bytes_required,
                                       std::vector<ObjectID> &objects_to_evict) = 0;

  /// Returns debugging information for this eviction policy.
  virtual std::string DebugString() const = 0;
};

class LRUCache {
 public:
  LRUCache(const std::string &name, int64_t size)
      : name_(name),
        original_capacity_(size),
        capacity_(size),
        used_capacity_(0),
        num_evictions_total_(0),
        bytes_evicted_total_(0) {}

  void Add(const ObjectID &key, int64_t size);

  int64_t Remove(const ObjectID &key);

  int64_t ChooseObjectsToEvict(int64_t num_bytes_required,
                               std::vector<ObjectID> &objects_to_evict);

  int64_t OriginalCapacity() const;

  int64_t Capacity() const;

  int64_t RemainingCapacity() const;

  void AdjustCapacity(int64_t delta);

  void Foreach(std::function<void(const ObjectID &)>);

  bool Exists(const ObjectID &key) const;

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

/// The eviction policy implementation
class EvictionPolicy : public IEvictionPolicy {
 public:
  EvictionPolicy(const IObjectStore &object_store, const IAllocator &allocator,
                 const LifecycleMetadataStore &meta_store);

  void OnObjectCreated(const ray::ObjectID &id) override;

  void OnObjectSealed(const ray::ObjectID &id) override;

  void OnObjectDeleting(const ray::ObjectID &id) override;

  void OnObjectRefIncreased(const ray::ObjectID &id) override;

  void OnObjectRefDecreased(const ray::ObjectID &id) override;

  int64_t RequireSpace(int64_t size, std::vector<ObjectID> &objects_to_evict) override;

  int64_t ChooseObjectsToEvict(int64_t num_bytes_required,
                               std::vector<ObjectID> &objects_to_evict) override;

  std::string DebugString() const override;

 private:
  friend struct EvictionPolicyTest;

  /// Returns the size of the object
  int64_t GetObjectSize(const ObjectID &object_id) const;

  /// Returns whether the object exist in cache or not
  bool IsObjectEvictable(const ObjectID &object_id) const;

  /// The number of bytes pinned by applications.
  int64_t pinned_memory_bytes_;

  /// Datastructure for the LRU cache.
  LRUCache cache_;

  const IObjectStore &object_store_;
  const IAllocator &allocator_;
  const LifecycleMetadataStore &meta_store_;
};

}  // namespace plasma
