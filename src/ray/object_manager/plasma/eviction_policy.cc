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

#include "ray/object_manager/plasma/eviction_policy.h"

#include <algorithm>
#include <sstream>

#include "ray/object_manager/plasma/plasma_allocator.h"

namespace plasma {

void LRUCache::Add(const ObjectID &key, int64_t size) {
  auto it = item_map_.find(key);
  RAY_CHECK(it == item_map_.end());
  // Note that it is important to use a list so the iterators stay valid.
  item_list_.emplace_front(key, size);
  item_map_.emplace(key, item_list_.begin());
  used_capacity_ += size;
}

int64_t LRUCache::Remove(const ObjectID &key) {
  auto it = item_map_.find(key);
  if (it == item_map_.end()) {
    return -1;
  }
  int64_t size = it->second->second;
  used_capacity_ -= size;
  item_list_.erase(it->second);
  item_map_.erase(it);
  RAY_CHECK(used_capacity_ >= 0) << DebugString();
  return size;
}

void LRUCache::AdjustCapacity(int64_t delta) {
  RAY_LOG(INFO) << "adjusting global lru capacity from " << Capacity() << " to "
                << (Capacity() + delta) << " (max " << OriginalCapacity() << ")";
  capacity_ += delta;
  RAY_CHECK(used_capacity_ >= 0) << DebugString();
}

int64_t LRUCache::Capacity() const { return capacity_; }

int64_t LRUCache::OriginalCapacity() const { return original_capacity_; }

int64_t LRUCache::RemainingCapacity() const { return capacity_ - used_capacity_; }

void LRUCache::Foreach(std::function<void(const ObjectID &)> f) {
  for (auto &pair : item_list_) {
    f(pair.first);
  }
}

std::string LRUCache::DebugString() const {
  std::stringstream result;
  result << "\n(" << name_ << ") capacity: " << Capacity();
  result << "\n(" << name_
         << ") used: " << 100. * (1. - (RemainingCapacity() / (double)OriginalCapacity()))
         << "%";
  result << "\n(" << name_ << ") num objects: " << item_map_.size();
  result << "\n(" << name_ << ") num evictions: " << num_evictions_total_;
  result << "\n(" << name_ << ") bytes evicted: " << bytes_evicted_total_;
  return result.str();
}

int64_t LRUCache::ChooseObjectsToEvict(int64_t num_bytes_required,
                                       std::vector<ObjectID> &objects_to_evict) {
  int64_t bytes_evicted = 0;
  auto it = item_list_.end();
  while (bytes_evicted < num_bytes_required && it != item_list_.begin()) {
    it--;
    objects_to_evict.push_back(it->first);
    bytes_evicted += it->second;
    bytes_evicted_total_ += it->second;
    num_evictions_total_ += 1;
  }
  return bytes_evicted;
}

bool LRUCache::Exists(const ObjectID &key) const { return item_map_.count(key) > 0; }

EvictionPolicy::EvictionPolicy(const IObjectStore &object_store,
                               const IAllocator &allocator)
    : pinned_memory_bytes_(0),
      cache_("global lru", allocator.GetFootprintLimit()),
      object_store_(object_store),
      allocator_(allocator) {}

int64_t EvictionPolicy::ChooseObjectsToEvict(int64_t num_bytes_required,
                                             std::vector<ObjectID> &objects_to_evict) {
  int64_t bytes_evicted =
      cache_.ChooseObjectsToEvict(num_bytes_required, objects_to_evict);
  // Update the LRU cache.
  for (auto &object_id : objects_to_evict) {
    cache_.Remove(object_id);
  }
  return bytes_evicted;
}

void EvictionPolicy::ObjectCreated(const ObjectID &object_id) {
  cache_.Add(object_id, GetObjectSize(object_id));
}

int64_t EvictionPolicy::RequireSpace(int64_t size,
                                     std::vector<ObjectID> &objects_to_evict) {
  // Check if there is enough space to create the object.
  int64_t required_space = allocator_.Allocated() + size - allocator_.GetFootprintLimit();
  // Try to free up at least as much space as we need right now but ideally
  // up to 20% of the total capacity.
  int64_t space_to_free = std::max(required_space, allocator_.GetFootprintLimit() / 5);
  // Choose some objects to evict, and update the return pointers.
  int64_t num_bytes_evicted = ChooseObjectsToEvict(space_to_free, objects_to_evict);
  RAY_LOG(DEBUG) << "There is not enough space to create this object, so evicting "
                 << objects_to_evict.size() << " objects to free up " << num_bytes_evicted
                 << " bytes. The number of bytes in use (before "
                 << "this eviction) is " << allocator_.Allocated() << ".";
  return required_space - num_bytes_evicted;
}

void EvictionPolicy::BeginObjectAccess(const ObjectID &object_id) {
  // If the object is in the LRU cache, remove it.
  cache_.Remove(object_id);
  pinned_memory_bytes_ += GetObjectSize(object_id);
}

void EvictionPolicy::EndObjectAccess(const ObjectID &object_id) {
  auto size = GetObjectSize(object_id);
  // Add the object to the LRU cache.
  cache_.Add(object_id, size);
  pinned_memory_bytes_ -= size;
}

void EvictionPolicy::RemoveObject(const ObjectID &object_id) {
  // If the object is in the LRU cache, remove it.
  cache_.Remove(object_id);
}

int64_t EvictionPolicy::GetObjectSize(const ObjectID &object_id) const {
  return object_store_.GetObject(object_id)->GetObjectSize();
}

bool EvictionPolicy::IsObjectExists(const ObjectID &object_id) const {
  return cache_.Exists(object_id);
}

std::string EvictionPolicy::DebugString() const { return cache_.DebugString(); }
}  // namespace plasma
