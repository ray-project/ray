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

#include "plasma/eviction_policy.h"
#include "plasma/plasma_allocator.h"

#include <algorithm>
#include <sstream>

namespace plasma {

void LRUCache::Add(const ObjectID& key, int64_t size) {
  auto it = item_map_.find(key);
  ARROW_CHECK(it == item_map_.end());
  // Note that it is important to use a list so the iterators stay valid.
  item_list_.emplace_front(key, size);
  item_map_.emplace(key, item_list_.begin());
  used_capacity_ += size;
}

int64_t LRUCache::Remove(const ObjectID& key) {
  auto it = item_map_.find(key);
  if (it == item_map_.end()) {
    return -1;
  }
  int64_t size = it->second->second;
  used_capacity_ -= size;
  item_list_.erase(it->second);
  item_map_.erase(it);
  ARROW_CHECK(used_capacity_ >= 0) << DebugString();
  return size;
}

void LRUCache::AdjustCapacity(int64_t delta) {
  ARROW_LOG(INFO) << "adjusting global lru capacity from " << Capacity() << " to "
                  << (Capacity() + delta) << " (max " << OriginalCapacity() << ")";
  capacity_ += delta;
  ARROW_CHECK(used_capacity_ >= 0) << DebugString();
}

int64_t LRUCache::Capacity() const { return capacity_; }

int64_t LRUCache::OriginalCapacity() const { return original_capacity_; }

int64_t LRUCache::RemainingCapacity() const { return capacity_ - used_capacity_; }

void LRUCache::Foreach(std::function<void(const ObjectID&)> f) {
  for (auto& pair : item_list_) {
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
                                       std::vector<ObjectID>* objects_to_evict) {
  int64_t bytes_evicted = 0;
  auto it = item_list_.end();
  while (bytes_evicted < num_bytes_required && it != item_list_.begin()) {
    it--;
    objects_to_evict->push_back(it->first);
    bytes_evicted += it->second;
    bytes_evicted_total_ += it->second;
    num_evictions_total_ += 1;
  }
  return bytes_evicted;
}

EvictionPolicy::EvictionPolicy(PlasmaStoreInfo* store_info, int64_t max_size)
    : pinned_memory_bytes_(0), store_info_(store_info), cache_("global lru", max_size) {}

int64_t EvictionPolicy::ChooseObjectsToEvict(int64_t num_bytes_required,
                                             std::vector<ObjectID>* objects_to_evict) {
  int64_t bytes_evicted =
      cache_.ChooseObjectsToEvict(num_bytes_required, objects_to_evict);
  // Update the LRU cache.
  for (auto& object_id : *objects_to_evict) {
    cache_.Remove(object_id);
  }
  return bytes_evicted;
}

void EvictionPolicy::ObjectCreated(const ObjectID& object_id, Client* client,
                                   bool is_create) {
  cache_.Add(object_id, GetObjectSize(object_id));
}

bool EvictionPolicy::SetClientQuota(Client* client, int64_t output_memory_quota) {
  return false;
}

bool EvictionPolicy::EnforcePerClientQuota(Client* client, int64_t size, bool is_create,
                                           std::vector<ObjectID>* objects_to_evict) {
  return true;
}

void EvictionPolicy::ClientDisconnected(Client* client) {}

bool EvictionPolicy::RequireSpace(int64_t size, std::vector<ObjectID>* objects_to_evict) {
  // Check if there is enough space to create the object.
  int64_t required_space =
      PlasmaAllocator::Allocated() + size - PlasmaAllocator::GetFootprintLimit();
  // Try to free up at least as much space as we need right now but ideally
  // up to 20% of the total capacity.
  int64_t space_to_free =
      std::max(required_space, PlasmaAllocator::GetFootprintLimit() / 5);
  ARROW_LOG(DEBUG) << "not enough space to create this object, so evicting objects";
  // Choose some objects to evict, and update the return pointers.
  int64_t num_bytes_evicted = ChooseObjectsToEvict(space_to_free, objects_to_evict);
  ARROW_LOG(INFO) << "There is not enough space to create this object, so evicting "
                  << objects_to_evict->size() << " objects to free up "
                  << num_bytes_evicted << " bytes. The number of bytes in use (before "
                  << "this eviction) is " << PlasmaAllocator::Allocated() << ".";
  return num_bytes_evicted >= required_space && num_bytes_evicted > 0;
}

void EvictionPolicy::BeginObjectAccess(const ObjectID& object_id) {
  // If the object is in the LRU cache, remove it.
  cache_.Remove(object_id);
  pinned_memory_bytes_ += GetObjectSize(object_id);
}

void EvictionPolicy::EndObjectAccess(const ObjectID& object_id) {
  auto size = GetObjectSize(object_id);
  // Add the object to the LRU cache.
  cache_.Add(object_id, size);
  pinned_memory_bytes_ -= size;
}

void EvictionPolicy::RemoveObject(const ObjectID& object_id) {
  // If the object is in the LRU cache, remove it.
  cache_.Remove(object_id);
}

void EvictionPolicy::RefreshObjects(const std::vector<ObjectID>& object_ids) {
  for (const auto& object_id : object_ids) {
    int64_t size = cache_.Remove(object_id);
    if (size != -1) {
      cache_.Add(object_id, size);
    }
  }
}

int64_t EvictionPolicy::GetObjectSize(const ObjectID& object_id) const {
  auto entry = store_info_->objects[object_id].get();
  return entry->data_size + entry->metadata_size;
}

std::string EvictionPolicy::DebugString() const { return cache_.DebugString(); }

}  // namespace plasma
