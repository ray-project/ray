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

namespace plasma {

void LRUCache::Add(const ObjectID& key, int64_t size) {
  auto it = item_map_.find(key);
  ARROW_CHECK(it == item_map_.end());
  // Note that it is important to use a list so the iterators stay valid.
  item_list_.emplace_front(key, size);
  item_map_.emplace(key, item_list_.begin());
}

void LRUCache::Remove(const ObjectID& key) {
  auto it = item_map_.find(key);
  ARROW_CHECK(it != item_map_.end());
  item_list_.erase(it->second);
  item_map_.erase(it);
}

int64_t LRUCache::ChooseObjectsToEvict(int64_t num_bytes_required,
                                       std::vector<ObjectID>* objects_to_evict) {
  int64_t bytes_evicted = 0;
  auto it = item_list_.end();
  while (bytes_evicted < num_bytes_required && it != item_list_.begin()) {
    it--;
    objects_to_evict->push_back(it->first);
    bytes_evicted += it->second;
  }
  return bytes_evicted;
}

EvictionPolicy::EvictionPolicy(PlasmaStoreInfo* store_info) : store_info_(store_info) {}

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

void EvictionPolicy::ObjectCreated(const ObjectID& object_id) {
  auto entry = store_info_->objects[object_id].get();
  cache_.Add(object_id, entry->data_size + entry->metadata_size);
}

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

void EvictionPolicy::BeginObjectAccess(const ObjectID& object_id,
                                       std::vector<ObjectID>* objects_to_evict) {
  // If the object is in the LRU cache, remove it.
  cache_.Remove(object_id);
}

void EvictionPolicy::EndObjectAccess(const ObjectID& object_id,
                                     std::vector<ObjectID>* objects_to_evict) {
  auto entry = store_info_->objects[object_id].get();
  // Add the object to the LRU cache.
  cache_.Add(object_id, entry->data_size + entry->metadata_size);
}

void EvictionPolicy::RemoveObject(const ObjectID& object_id) {
  // If the object is in the LRU cache, remove it.
  cache_.Remove(object_id);
}

}  // namespace plasma
