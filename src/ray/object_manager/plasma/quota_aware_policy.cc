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

#include "ray/object_manager/plasma/quota_aware_policy.h"

#include "ray/object_manager/plasma/object_directory.h"
#include "ray/object_manager/plasma/plasma_allocator.h"
#include "ray/util/logging.h"

#include <algorithm>
#include <sstream>

namespace plasma {

void LRUCache::Add(const ObjectID& key, int64_t size) {
  auto it = item_map_.find(key);
  RAY_CHECK(it == item_map_.end());
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

QuotaAwarePolicy::QuotaAwarePolicy(ObjectDirectory* store_info, int64_t max_size)
    : pinned_memory_bytes_(0), store_info_(store_info), cache_("global lru", max_size) {}

int64_t QuotaAwarePolicy::ChooseObjectsToEvict(int64_t num_bytes_required,
                                             std::vector<ObjectID>* objects_to_evict) {
  int64_t bytes_evicted =
      cache_.ChooseObjectsToEvict(num_bytes_required, objects_to_evict);
  // Update the LRU cache.
  for (auto& object_id : *objects_to_evict) {
    cache_.Remove(object_id);
  }
  return bytes_evicted;
}

bool QuotaAwarePolicy::RequireSpace(int64_t size, std::vector<ObjectID>* objects_to_evict) {
  // Check if there is enough space to create the object.
  int64_t required_space =
      PlasmaAllocator::Allocated() + size - PlasmaAllocator::GetFootprintLimit();
  // Try to free up at least as much space as we need right now but ideally
  // up to 20% of the total capacity.
  int64_t space_to_free =
      std::max(required_space, PlasmaAllocator::GetFootprintLimit() / 5);
  RAY_LOG(DEBUG) << "not enough space to create this object, so evicting objects";
  // Choose some objects to evict, and update the return pointers.
  int64_t num_bytes_evicted = ChooseObjectsToEvict(space_to_free, objects_to_evict);
  RAY_LOG(INFO) << "There is not enough space to create this object, so evicting "
                  << objects_to_evict->size() << " objects to free up "
                  << num_bytes_evicted << " bytes. The number of bytes in use (before "
                  << "this eviction) is " << PlasmaAllocator::Allocated() << ".";
  return num_bytes_evicted >= required_space && num_bytes_evicted > 0;
}

bool QuotaAwarePolicy::HasQuota(Client* client, bool is_create) {
  if (!is_create) {
    return false;  // no quota enforcement on read requests yet
  }
  return per_client_cache_.find(client) != per_client_cache_.end();
}

void QuotaAwarePolicy::ObjectCreated(const ObjectID& object_id, int64_t size,
                                     Client* client, bool is_create) {
  if (HasQuota(client, is_create)) {
    per_client_cache_[client]->Add(object_id, size);
    owned_by_client_[object_id] = client;
  } else {
    cache_.Add(object_id, size);
  }
}

bool QuotaAwarePolicy::SetClientQuota(Client* client, int64_t output_memory_quota) {
  if (per_client_cache_.find(client) != per_client_cache_.end()) {
    RAY_LOG(WARNING) << "Cannot change the client quota once set";
    return false;
  }

  if (cache_.Capacity() - output_memory_quota <
      cache_.OriginalCapacity() * kGlobalLruReserveFraction) {
    RAY_LOG(WARNING) << "Not enough memory to set client quota: " << DebugString();
    return false;
  }

  // those objects will be lazily evicted on the next call
  cache_.AdjustCapacity(-output_memory_quota);
  per_client_cache_[client] =
      std::unique_ptr<LRUCache>(new LRUCache(client->name, output_memory_quota));
  return true;
}

bool QuotaAwarePolicy::EnforcePerClientQuota(Client* client, int64_t size, bool is_create,
                                             std::vector<ObjectID>* objects_to_evict) {
  if (!HasQuota(client, is_create)) {
    return true;
  }

  auto& client_cache = per_client_cache_[client];
  if (size > client_cache->Capacity()) {
    RAY_LOG(WARNING) << "object too large (" << size
                       << " bytes) to fit in client quota " << client_cache->Capacity()
                       << " " << DebugString();
    return false;
  }

  if (client_cache->RemainingCapacity() >= size) {
    return true;
  }

  int64_t space_to_free = size - client_cache->RemainingCapacity();
  if (space_to_free > 0) {
    std::vector<ObjectID> candidates;
    client_cache->ChooseObjectsToEvict(space_to_free, &candidates);
    for (ObjectID& object_id : candidates) {
      if (shared_for_read_.count(object_id)) {
        // Pinned so we can't evict it, so demote the object to global LRU instead.
        // We an do this by simply removing it from all data structures, so that
        // the next EndObjectAccess() will add it back to global LRU.
        shared_for_read_.erase(object_id);
      } else {
        objects_to_evict->push_back(object_id);
      }
      owned_by_client_.erase(object_id);
      client_cache->Remove(object_id);
    }
  }
  return true;
}

void QuotaAwarePolicy::BeginObjectAccess(const ObjectID& object_id, int64_t size) {
  pinned_memory_bytes_ += size;
  if (owned_by_client_.find(object_id) != owned_by_client_.end()) {
    shared_for_read_.insert(object_id);
  } else {
    // If the object is in the LRU cache, remove it.
    cache_.Remove(object_id);
  }
}

void QuotaAwarePolicy::EndObjectAccess(const ObjectID& object_id, int64_t size) {
  pinned_memory_bytes_ -= size;
  if (owned_by_client_.find(object_id) != owned_by_client_.end()) {
    shared_for_read_.erase(object_id);
  } else {
    // Add the object to the LRU cache.
    cache_.Add(object_id, size);
  }
}

void QuotaAwarePolicy::RemoveObject(const ObjectID& object_id) {
  if (owned_by_client_.find(object_id) != owned_by_client_.end()) {
    per_client_cache_[owned_by_client_[object_id]]->Remove(object_id);
    owned_by_client_.erase(object_id);
    shared_for_read_.erase(object_id);
    return;
  }
  // If the object is in the LRU cache, remove it.
  cache_.Remove(object_id);
}

void QuotaAwarePolicy::RefreshObjects(const std::vector<ObjectID>& object_ids) {
  for (const auto& object_id : object_ids) {
    if (owned_by_client_.find(object_id) != owned_by_client_.end()) {
      int64_t size = per_client_cache_[owned_by_client_[object_id]]->Remove(object_id);
      per_client_cache_[owned_by_client_[object_id]]->Add(object_id, size);
    }
    int64_t size = cache_.Remove(object_id);
    if (size != -1) {
      cache_.Add(object_id, size);
    }
  }
}

void QuotaAwarePolicy::ClientDisconnected(Client* client) {
  if (per_client_cache_.find(client) == per_client_cache_.end()) {
    return;
  }
  // return capacity back to global LRU
  cache_.AdjustCapacity(per_client_cache_[client]->Capacity());
  // clean up any entries used to track this client's quota usage
  per_client_cache_[client]->Foreach([this](const ObjectID& obj) {
    if (!shared_for_read_.count(obj)) {
      // only add it to the global LRU if we have it in pinned mode
      // otherwise, EndObjectAccess will add it later
      cache_.Add(obj, store_info_->GetObjectSize(obj));
    }
    owned_by_client_.erase(obj);
    shared_for_read_.erase(obj);
  });
  per_client_cache_.erase(client);
}

std::string QuotaAwarePolicy::DebugString() const {
  std::stringstream result;
  result << "num clients with quota: " << per_client_cache_.size();
  result << "\nquota map size: " << owned_by_client_.size();
  result << "\npinned quota map size: " << shared_for_read_.size();
  result << "\nallocated bytes: " << PlasmaAllocator::Allocated();
  result << "\nallocation limit: " << PlasmaAllocator::GetFootprintLimit();
  result << "\npinned bytes: " << pinned_memory_bytes_;
  result << cache_.DebugString();
  for (const auto& pair : per_client_cache_) {
    result << pair.second->DebugString();
  }
  return result.str();
}

}  // namespace plasma
