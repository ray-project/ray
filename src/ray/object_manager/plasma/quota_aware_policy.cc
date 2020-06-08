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

#include "plasma/quota_aware_policy.h"
#include "plasma/common.h"
#include "plasma/plasma_allocator.h"

#include <algorithm>
#include <memory>
#include <sstream>

namespace plasma {

QuotaAwarePolicy::QuotaAwarePolicy(PlasmaStoreInfo* store_info, int64_t max_size)
    : EvictionPolicy(store_info, max_size) {}

bool QuotaAwarePolicy::HasQuota(Client* client, bool is_create) {
  if (!is_create) {
    return false;  // no quota enforcement on read requests yet
  }
  return per_client_cache_.find(client) != per_client_cache_.end();
}

void QuotaAwarePolicy::ObjectCreated(const ObjectID& object_id, Client* client,
                                     bool is_create) {
  if (HasQuota(client, is_create)) {
    per_client_cache_[client]->Add(object_id, GetObjectSize(object_id));
    owned_by_client_[object_id] = client;
  } else {
    EvictionPolicy::ObjectCreated(object_id, client, is_create);
  }
}

bool QuotaAwarePolicy::SetClientQuota(Client* client, int64_t output_memory_quota) {
  if (per_client_cache_.find(client) != per_client_cache_.end()) {
    ARROW_LOG(WARNING) << "Cannot change the client quota once set";
    return false;
  }

  if (cache_.Capacity() - output_memory_quota <
      cache_.OriginalCapacity() * kGlobalLruReserveFraction) {
    ARROW_LOG(WARNING) << "Not enough memory to set client quota: " << DebugString();
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
    ARROW_LOG(WARNING) << "object too large (" << size
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

void QuotaAwarePolicy::BeginObjectAccess(const ObjectID& object_id) {
  if (owned_by_client_.find(object_id) != owned_by_client_.end()) {
    shared_for_read_.insert(object_id);
    pinned_memory_bytes_ += GetObjectSize(object_id);
    return;
  }
  EvictionPolicy::BeginObjectAccess(object_id);
}

void QuotaAwarePolicy::EndObjectAccess(const ObjectID& object_id) {
  if (owned_by_client_.find(object_id) != owned_by_client_.end()) {
    shared_for_read_.erase(object_id);
    pinned_memory_bytes_ -= GetObjectSize(object_id);
    return;
  }
  EvictionPolicy::EndObjectAccess(object_id);
}

void QuotaAwarePolicy::RemoveObject(const ObjectID& object_id) {
  if (owned_by_client_.find(object_id) != owned_by_client_.end()) {
    per_client_cache_[owned_by_client_[object_id]]->Remove(object_id);
    owned_by_client_.erase(object_id);
    shared_for_read_.erase(object_id);
    return;
  }
  EvictionPolicy::RemoveObject(object_id);
}

void QuotaAwarePolicy::RefreshObjects(const std::vector<ObjectID>& object_ids) {
  for (const auto& object_id : object_ids) {
    if (owned_by_client_.find(object_id) != owned_by_client_.end()) {
      int64_t size = per_client_cache_[owned_by_client_[object_id]]->Remove(object_id);
      per_client_cache_[owned_by_client_[object_id]]->Add(object_id, size);
    }
  }
  EvictionPolicy::RefreshObjects(object_ids);
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
      cache_.Add(obj, GetObjectSize(obj));
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
