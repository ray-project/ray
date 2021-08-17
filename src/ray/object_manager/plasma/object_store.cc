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

#include "ray/object_manager/plasma/object_store.h"

namespace plasma {

ObjectStore::ObjectStore(IAllocator &allocator)
    : allocator_(allocator), object_table_() {}

const LocalObject *ObjectStore::CreateObject(const ray::ObjectInfo &object_info,
                                             plasma::flatbuf::ObjectSource source,
                                             bool fallback_allocate) {
  RAY_LOG(DEBUG) << "attempting to create object " << object_info.object_id << " size "
                 << object_info.data_size;
  RAY_CHECK(object_table_.count(object_info.object_id) == 0)
      << object_info.object_id << " already exists!";
  auto object_size = object_info.GetObjectSize();
  auto allocation = fallback_allocate ? allocator_.FallbackAllocate(object_size)
                                      : allocator_.Allocate(object_size);
  RAY_LOG_EVERY_MS(INFO, 10 * 60 * 1000)
      << "Object store current usage " << (allocator_.Allocated() / 1e9) << " / "
      << (allocator_.GetFootprintLimit() / 1e9) << " GB.";
  if (!allocation.has_value()) {
    return nullptr;
  }
  auto ptr = std::make_unique<LocalObject>(std::move(allocation.value()));
  auto entry =
      object_table_.emplace(object_info.object_id, std::move(ptr)).first->second.get();
  entry->object_info = object_info;
  entry->state = ObjectState::PLASMA_CREATED;
  entry->create_time = std::time(nullptr);
  entry->construct_duration = -1;
  entry->source = source;

  num_objects_unsealed_++;
  num_bytes_unsealed_ += entry->GetObjectSize();
  num_bytes_created_total_ += entry->GetObjectSize();
  RAY_LOG(DEBUG) << "create object " << object_info.object_id << " succeeded";
  return entry;
}

const LocalObject *ObjectStore::GetObject(const ObjectID &object_id) const {
  auto it = object_table_.find(object_id);
  if (it == object_table_.end()) {
    return nullptr;
  }
  return it->second.get();
}

const LocalObject *ObjectStore::SealObject(const ObjectID &object_id) {
  auto entry = GetMutableObject(object_id);
  if (entry == nullptr || entry->state == ObjectState::PLASMA_SEALED) {
    return nullptr;
  }
  entry->state = ObjectState::PLASMA_SEALED;
  entry->construct_duration = std::time(nullptr) - entry->create_time;
  num_objects_unsealed_--;
  num_bytes_unsealed_ -= entry->GetObjectSize();
  return entry;
}

bool ObjectStore::DeleteObject(const ObjectID &object_id) {
  auto entry = GetMutableObject(object_id);
  if (entry == nullptr) {
    return false;
  }
  if (entry->state == ObjectState::PLASMA_CREATED) {
    num_bytes_unsealed_ -= entry->GetObjectSize();
    num_objects_unsealed_--;
  }
  allocator_.Free(std::move(entry->allocation));
  object_table_.erase(object_id);
  return true;
}

int64_t ObjectStore::GetNumBytesCreatedTotal() const { return num_bytes_created_total_; }

int64_t ObjectStore::GetNumBytesUnsealed() const { return num_bytes_unsealed_; };

int64_t ObjectStore::GetNumObjectsUnsealed() const { return num_objects_unsealed_; };

void ObjectStore::GetDebugDump(std::stringstream &buffer) const {
  size_t num_objects_spillable = 0;
  size_t num_bytes_spillable = 0;
  size_t num_objects_unsealed = 0;
  size_t num_bytes_unsealed = 0;
  size_t num_objects_in_use = 0;
  size_t num_bytes_in_use = 0;
  size_t num_objects_evictable = 0;
  size_t num_bytes_evictable = 0;

  size_t num_objects_created_by_worker = 0;
  size_t num_bytes_created_by_worker = 0;
  size_t num_objects_restored = 0;
  size_t num_bytes_restored = 0;
  size_t num_objects_received = 0;
  size_t num_bytes_received = 0;
  size_t num_objects_errored = 0;
  size_t num_bytes_errored = 0;
  // TODO(scv119): generate metrics eagerly.
  for (const auto &obj_entry : object_table_) {
    const auto &obj = obj_entry.second;
    if (obj->state == ObjectState::PLASMA_CREATED) {
      num_objects_unsealed++;
      num_bytes_unsealed += obj->object_info.data_size;
    } else if (obj->ref_count == 1 &&
               obj->source == plasma::flatbuf::ObjectSource::CreatedByWorker) {
      num_objects_spillable++;
      num_bytes_spillable += obj->object_info.data_size;
    } else if (obj->ref_count > 0) {
      num_objects_in_use++;
      num_bytes_in_use += obj->object_info.data_size;
    } else {
      num_bytes_evictable++;
      num_bytes_evictable += obj->object_info.data_size;
    }

    if (obj->source == plasma::flatbuf::ObjectSource::CreatedByWorker) {
      num_objects_created_by_worker++;
      num_bytes_created_by_worker += obj->object_info.data_size;
    } else if (obj->source == plasma::flatbuf::ObjectSource::RestoredFromStorage) {
      num_objects_restored++;
      num_bytes_restored += obj->object_info.data_size;
    } else if (obj->source == plasma::flatbuf::ObjectSource::ReceivedFromRemoteRaylet) {
      num_objects_received++;
      num_bytes_received += obj->object_info.data_size;
    } else if (obj->source == plasma::flatbuf::ObjectSource::ErrorStoredByRaylet) {
      num_objects_errored++;
      num_bytes_errored += obj->object_info.data_size;
    }
  }
  buffer << "- objects spillable: " << num_objects_spillable << "\n";
  buffer << "- bytes spillable: " << num_bytes_spillable << "\n";
  buffer << "- objects unsealed: " << num_objects_unsealed << "\n";
  buffer << "- bytes unsealed: " << num_bytes_unsealed << "\n";
  buffer << "- objects in use: " << num_objects_in_use << "\n";
  buffer << "- bytes in use: " << num_bytes_in_use << "\n";
  buffer << "- objects evictable: " << num_objects_evictable << "\n";
  buffer << "- bytes evictable: " << num_bytes_evictable << "\n";
  buffer << "\n";

  buffer << "- objects created by worker: " << num_objects_created_by_worker << "\n";
  buffer << "- bytes created by worker: " << num_bytes_created_by_worker << "\n";
  buffer << "- objects restored: " << num_objects_restored << "\n";
  buffer << "- bytes restored: " << num_bytes_restored << "\n";
  buffer << "- objects received: " << num_objects_received << "\n";
  buffer << "- bytes received: " << num_bytes_received << "\n";
  buffer << "- objects errored: " << num_objects_errored << "\n";
  buffer << "- bytes errored: " << num_bytes_errored << "\n";
}

LocalObject *ObjectStore::GetMutableObject(const ObjectID &object_id) {
  auto it = object_table_.find(object_id);
  if (it == object_table_.end()) {
    return nullptr;
  }
  return it->second.get();
}

}  // namespace plasma
