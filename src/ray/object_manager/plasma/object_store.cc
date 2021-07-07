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

#include "ray/object_manager/plasma/plasma_allocator.h"

namespace plasma {

ObjectStore::ObjectStore() : object_table_() {}

LocalObject *ObjectStore::CreateObject(Allocation allocation_info,
                                       const ray::ObjectInfo &object_info,
                                       plasma::flatbuf::ObjectSource source) {
  RAY_LOG(DEBUG) << "create object " << object_info.object_id << " succeeded";
  auto ptr = std::make_unique<LocalObject>();
  auto entry =
      object_table_.emplace(object_info.object_id, std::move(ptr)).first->second.get();
  entry->allocation = std::move(allocation_info);
  entry->object_info = object_info;
  entry->state = ObjectState::PLASMA_CREATED;
  entry->create_time = std::time(nullptr);
  entry->construct_duration = -1;
  entry->source = source;

  num_objects_unsealed_++;
  num_bytes_unsealed_ += entry->GetObjectSize();
  num_bytes_created_total_ += entry->GetObjectSize();
  return entry;
}

LocalObject *ObjectStore::GetObjectInternal(const ObjectID &object_id) {
  auto it = object_table_.find(object_id);
  if (it == object_table_.end()) {
    return nullptr;
  }
  return it->second.get();
}

const LocalObject *ObjectStore::GetObject(const ObjectID &object_id) const {
  auto it = object_table_.find(object_id);
  if (it == object_table_.end()) {
    return nullptr;
  }
  return it->second.get();
}

ObjectStatus ObjectStore::ContainsSealedObject(const ObjectID &object_id) {
  auto entry = GetObjectInternal(object_id);
  return entry && entry->state == ObjectState::PLASMA_SEALED
             ? ObjectStatus::OBJECT_FOUND
             : ObjectStatus::OBJECT_NOT_FOUND;
}

const LocalObject *ObjectStore::SealObject(const ObjectID &object_id) {
  auto entry = GetObjectInternal(object_id);
  RAY_CHECK(entry != nullptr);
  RAY_CHECK(entry->state == ObjectState::PLASMA_CREATED);
  // Set the state of object to SEALED.
  entry->state = ObjectState::PLASMA_SEALED;
  // Set object construction duration.
  entry->construct_duration = std::time(nullptr) - entry->create_time;

  num_objects_unsealed_--;
  num_bytes_unsealed_ -= entry->GetObjectSize();
  return entry;
}

void ObjectStore::DeleteObject(const ObjectID &object_id) {
  auto entry = GetObject(object_id);
  PlasmaAllocator::Free(entry->allocation);
  if (entry->state == ObjectState::PLASMA_CREATED) {
    num_bytes_unsealed_ -= entry->GetObjectSize();
    num_objects_unsealed_--;
  }
  object_table_.erase(object_id);
}

size_t ObjectStore::GetNumBytesCreatedTotal() const { return num_bytes_created_total_; }

size_t ObjectStore::GetNumBytesUnsealed() const { return num_bytes_unsealed_; };

size_t ObjectStore::GetNumObjectsUnsealed() const { return num_objects_unsealed_; };

int64_t ObjectStore::GetNumBytesAllocated() const { return PlasmaAllocator::Allocated(); }

int64_t ObjectStore::GetNumBytesCapacity() const {
  return PlasmaAllocator::GetFootprintLimit();
}

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

}  // namespace plasma
