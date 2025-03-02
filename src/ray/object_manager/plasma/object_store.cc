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

#include <memory>
#include <utility>

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

#if defined(__APPLE__) || defined(__linux__)
  if (object_info.is_mutable) {
    RAY_LOG(DEBUG) << "PlasmaObjectHeader::Init " << object_info.object_id;
    entry->GetPlasmaObjectHeader()->Init();
  }
#endif

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
  return entry;
}

bool ObjectStore::DeleteObject(const ObjectID &object_id) {
  auto entry = GetMutableObject(object_id);
  if (!entry) {
    return false;
  }

  allocator_.Free(std::move(entry->allocation));
  object_table_.erase(object_id);
  return true;
}

LocalObject *ObjectStore::GetMutableObject(const ObjectID &object_id) {
  auto it = object_table_.find(object_id);
  if (it == object_table_.end()) {
    return nullptr;
  }
  return it->second.get();
}

}  // namespace plasma
