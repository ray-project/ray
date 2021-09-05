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

#include "ray/object_manager/plasma/lifecycle_meta_store.h"

namespace plasma {

using namespace ray;

namespace {
const LifecycleMetadata kDefaultStats{};
}

bool LifecycleMetadataStore::IncreaseRefCount(const ObjectID &id) {
  store_[id].ref_count += 1;
  return true;
}

bool LifecycleMetadataStore::DecreaseRefCount(const ObjectID &id) {
  if (GetMetadata(id).ref_count == 0) {
    return false;
  }
  store_[id].ref_count -= 1;
  return true;
}

void LifecycleMetadataStore::RemoveObject(const ObjectID &id) { store_.erase(id); }

const LifecycleMetadata &LifecycleMetadataStore::GetMetadata(const ObjectID &id) const {
  auto it = store_.find(id);
  if (it == store_.end()) {
    return kDefaultStats;
  }
  return it->second;
}

int64_t LifecycleMetadataStore::GetRefCount(const ray::ObjectID &id) const {
  return GetMetadata(id).ref_count;
}

}  // namespace plasma
