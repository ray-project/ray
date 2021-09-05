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

#pragma once

#include "absl/container/flat_hash_map.h"
#include "ray/common/id.h"

namespace plasma {

struct LifecycleMetadata {
  LifecycleMetadata() : ref_count(0) {}
  int64_t ref_count;
};

// LifecycleMetadataStore stores the metadata for object lifecycles,
// including ref_counts.
class LifecycleMetadataStore {
 public:
  bool IncreaseRefCount(const ray::ObjectID &id);
  bool DecreaseRefCount(const ray::ObjectID &id);
  void RemoveObject(const ray::ObjectID &id);
  const LifecycleMetadata &GetMetadata(const ray::ObjectID &id) const;
  int64_t GetRefCount(const ray::ObjectID &id) const;

 private:
  friend struct ObjectLifecycleManagerTest;

  absl::flat_hash_map<ray::ObjectID, LifecycleMetadata> store_;
};
}  // namespace plasma
