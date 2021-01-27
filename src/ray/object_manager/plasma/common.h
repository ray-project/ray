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

#include <stddef.h>

#include <memory>
#include <string>
#include <unordered_map>

#include "ray/common/id.h"
#include "ray/object_manager/format/object_manager_generated.h"
#include "ray/object_manager/plasma/compat.h"

namespace plasma {

using ray::NodeID;
using ray::ObjectID;
using ray::WorkerID;

enum class ObjectLocation : int32_t { Local, Remote, Nonexistent };

/// Size of object hash digests.
constexpr int64_t kDigestSize = sizeof(uint64_t);

enum class ObjectState : int {
  /// Object was created but not sealed in the local Plasma Store.
  PLASMA_CREATED = 1,
  /// Object is sealed and stored in the local Plasma Store.
  PLASMA_SEALED = 2,
  /// Object is evicted to external store.
  PLASMA_EVICTED = 3,
};

/// This type is used by the Plasma store. It is here because it is exposed to
/// the eviction policy.
struct ObjectTableEntry {
  ObjectTableEntry();

  ~ObjectTableEntry();

  /// Memory mapped file containing the object.
  MEMFD_TYPE fd;
  /// Device number.
  int device_num;
  /// Size of the underlying map.
  int64_t map_size;
  /// Offset from the base of the mmap.
  ptrdiff_t offset;
  /// Pointer to the object data. Needed to free the object.
  uint8_t *pointer;
  /// Size of the object in bytes.
  int64_t data_size;
  /// Size of the object metadata in bytes.
  int64_t metadata_size;
  /// Number of clients currently using this object.
  int ref_count;
  /// Owner's raylet ID.
  NodeID owner_raylet_id;
  /// Owner's IP address.
  std::string owner_ip_address;
  /// Owner's port.
  int owner_port;
  /// Owner's worker ID.
  WorkerID owner_worker_id;
  /// Unix epoch of when this object was created.
  int64_t create_time;
  /// How long creation of this object took.
  int64_t construct_duration;
  /// The state of the object, e.g., whether it is open or sealed.
  ObjectState state;
};

/// Mapping from ObjectIDs to information about the object.
typedef std::unordered_map<ObjectID, std::unique_ptr<ObjectTableEntry>> ObjectTable;

}  // namespace plasma
