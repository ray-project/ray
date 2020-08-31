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

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "ray/object_manager/plasma/common.h"
#include "ray/object_manager/plasma/compat.h"

#ifdef PLASMA_CUDA
using arrow::cuda::CudaIpcMemHandle;
#endif

namespace plasma {

/// Allocation granularity used in plasma for object allocation.
constexpr int64_t kBlockSize = 64;

// TODO(pcm): Replace this by the flatbuffers message PlasmaObjectSpec.
struct PlasmaObject {
#ifdef PLASMA_CUDA
  // IPC handle for Cuda.
  std::shared_ptr<CudaIpcMemHandle> ipc_handle;
#endif
  /// The file descriptor of the memory mapped file in the store. It is used as
  /// a unique identifier of the file in the client to look up the corresponding
  /// file descriptor on the client's side.
  MEMFD_TYPE store_fd;
  /// The offset in bytes in the memory mapped file of the data.
  ptrdiff_t data_offset;
  /// The offset in bytes in the memory mapped file of the metadata.
  ptrdiff_t metadata_offset;
  /// The size in bytes of the data.
  int64_t data_size;
  /// The size in bytes of the metadata.
  int64_t metadata_size;
  /// Device number object is on.
  int device_num;

  bool operator==(const PlasmaObject& other) const {
    return (
#ifdef PLASMA_CUDA
        (ipc_handle == other.ipc_handle) &&
#endif
        (store_fd == other.store_fd) && (data_offset == other.data_offset) &&
        (metadata_offset == other.metadata_offset) && (data_size == other.data_size) &&
        (metadata_size == other.metadata_size) && (device_num == other.device_num));
  }
};

enum class ObjectStatus : int {
  /// The object was not found.
  OBJECT_NOT_FOUND = 0,
  /// The object was found.
  OBJECT_FOUND = 1
};

/// The plasma store information that is exposed to the eviction policy.
struct PlasmaStoreInfo {
  /// Objects that are in the Plasma store.
  ObjectTable objects;
  /// Boolean flag indicating whether to start the object store with hugepages
  /// support enabled. Huge pages are substantially larger than normal memory
  /// pages (e.g. 2MB or 1GB instead of 4KB) and using them can reduce
  /// bookkeeping overhead from the OS.
  bool hugepages_enabled;
  /// A (platform-dependent) directory where to create the memory-backed file.
  std::string directory;
};

/// Get an entry from the object table and return NULL if the object_id
/// is not present.
///
/// \param store_info The PlasmaStoreInfo that contains the object table.
/// \param object_id The object_id of the entry we are looking for.
/// \return The entry associated with the object_id or NULL if the object_id
///         is not present.
ObjectTableEntry* GetObjectTableEntry(PlasmaStoreInfo* store_info,
                                      const ObjectID& object_id);

/// Globally accessible reference to plasma store configuration.
extern const PlasmaStoreInfo* plasma_config;

}  // namespace plasma
