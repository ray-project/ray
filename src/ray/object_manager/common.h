// Copyright 2020-2021 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <semaphore.h>

#include <atomic>
#include <boost/asio.hpp>
#include <functional>

#include "ray/common/id.h"
#include "ray/common/status.h"

namespace ray {

/// A callback to asynchronously spill objects when space is needed.
/// It spills enough objects to saturate all spill IO workers.
using SpillObjectsCallback = std::function<bool()>;

/// A callback to call when space has been released.
using SpaceReleasedCallback = std::function<void()>;

/// A callback to call when a spilled object needs to be returned to the object store.
using RestoreSpilledObjectCallback =
    std::function<void(const ObjectID &,
                       int64_t size,
                       const std::string &,
                       std::function<void(const ray::Status &)>)>;

struct PlasmaObjectHeader {
  // Used to signal to the writer when all readers are done.
  sem_t rw_semaphore;

  // Protects all following state, used to signal from writer to readers.
  pthread_mutex_t wr_mut;
  // Used to signal to readers when the writer is done writing a new version.
  pthread_cond_t cond;
  // The object version. For immutable objects, this gets incremented to 1 on
  // the first write and then should never be modified. For mutable objects,
  // each new write must increment the version before releasing to readers.
  int64_t version = 0;
  // The total number of reads allowed before the writer can write again. This
  // value should be set by the writer before releasing to readers.
  // For immutable objects, this is set to -1 and infinite reads are allowed.
  // Otherwise, readers must acquire/release before/after reading.
  int64_t num_readers = 0;
  // The number of readers who can acquire the current version. For mutable
  // objects, readers must ensure this is > 0 and decrement before they read.
  // Once this value reaches 0, no more readers are allowed until the writer
  // writes a new version.
  int64_t num_read_acquires_remaining = 0;
  // The number of readers who must release the current version before a new
  // version can be written. For mutable objects, readers must decrement this
  // when they are done reading the current version. Once this value reaches 0,
  // the reader should signal to the writer that they can write again.
  int64_t num_read_releases_remaining = 0;
  // The valid data and metadata size of the Ray object.
  // Not used for immutable objects.
  // For mutable objects, this should be modified when the new object has a
  // different data/metadata size.
  uint64_t data_size = 0;
  uint64_t metadata_size = 0;

  void Init();

  void Destroy();

  // Blocks until there are no more readers.
  // NOTE: Caller should ensure there is one writer at a time.
  /// \param write_version The new version for write.
  /// \param new_size The new data size of the object.
  void WriteAcquire(int64_t write_version, uint64_t new_data_size);

  // Call after completing a write to signal to num_readers many readers.
  void WriteRelease(int64_t write_version, int64_t num_readers);

  // Blocks until the given version or a more recent version is ready to read.
  //
  // \param read_version The minimum version to wait for.
  // \return The version that was read. This should be passed to ReadRelease
  // when the reader is done.
  int64_t ReadAcquire(int64_t read_version);

  // Finishes the read. If all reads are done, signals to the
  // writer. This is not necessary to call for objects that have
  // num_readers=-1.
  void ReadRelease(int64_t read_version);

  // Get the data size of the plasma object.
  // The reader must first ReadAcquire.
  uint64_t GetDataSize() const;
};

/// A struct that includes info about the object.
struct ObjectInfo {
  ObjectID object_id;
  bool is_mutable;
  int64_t data_size = 0;
  int64_t metadata_size = 0;
  /// Owner's raylet ID.
  NodeID owner_raylet_id;
  /// Owner's IP address.
  std::string owner_ip_address;
  /// Owner's port.
  int owner_port;
  /// Owner's worker ID.
  WorkerID owner_worker_id;

  int64_t GetObjectSize() const {
    return sizeof(PlasmaObjectHeader) + data_size + metadata_size;
  }

  bool operator==(const ObjectInfo &other) const {
    return ((object_id == other.object_id) && (data_size == other.data_size) &&
            (metadata_size == other.metadata_size) &&
            (owner_raylet_id == other.owner_raylet_id) &&
            (owner_ip_address == other.owner_ip_address) &&
            (owner_port == other.owner_port) &&
            (owner_worker_id == other.owner_worker_id));
  }
};

// A callback to call when an object is added to the shared memory store.
using AddObjectCallback = std::function<void(const ObjectInfo &)>;

// A callback to call when an object is removed from the shared memory store.
using DeleteObjectCallback = std::function<void(const ObjectID &)>;

}  // namespace ray
