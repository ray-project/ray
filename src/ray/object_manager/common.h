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
  // Protects all following state. We use a mutex and conditional variable here
  // because there can be multiple readers and:
  // - we should not write again until all readers are done.
  // - we should not read again until a write is done.
  pthread_mutex_t mut;
  pthread_cond_t cond;
  sem_t rw_semaphore;
  int64_t version = 0;
  // Max number of reads allowed before the writer can write
  // again. This value should be set by the writer before
  // posting to can_read. reader_mut must be held when
  // reading.
  int64_t max_readers = 0;
  // Readers increment once they are done reading. Once this value reaches
  // max_readers, the last reader should signal to the writer.
  int64_t num_reads_remaining = 0;
  // Number of readers currently reading. Not necessary for synchronization,
  // but useful for debugging.
  int64_t num_readers_acquired = 0;
  // The valid data size of the Ray object.
  // Normally, Plasma object is immutable, and it is equivalent to the
  // data buffer size. However, this can be overwritten when the plasma object
  // is mutable (E.g., when accerlated DAG is used).
  // This should not be modified or accessed directly.
  uint64_t data_size = 0;
  uint64_t metadata_size = 0;  // TODO is it safe to grow this value?

  void Init();
  void Destroy();
  // Blocks until there are no more readers.
  // NOTE: This does not protect against multiple writers.
  /// \param write_version The new version for write.
  /// \param new_size The new data size of the object.
  /// \return whether the acquire was successful.
  bool WriteAcquire(int64_t write_version, uint64_t new_size, uint64_t new_metadata_size, bool try_acquire);
  // Call after completing a write to signal to max_readers many readers.
  void WriteRelease(int64_t write_version, int64_t max_readers);
  // Blocks until the given version is ready to read.
  int64_t ReadAcquire(int64_t read_version);
  // Finishes the read. If all reads are done, signals to the
  // writer. This is not necessary to call for objects that have
  // max_readers=-1.
  void ReadRelease(int64_t read_version);
  // Get the data size of the plasma object.
  // This has to be called only when reader lock is acquired
  // via ReadAcquire.
  uint64_t GetDataSize() const;
  uint64_t GetMetadataSize() const;
};

/// A struct that includes info about the object.
struct ObjectInfo {
  ObjectID object_id;
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
