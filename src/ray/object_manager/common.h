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

#if defined(__APPLE__) || defined(__linux__)
#include <semaphore.h>
#endif

#include <atomic>
#include <boost/asio.hpp>
#include <functional>
#include <string>

#include "ray/common/id.h"
#include "ray/common/status.h"

// On Darwin, named semaphores cannot have names longer than PSEMNAMLEN. On other
// platforms, we define PSEMNAMLEN to be 30, which is what seems to work in Darwin.
#ifdef __APPLE__
#include <sys/posix_sem.h>
#elif !defined(PSEMNAMLEN)
#define PSEMNAMLEN 30UL
#endif

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

/// A header for all plasma objects that is allocated and stored in shared
/// memory. Therefore, it can be accessed across processes.
///
/// For normal immutable objects, no synchronization between processes is
/// needed once the object has been Sealed. For experimental mutable objects,
/// we use the header to synchronize between writer and readers.
struct PlasmaObjectHeader {
#if defined(__APPLE__) || defined(__linux__)
  struct Semaphores {
    // Synchronizes readers and writers of the mutable object.
    sem_t *object_sem;
    // Synchronizes accesses to the object header.
    sem_t *header_sem;
  };

  enum class SemaphoresCreationLevel {
    kUnitialized,
    kInitializing,
    kDone,
  };

  // Used to synchronize initialization of the semaphores. Multiple processes may access
  // this mutable object, so this variable lets them coordinate to determine who will
  // initialize the semaphores.
  std::atomic<SemaphoresCreationLevel> semaphores_created =
      SemaphoresCreationLevel::kUnitialized;
  // A unique name for this object. This unique name is used to generate the named
  // semaphores for this object.
  char unique_name[32];
#else  // defined(__APPLE__) || defined(__linux__)
  // Fake types for Windows.
  struct sem_t {};
  struct Semaphores {};
#endif

  // The object version. For immutable objects, this gets incremented to 1 on
  // the first write and then should never be modified. For mutable objects,
  // each new write must increment the version before releasing to readers.
  int64_t version = 0;
  // Indicates whether the current version has been written. is_sealed=false
  // means that there is a writer who has WriteAcquire'd but not yet
  // WriteRelease'd the current version. is_sealed=true means that `version`
  // has been WriteRelease'd. A reader may read the actual object value if
  // is_sealed=true and num_read_acquires_remaining != 0.
  bool is_sealed = false;
  // Set to indicate an error was encountered computing the next version of
  // the mutable object. Lockless access allowed.
  std::atomic<bool> has_error = false;
  // The total number of reads allowed before the writer can write again. This
  // value should be set by the writer before releasing to readers.
  // For immutable objects, this is set to -1 and infinite reads are allowed.
  // Otherwise, readers must acquire/release before/after reading.
  int64_t num_readers = 0;
  // The number of readers who can acquire the current version. For mutable
  // objects, readers must ensure this is > 0 and decrement before they read.
  // Once this value reaches 0, no more readers are allowed until the writer
  // writes a new version.
  // NOTE(swang): Technically we do not need this because
  // num_read_releases_remaining protects against too many readers. However,
  // this allows us to throw an error as soon as the n+1-th reader begins,
  // instead of waiting to error until the n+1-th reader is done reading.
  uint64_t num_read_acquires_remaining = 0;
  // The number of readers who must release the current version before a new
  // version can be written. For mutable objects, readers must decrement this
  // when they are done reading the current version. Once this value reaches 0,
  // the reader should signal to the writer that they can write again.
  uint64_t num_read_releases_remaining = 0;
  // The valid data and metadata size of the Ray object.
  // Not used for immutable objects.
  // For mutable objects, this should be modified when the new object has a
  // different data/metadata size.
  uint64_t data_size = 0;
  uint64_t metadata_size = 0;

  /// Blocks until all readers for the previous write have ReadRelease'd the
  /// value. Protects against concurrent writers.
  ///
  /// \param sem The semaphores for this channel.
  /// \param data_size The new data size of the object.
  /// \param metadata_size The new metadata size of the object.
  /// \param num_readers The number of readers for the object.
  /// \param timeout_point The time point when to timeout if the semaphore is not
  /// acquired. If this is nullptr, then there is no timeout and the method will block
  /// indefinitely until the semaphore is acquired. If timeout_point is already passed,
  /// then the method will try once to acquire the semaphore, and return either OK or
  /// TimedOut immediately without blocking.
  /// \return if the acquire was successful.
  Status WriteAcquire(Semaphores &sem,
                      uint64_t data_size,
                      uint64_t metadata_size,
                      int64_t num_readers,
                      const std::optional<std::chrono::steady_clock::time_point>
                          &timeout_point = std::nullopt);

  /// Call after completing a write to signal that readers may read.
  /// num_readers should be set before calling this.
  ///
  /// \param sem The semaphores for this channel.
  Status WriteRelease(Semaphores &sem);

  /// Blocks until the given version is ready to read. Returns false if the
  /// maximum number of readers have already read the requested version.
  ///
  /// \param[in] object_id ObjectID to acquire a lock from.
  /// \param[in] sem The semaphores for this channel.
  /// \param[in] read_version The version to read.
  /// \param[out] version_read For normal immutable objects, this will be set to
  /// 0. Otherwise, the current version.
  /// \param[in] timeout_point The time point when to timeout if the semaphore is not
  /// acquired. If this is nullptr, then there is no timeout and the method will block
  /// indefinitely until the semaphore is acquired. If timeout_point is already passed,
  /// then the method will try once to acquire the semaphore, and return either OK or
  /// TimedOut immediately without blocking.
  /// \return Whether the correct version was read and there were still
  /// reads remaining.
  Status ReadAcquire(const ObjectID &object_id,
                     Semaphores &sem,
                     int64_t version_to_read,
                     int64_t &version_read,
                     const std::function<Status()> &check_signals,
                     const std::optional<std::chrono::steady_clock::time_point>
                         &timeout_point = std::nullopt);

  // Finishes the read. If all reads are done, signals to the writer. This is
  // not necessary to call for objects that have num_readers=-1.
  ///
  /// \param sem The semaphores for this channel.
  /// \param read_version This must match the version previously passed in
  /// ReadAcquire.
  Status ReadRelease(Semaphores &sem, int64_t read_version);

  /// Set up synchronization primitives.
  void Init();

  /// Helper method to acquire a semaphore while failing if the error bit is set. This
  /// method is idempotent.
  ///
  /// \param sem The semaphore to acquire.
  /// \param timeout_point The time point when to timeout if the semaphore is not
  /// acquired. If this is nullptr, then there is no timeout and the method will block
  /// indefinitely until the semaphore is acquired. If timeout_point is already passed,
  /// then the method will try once to acquire the semaphore, and return either OK or
  /// TimedOut immediately without blocking.
  /// \return OK if the mutex was acquired successfully, TimedOut if timed out.
  Status TryToAcquireSemaphore(
      sem_t *sem,
      const std::optional<std::chrono::steady_clock::time_point> &timeout_point =
          std::nullopt,
      const std::function<Status()> &check_signals = nullptr) const;

  /// Set the error bit. This is a non-blocking method.
  ///
  /// \param sem The semaphores for this channel.
  void SetErrorUnlocked(Semaphores &sem);

  /// Checks if `has_error` has been set.
  ///
  /// \return OK if `has_error` has not been yet, and an IOError otherwise.
  Status CheckHasError() const;
};

/// A struct that includes info about the object.
struct ObjectInfo {
  ObjectID object_id;
  bool is_mutable = false;
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
    return data_size + metadata_size + (is_mutable ? sizeof(PlasmaObjectHeader) : 0);
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
