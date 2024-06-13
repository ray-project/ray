// Copyright 2017 The Ray Authors.
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

#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "absl/container/node_hash_map.h"
#include "gtest/gtest_prod.h"
#include "ray/common/buffer.h"
#include "ray/common/ray_object.h"
#include "ray/common/status.h"
#include "ray/object_manager/plasma/client.h"
#include "ray/object_manager/plasma/common.h"
#include "ray/object_manager/plasma/plasma.h"
#include "ray/util/visibility.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {
namespace experimental {

class MutableObjectManager : public std::enable_shared_from_this<MutableObjectManager> {
 public:
  /// Buffer for a mutable object. This buffer wraps a shared memory buffer of
  /// a mutable object, and read-releases the mutable object when it is destructed.
  /// This auto-releasing behavior enables a cleaner API for accelerated DAG so that
  /// manual calls to ReadRelease() are not needed.
  class MutableObjectBuffer : public SharedMemoryBuffer {
   public:
    MutableObjectBuffer(std::shared_ptr<MutableObjectManager> mutable_object_manager,
                        std::shared_ptr<Buffer> buffer,
                        const ObjectID &object_id)
        : SharedMemoryBuffer(buffer, 0, buffer->Size()),
          mutable_object_manager_(mutable_object_manager),
          object_id_(object_id) {}

    ~MutableObjectBuffer() {
      RAY_UNUSED(mutable_object_manager_->ReadRelease(object_id_));
    }

    const ObjectID &object_id() const { return object_id_; }

   private:
    std::shared_ptr<MutableObjectManager> mutable_object_manager_;
    ObjectID object_id_;
  };

  struct Channel {
    Channel(std::unique_ptr<plasma::MutableObject> mutable_object_ptr)
        : lock(std::make_unique<std::mutex>()),
          mutable_object(std::move(mutable_object_ptr)) {}

    // WriteAcquire() sets this to true. WriteRelease() sets this to false.
    bool written = false;
    // ReadAcquire() sets this to true. ReadRelease() sets this to false. This is used by
    // the destructor to determine if the channel lock must be unlocked. This is necessary
    // if a reader exits after calling ReadAcquire() and before calling ReadRelease().
    bool reading = false;

    // This mutex protects `next_version_to_read`.
    std::unique_ptr<std::mutex> lock;
    // The last version that we read. To read again, we must pass a newer
    // version than this.
    int64_t next_version_to_read = 1;

    bool reader_registered = false;
    bool writer_registered = false;

    std::unique_ptr<plasma::MutableObject> mutable_object;
  } ABSL_CACHELINE_ALIGNED;

  MutableObjectManager() = default;
  ~MutableObjectManager();

  /// Registers a channel for `object_id`.
  ///
  /// \param[in] object_id The ID of the object.
  /// \param[in] mutable_object Contains pointers for the object
  /// header, which is used to synchronize with other writers and readers, and
  /// the object data and metadata, which is read by the application.
  /// \param[in] reader True if the reader is registering this channel. False if the
  /// writer is registering this channel.
  /// \return The return status.
  Status RegisterChannel(const ObjectID &object_id,
                         std::unique_ptr<plasma::MutableObject> mutable_object,
                         bool reader);

  /// Checks if a channel is registered for an object.
  ///
  /// \param[in] object_id The ID of the object.
  /// The return status. True if the channel is registered for object_id, false otherwise.
  bool ChannelRegistered(const ObjectID &object_id) { return GetChannel(object_id); }

  /// Checks if a reader channel is registered for an object.
  ///
  /// \param[in] object_id The ID of the object.
  /// The return status. True if the channel is registered as a reader for object_id,
  /// false otherwise.
  bool ReaderChannelRegistered(const ObjectID &object_id) {
    Channel *c = GetChannel(object_id);
    if (!c) {
      return false;
    }
    return c->reader_registered;
  }

  /// Checks if a writer channel is registered for an object.
  ///
  /// \param[in] object_id The ID of the object.
  /// The return status. True if the channel is registered as a writer for object_id,
  /// false otherwise.
  bool WriterChannelRegistered(const ObjectID &object_id) {
    Channel *c = GetChannel(object_id);
    if (!c) {
      return false;
    }
    return c->writer_registered;
  }

  /// Acquires a write lock on the object that prevents readers from reading
  /// until we are done writing. This is safe for concurrent writers.
  ///
  /// \param[in] object_id The ID of the object.
  /// \param[in] data_size The size of the object to write. This overwrites the
  /// current data size.
  /// \param[in] metadata A pointer to the object metadata buffer to copy. This
  /// will overwrite the current metadata.
  /// \param[in] metadata_size The number of bytes to copy from the metadata
  /// pointer.
  /// \param[in] num_readers The number of readers that must read and release
  /// value we will write before the next WriteAcquire can proceed. The readers
  /// may not start reading until WriteRelease is called.
  /// \param[out] data The mutable object buffer in plasma that can be written to.
  /// \return The return status.
  Status WriteAcquire(const ObjectID &object_id,
                      int64_t data_size,
                      const uint8_t *metadata,
                      int64_t metadata_size,
                      int64_t num_readers,
                      std::shared_ptr<Buffer> &data);

  /// Releases an acquired write lock on the object, allowing readers to read.
  /// This is the equivalent of "Seal" for normal objects.
  ///
  /// \param[in] object_id The ID of the object.
  /// \return The return status.
  Status WriteRelease(const ObjectID &object_id);

  /// Acquires a read lock on the object that prevents the writer from writing
  /// again until we are done reading the current value.
  ///
  /// \param[in] object_id The ID of the object.
  /// \param[out] result The read object. This buffer is guaranteed to be valid
  /// until the caller calls ReadRelease next.
  /// \return The return status. The ReadAcquire can fail if there have already
  /// been `num_readers` for the current value.
  Status ReadAcquire(const ObjectID &object_id, std::shared_ptr<RayObject> &result);

  /// Releases the object, allowing it to be written again. If the caller did
  /// not previously ReadAcquire the object, then this first blocks until the
  /// latest value is available to read, then releases the value.
  ///
  /// \param[in] object_id The ID of the object.
  Status ReadRelease(const ObjectID &object_id);

  /// Sets the error bit, causing all future readers and writers to raise an
  /// error on acquire.
  ///
  /// \param[in] object_id The ID of the object.
  Status SetError(const ObjectID &object_id);

  /// Sets the error bit on all channels, causing all future readers and writers to raise
  /// an error on acquire.
  Status SetErrorAll();

  Channel *GetChannel(const ObjectID &object_id);

 private:
  // Returns the plasma object header for the object.
  PlasmaObjectHeader *GetHeader(const ObjectID &object_id);

  // Returns the unique semaphore name for the object. This name is intended to be used
  // for the object's named sempahores.
  std::string GetSemaphoreName(PlasmaObjectHeader *header);

  // Opens named semaphores for the object. This method must be called before
  // `GetSemaphores()`.
  void OpenSemaphores(const ObjectID &object_id, PlasmaObjectHeader *header);

  // Returns the named semaphores for the object. `OpenSemaphores()` must be called
  // before this method.
  bool GetSemaphores(const ObjectID &object_id, PlasmaObjectHeader::Semaphores &sem);

  // Closes, unlinks, and destroys the named semaphores for the object. Note that the
  // destructor calls this method for all remaining objects.
  void DestroySemaphores(const ObjectID &object_id);

  // Internal method used to set the error bit on `object_id`. The destructor lock must be
  // held before calling this method.
  Status SetErrorInternal(const ObjectID &object_id)
      ABSL_SHARED_LOCKS_REQUIRED(destructor_lock_);

  FRIEND_TEST(MutableObjectTest, TestBasic);
  FRIEND_TEST(MutableObjectTest, TestMultipleReaders);
  FRIEND_TEST(MutableObjectTest, TestWriterFails);
  FRIEND_TEST(MutableObjectTest, TestWriterFailsAfterAcquire);
  FRIEND_TEST(MutableObjectTest, TestReaderFails);
  FRIEND_TEST(MutableObjectTest, TestWriteAcquireDuringFailure);
  FRIEND_TEST(MutableObjectTest, TestReadAcquireDuringFailure);
  FRIEND_TEST(MutableObjectTest, TestReadMultipleAcquireDuringFailure);

  // TODO(jhumphri): If we do need to synchronize accesses to this map, we may want to
  // consider using RCU to avoid synchronization overhead in the common case.
  // This map holds the channels for readers and writers of mutable objects.
  absl::Mutex channel_lock_;
  // `channels_` requires pointer stability as one thread may hold a Channel pointer while
  // another thread mutates `channels_`. Thus, we use absl::node_hash_map instead of
  // absl::flat_hash_map.
  absl::node_hash_map<ObjectID, Channel> channels_;

  // This maps holds the semaphores for each mutable object. The semaphores are used to
  // (1) synchronize accesses to the object header and (2) synchronize readers and writers
  // of the mutable object.
  absl::flat_hash_map<ObjectID, PlasmaObjectHeader::Semaphores> semaphores_;

  // This lock ensures that the destructor does not start tearing down the manager and
  // freeing the memory until all readers and writers are outside the Acquire()/Release()
  // functions. Without this lock, readers and writers still inside those methods could
  // see inconsistent state or access freed memory.
  //
  // The calling threads are all readers and writers, along with the thread that calls the
  // destructor.
  absl::Mutex destructor_lock_;
};

}  // namespace experimental
}  // namespace ray
