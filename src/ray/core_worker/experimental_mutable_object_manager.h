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
#include <string>
#include <vector>

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

class MutableObjectManager {
 public:
  struct Channel {
    Channel(std::unique_ptr<plasma::MutableObject> mutable_object_ptr)
        : mutable_object(std::move(mutable_object_ptr)) {}

    std::unique_ptr<plasma::MutableObject> mutable_object;
  } ABSL_CACHELINE_ALIGNED;

  struct WriterChannel : public Channel {
    WriterChannel(std::unique_ptr<plasma::MutableObject> mutable_object_ptr)
        : Channel(std::move(mutable_object_ptr)) {}

    // WriteAcquire() sets this to true. WriteRelease() sets this to false.
    bool written = false;
  };

  // Thread-safe for multiple ReadAcquire threads and one ReadRelease thread.
  struct ReaderChannel : public Channel {
    ReaderChannel(std::unique_ptr<plasma::MutableObject> mutable_object_ptr)
        : Channel(std::move(mutable_object_ptr)), lock(std::make_unique<absl::Mutex>()) {}

    /// This mutex protects `next_version_to_read`.
    std::unique_ptr<absl::Mutex> lock;
    /// The last version that we read. To read again, we must pass a newer
    /// version than this.
    int64_t next_version_to_read = 1;
  };

  MutableObjectManager() = default;
  ~MutableObjectManager();

  /// Registers the caller as a writer for the channel.
  ///
  /// \param[in] object_id The ID of the object.
  /// \param[in] mutable_object Contains pointers for the object
  /// header, which is used to synchronize with other writers and readers, and
  /// the object data and metadata, which is read by the application.  \return
  /// The return status. The function is not idempotent and it returns Invalid
  /// if it is called twice.
  Status RegisterWriterChannel(const ObjectID &object_id,
                               std::unique_ptr<plasma::MutableObject> mutable_object);

  /// Registers the caller as a reader for the channel.
  ///
  /// \param[in] object_id The ID of the object.
  /// \param[in] mutable_object Contains pointers for the object
  /// header, which is used to synchronize with other writers and readers, and
  /// the object data and metadata, read by the application.
  /// The return status. The function is not idempotent and it returns Invalid
  /// if it is called twice.
  Status RegisterReaderChannel(const ObjectID &object_id,
                               std::unique_ptr<plasma::MutableObject> mutable_object);

  /// Checks if a writer channel is registered for an object.
  ///
  /// \param[in] object_id The ID of the object.
  /// The return status. True if the writer channel is registered for object_id, false
  /// otherwise.
  bool WriterChannelRegistered(const ObjectID &object_id) {
    return GetChannel<WriterChannel>(writer_channels_, object_id);
  }

  /// Checks if a reader channel is registered for an object.
  ///
  /// \param[in] object_id The ID of the object.
  /// The return status. True if the reader channel is registered for object_id, false
  /// otherwise.
  bool ReaderChannelRegistered(const ObjectID &object_id) {
    return GetChannel<ReaderChannel>(reader_channels_, object_id);
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

 private:
  template <typename T>
  Status RegisterChannel(absl::flat_hash_map<ObjectID, T> &channels,
                         const ObjectID &object_id,
                         std::unique_ptr<plasma::MutableObject> &mutable_object);

#if defined(__APPLE__) || defined(__linux__)
  template <typename T>
  T *GetChannel(absl::flat_hash_map<ObjectID, T> &channels, const ObjectID &object_id) {
    auto entry = channels.find(object_id);
    if (entry == channels.end()) {
      return nullptr;
    }
    return &entry->second;
  }
#else
  template <typename T>
  T *GetChannel(absl::flat_hash_map<ObjectID, T> &channels, const ObjectID &object_id) {
    return nullptr;
  }
#endif

  // Returns the plasma object header for the object.
  PlasmaObjectHeader *GetHeader(const ObjectID &object_id);

  // Returns the unique semaphore name for the object. This name is intended to be used
  // for the object's named sempahores.
  std::string GetSemaphoreName(const ObjectID &object_id);

  // Opens named semaphores for the object. This method must be called before
  // `GetSemaphores()`.
  void OpenSemaphores(const ObjectID &object_id);

  // Returns the named semaphores for the object. `OpenSemaphores()` must be called
  // before this method.
  PlasmaObjectHeader::Semaphores GetSemaphores(const ObjectID &object_id);

  // Closes, unlinks, and destroys the named semaphores for the object. Note that the
  // destructor calls this method for all remaining objects.
  void DestroySemaphores(const ObjectID &object_id);

  FRIEND_TEST(MutableObjectTest, TestBasic);
  FRIEND_TEST(MutableObjectTest, TestMultipleReaders);
  FRIEND_TEST(MutableObjectTest, TestWriterFails);
  FRIEND_TEST(MutableObjectTest, TestWriterFailsAfterAcquire);
  FRIEND_TEST(MutableObjectTest, TestReaderFails);
  FRIEND_TEST(MutableObjectTest, TestWriteAcquireDuringFailure);
  FRIEND_TEST(MutableObjectTest, TestReadAcquireDuringFailure);
  FRIEND_TEST(MutableObjectTest, TestReadMultipleAcquireDuringFailure);

  // TODO(swang): Access to these maps is not threadsafe. This is fine in the
  // case that all channels are initialized before any reads and writes are
  // issued, but may not work if channels are added/removed during run time
  // (i.e., if there are multiple DAGs).
  // TODO(jhumphri): If we do need to synchronize accesses to these maps, we may want to
  // consider using RCU to avoid synchronization overhead in the common case.
  // These two maps hold the channels for readers and writers of mutable objects.
  absl::flat_hash_map<ObjectID, ReaderChannel> reader_channels_;
  absl::flat_hash_map<ObjectID, WriterChannel> writer_channels_;

  // This maps holds the semaphores for each mutable object. The semaphores are used to
  // (1) synchronize accesses to the object header and (2) synchronize readers and writers
  // of the mutable object.
  absl::flat_hash_map<ObjectID, PlasmaObjectHeader::Semaphores> semaphores_;
};

}  // namespace experimental
}  // namespace ray
