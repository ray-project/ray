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

#include "ray/common/buffer.h"
#include "ray/common/ray_object.h"
#include "ray/common/status.h"
#include "ray/object_manager/plasma/client.h"
#include "ray/object_manager/plasma/common.h"
#include "ray/object_manager/plasma/plasma.h"
#include "ray/util/visibility.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {

class ExperimentalMutableObjectManager {
 public:
  ExperimentalMutableObjectManager() {}

  /// Register the caller as a writer for the channel.
  ///
  /// \param[in] object_id The ID of the object.
  /// \param[in] mutable_object Struct containing pointers for the object
  /// header, which is used to synchronize with other writers and readers, and
  /// the object data and metadata, which is read by the application.  \return
  /// The return status. The function is not idempotent and it returns Invalid
  /// if it is called twice
  Status RegisterWriterChannel(const ObjectID &object_id,
                               std::unique_ptr<plasma::MutableObject> mutable_object);

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
                      std::shared_ptr<Buffer> *data);

  /// Releases an acquired write lock on the object, allowing readers to read.
  /// This is the equivalent of "Seal" for normal objects.
  ///
  /// \param[in] object_id The ID of the object.
  /// \return The return status.
  Status WriteRelease(const ObjectID &object_id);

  /// Register the caller as a writer for the channel.
  ///
  /// \param[in] object_id The ID of the object.
  /// \param[in] mutable_object Struct containing pointers for the object
  /// header, which is used to synchronize with other writers and readers, and
  /// the object data and metadata, read by the application.
  /// The return status. The function is not idempotent and it returns Invalid
  /// if it is called twice
  Status RegisterReaderChannel(const ObjectID &object_id,
                               std::unique_ptr<plasma::MutableObject> mutable_object);

  /// Return whether the caller is registered as a reader for this object.
  ///
  /// \return Whether the reader is registered.
  bool ReaderChannelRegistered(const ObjectID &object_id) const;

  /// Acquires a read lock on the object that prevents the writer from writing
  /// again until we are done reading the current value.
  ///
  /// \param[in] object_id The ID of the object.
  /// \param[out] result The read object. This buffer is guaranteed to be valid
  /// until the caller calls ReadRelease next.
  /// \return The return status. The ReadAcquire can fail if there have already
  /// been `num_readers` for the current value.
  Status ReadAcquire(const ObjectID &object_id, std::shared_ptr<RayObject> *result);

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
  struct WriterChannel {
    WriterChannel(std::unique_ptr<plasma::MutableObject> mutable_object_ptr)
        : mutable_object(std::move(mutable_object_ptr)) {}

    /// Whether the object is sealed, i.e. whether the last writer still needs
    /// to call WriteRelease.
    bool is_sealed = true;
    std::unique_ptr<plasma::MutableObject> mutable_object;
  };

  struct ReaderChannel {
    ReaderChannel(std::unique_ptr<plasma::MutableObject> mutable_object_ptr)
        : mutable_object(std::move(mutable_object_ptr)) {}

    /// The last version that we read. To read again, we must pass a newer
    /// version than this.
    int64_t next_version_to_read = 1;
    /// Whether we currently have a read lock on the object. If this is true,
    /// then it is safe to read the value of the object. For immutable objects,
    /// this will always be true once the object has been sealed. For mutable
    /// objects, ReadRelease resets this to false, and ReadAcquire resets to
    /// true.
    bool read_acquired = false;
    std::unique_ptr<plasma::MutableObject> mutable_object;
  };

  Status EnsureGetAcquired(ReaderChannel &channel);

  /// All channels for which we are registered as a writer. This can overlap
  /// with reader channels (e.g., if the CoreWorker is multithreaded and one
  /// thread reads while the other writes).
  absl::flat_hash_map<ObjectID, WriterChannel> writer_channels_;
  /// All channels for which we are registered as a reader.
  absl::flat_hash_map<ObjectID, ReaderChannel> reader_channels_;
};

}  // namespace ray
