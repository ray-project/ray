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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/object_manager/memory_object_reader.h"

namespace ray {

/// \class ObjectBufferPool Exposes chunks of object buffers for use by the ObjectManager.
class ObjectBufferPool {
 public:
  /// Information needed to read or write an object chunk.
  /// This is the structure returned whenever an object chunk is
  /// accessed via Get and Create.
  struct ChunkInfo {
    ChunkInfo(uint64_t chunk_index,
              uint8_t *data,
              uint64_t buffer_length,
              std::shared_ptr<Buffer> buffer_ref)
        : chunk_index_(chunk_index),
          data_(data),
          buffer_length_(buffer_length),
          buffer_ref_(std::move(buffer_ref)){};
    /// The index of this object chunk within the object, starting with 0.
    uint64_t chunk_index_;
    /// A pointer to the start position of this object chunk.
    uint8_t *data_;
    /// The size of this object chunk.
    uint64_t buffer_length_;
    /// A shared reference to the underlying buffer, keeping it alive.
    std::shared_ptr<Buffer> buffer_ref_;
  };

  /// Constructor.
  ///
  /// \param store_client Plasma store client. Used for testing purposes only.
  /// \param chunk_size The chunk size into which objects are to be split.
  ObjectBufferPool(std::shared_ptr<plasma::PlasmaClientInterface> store_client,
                   const uint64_t chunk_size);

  ~ObjectBufferPool();

  /// This object cannot be copied due to pool_mutex.
  ObjectBufferPool(const ObjectBufferPool &) = delete;
  ObjectBufferPool &operator=(const ObjectBufferPool &) = delete;

  /// Computes the number of chunks needed to transfer an object and its metadata.
  ///
  /// \param data_size The size of the object + metadata.
  /// \return The number of chunks into which the object will be split.
  uint64_t GetNumChunks(uint64_t data_size) const;

  /// Computes the buffer length of a chunk of an object.
  ///
  /// \param chunk_index The chunk index for which to obtain the buffer length.
  /// \param data_size The size of the object + metadata.
  /// \return The buffer length of the chunk at chunk_index.
  uint64_t GetBufferLength(uint64_t chunk_index, uint64_t data_size) const;

  /// Returns an object reader for read.
  ///
  /// \param object_id The ObjectID.
  /// \param owner_address The address of the object's owner.
  /// \return A pair consisting of a MemoryObjectReader and status of invoking
  /// this method. An IOError status is returned if the Get call on the plasma store
  /// fails, and the MemoryObjectReader will be empty.
  std::pair<std::shared_ptr<MemoryObjectReader>, Status> CreateObjectReader(
      const ObjectID &object_id, rpc::Address owner_address)
      ABSL_LOCKS_EXCLUDED(pool_mutex_);

  /// Returns a chunk of an empty object at the given chunk_index. The object chunk
  /// serves as the buffer that is to be written to by a connection receiving an
  /// object from a remote node. Only one thread is permitted to create the object
  /// chunk at chunk_index. Multiple threads attempting to create the same object
  /// chunk will result in one succeeding. The ObjectManager is responsible for
  /// handling create failures. This method will fail if it's invoked on a chunk_index
  /// on which WriteChunk has already been invoked.
  ///
  /// \param object_id The ObjectID.
  /// \param owner_address The address of the object's owner.
  /// \param data_size The sum of the object size and metadata size.
  /// \param metadata_size The size of the metadata.
  /// \param chunk_index The index of the chunk.
  /// \return status of invoking this method.
  /// An IOError status is returned if object creation on the store client fails,
  /// or if create is invoked consecutively on the same chunk
  /// (with no intermediate AbortCreateChunk).
  Status CreateChunk(const ObjectID &object_id,
                     const rpc::Address &owner_address,
                     uint64_t data_size,
                     uint64_t metadata_size,
                     uint64_t chunk_index) ABSL_LOCKS_EXCLUDED(pool_mutex_);

  /// Write to a Chunk of an object. If all chunks of an object is written,
  /// it seals the object.
  ///
  /// This method will fail if it's invoked on a chunk_index on which
  /// CreateChunk was not first invoked, or a chunk_index on which
  /// WriteChunk has already been invoked.
  ///
  /// \param object_id The ObjectID.
  /// \param chunk_index The index of the chunk.
  /// \param data The data to write into the chunk.
  void WriteChunk(const ObjectID &object_id,
                  uint64_t data_size,
                  uint64_t metadata_size,
                  uint64_t chunk_index,
                  const std::string &data) ABSL_LOCKS_EXCLUDED(pool_mutex_);

  /// Free a list of objects from object store.
  ///
  /// \param object_ids the The list of ObjectIDs to be deleted.
  void FreeObjects(const std::vector<ObjectID> &object_ids)
      ABSL_LOCKS_EXCLUDED(pool_mutex_);

  /// Abort the create operation associated with an object. This destroys the buffer
  /// state, including create operations in progress for all chunks of the object.
  void AbortCreate(const ObjectID &object_id) ABSL_LOCKS_EXCLUDED(pool_mutex_);

  /// Returns debug string for class.
  ///
  /// \return string.
  std::string DebugString() const ABSL_LOCKS_EXCLUDED(pool_mutex_);

 private:
  /// Splits an object into ceil(data_size/chunk_size) chunks, which will
  /// either be read or written to in parallel.
  std::vector<ChunkInfo> BuildChunks(const ObjectID &object_id,
                                     uint8_t *data,
                                     uint64_t data_size,
                                     std::shared_ptr<Buffer> buffer_ref)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(pool_mutex_);

  /// Ensures buffer for the object exists, and creates the buffer if needed.
  /// Returns OK if buffer exists.
  /// Must hold pool_mutex_ when calling this function. pool_mutex_ can be released
  /// during the call.
  Status EnsureBufferExists(const ObjectID &object_id,
                            const rpc::Address &owner_address,
                            uint64_t data_size,
                            uint64_t metadata_size,
                            uint64_t chunk_index)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(pool_mutex_);

  void AbortCreateInternal(const ObjectID &object_id)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(pool_mutex_);

  /// The state of a chunk associated with a create operation.
  enum class CreateChunkState : uint8_t { AVAILABLE = 0, REFERENCED, SEALED };

  /// Holds the state of creating chunks. Members are protected by pool_mutex_.
  struct CreateBufferState {
    CreateBufferState(uint64_t metadata_size,
                      uint64_t data_size,
                      std::vector<ChunkInfo> chunk_info)
        : metadata_size_(metadata_size),
          data_size_(data_size),
          chunk_info_(std::move(chunk_info)),
          chunk_state_(chunk_info_.size(), CreateChunkState::AVAILABLE),
          num_seals_remaining_(chunk_info_.size()) {}
    /// Total size of the object metadata.
    uint64_t metadata_size_;
    /// Total size of the object data.
    uint64_t data_size_;
    /// A vector maintaining information about the chunks which comprise
    /// an object.
    std::vector<ChunkInfo> chunk_info_;
    /// The state of each chunk, which is used to enforce strict state
    /// transitions of each chunk.
    std::vector<CreateChunkState> chunk_state_;
    /// The number of chunks left to seal before the buffer is sealed.
    uint64_t num_seals_remaining_;
    /// The number of inflight copy operations.
    uint64_t num_inflight_copies_ = 0;
  };

  /// Returned when GetChunk or CreateChunk fails.
  const ChunkInfo errored_chunk_ = {0, nullptr, 0, nullptr};

  /// Mutex to protect create_buffer_ops_, create_buffer_state_ and following invariants:
  /// - create_buffer_ops_ contains an object_id iff there is an inflight operation to
  ///   create the buffer for the object.
  /// - An object_id cannot appear in both create_buffer_ops_ and create_buffer_state_.
  mutable absl::Mutex pool_mutex_;
  /// Makes sure each object has at most one inflight create buffer operation.
  /// Other operations can wait on the std::condition_variable for the operation
  /// to complete. If successful, the corresponding entry in create_buffer_state_
  /// will be created.
  absl::flat_hash_map<ObjectID, std::shared_ptr<absl::CondVar>> create_buffer_ops_
      ABSL_GUARDED_BY(pool_mutex_);
  /// The state of a buffer that's currently being used.
  absl::flat_hash_map<ObjectID, CreateBufferState> create_buffer_state_
      ABSL_GUARDED_BY(pool_mutex_);

  /// Plasma client pool.
  std::shared_ptr<plasma::PlasmaClientInterface> store_client_;

  /// Determines the maximum chunk size to be transferred by a single thread.
  const uint64_t default_chunk_size_;

  friend class ObjectBufferPoolTest;
};

}  // namespace ray
