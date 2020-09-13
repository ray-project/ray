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

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/bind.hpp>
#include <list>
#include <memory>
#include <mutex>
#include <vector>

#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/object_manager/plasma/client.h"

namespace ray {

/// \class ObjectBufferPool Exposes chunks of object buffers for use by the ObjectManager.
class ObjectBufferPool {
 public:
  /// Information needed to read or write an object chunk.
  /// This is the structure returned whenever an object chunk is
  /// accessed via Get and Create.
  struct ChunkInfo {
    ChunkInfo(uint64_t chunk_index, uint8_t *data, uint64_t buffer_length)
        : chunk_index(chunk_index), data(data), buffer_length(buffer_length){};
    /// A pointer to the start position of this object chunk.
    uint64_t chunk_index;
    /// A pointer to the start position of this object chunk.
    uint8_t *data;
    /// The size of this object chunk.
    uint64_t buffer_length;
  };

  /// Constructor.
  ///
  /// \param store_socket_name The socket name of the store to which plasma clients
  /// connect.
  /// \param chunk_size The chunk size into which objects are to be split.
  ObjectBufferPool(const std::string &store_socket_name, const uint64_t chunk_size);

  ~ObjectBufferPool();

  /// This object cannot be copied due to pool_mutex.
  RAY_DISALLOW_COPY_AND_ASSIGN(ObjectBufferPool);

  /// Computes the number of chunks needed to transfer an object and its metadata.
  ///
  /// \param data_size The size of the object + metadata.
  /// \return The number of chunks into which the object will be split.
  uint64_t GetNumChunks(uint64_t data_size);

  /// Computes the buffer length of a chunk of an object.
  ///
  /// \param chunk_index The chunk index for which to obtain the buffer length.
  /// \param data_size The size of the object + metadata.
  /// \return The buffer length of the chunk at chunk_index.
  uint64_t GetBufferLength(uint64_t chunk_index, uint64_t data_size);

  /// Returns a chunk of an object at the given chunk_index. The object chunk serves
  /// as the data that is to be written to a connection as part of sending an object to
  /// a remote node.
  ///
  /// \param object_id The ObjectID.
  /// \param data_size The sum of the object size and metadata size.
  /// \param metadata_size The size of the metadata.
  /// \param chunk_index The index of the chunk.
  /// \return A pair consisting of a ChunkInfo and status of invoking this method.
  /// An IOError status is returned if the Get call on the plasma store fails.
  std::pair<const ObjectBufferPool::ChunkInfo &, ray::Status> GetChunk(
      const ObjectID &object_id, uint64_t data_size, uint64_t metadata_size,
      uint64_t chunk_index);

  /// When a chunk is done being used as part of a get, this method releases the chunk.
  /// If all chunks of an object are released, the object buffer will be released.
  ///
  /// \param object_id The object_id of the buffer to release.
  /// \param chunk_index The index of the chunk.
  void ReleaseGetChunk(const ObjectID &object_id, uint64_t chunk_index);

  /// Returns a chunk of an empty object at the given chunk_index. The object chunk
  /// serves as the buffer that is to be written to by a connection receiving an object
  /// from a remote node. Only one thread is permitted to create the object chunk at
  /// chunk_index. Multiple threads attempting to create the same object chunk will
  /// result in one succeeding. The ObjectManager is responsible for handling
  /// create failures. This method will fail if it's invoked on a chunk_index on which
  /// SealChunk has already been invoked.
  ///
  /// \param object_id The ObjectID.
  /// \param owner_address The address of the object's owner.
  /// \param data_size The sum of the object size and metadata size.
  /// \param metadata_size The size of the metadata.
  /// \param chunk_index The index of the chunk.
  /// \return A pair consisting of ChunkInfo and status of invoking this method.
  /// An IOError status is returned if object creation on the store client fails,
  /// or if create is invoked consecutively on the same chunk
  /// (with no intermediate AbortCreateChunk).
  std::pair<const ObjectBufferPool::ChunkInfo &, ray::Status> CreateChunk(
      const ObjectID &object_id, const rpc::Address &owner_address, uint64_t data_size,
      uint64_t metadata_size, uint64_t chunk_index);

  /// Abort the create operation associated with a chunk at chunk_index.
  /// This method will fail if it's invoked on a chunk_index on which
  /// CreateChunk was not first invoked, or a chunk_index on which
  /// SealChunk has already been invoked.
  ///
  /// \param object_id The ObjectID.
  /// \param chunk_index The index of the chunk.
  void AbortCreateChunk(const ObjectID &object_id, uint64_t chunk_index);

  /// Seal the object associated with a create operation. This is invoked whenever
  /// a chunk is successfully written to.
  /// This method will fail if it's invoked on a chunk_index on which
  /// CreateChunk was not first invoked, or a chunk_index on which
  /// SealChunk or AbortCreateChunk has already been invoked.
  ///
  /// \param object_id The ObjectID.
  /// \param chunk_index The index of the chunk.
  void SealChunk(const ObjectID &object_id, uint64_t chunk_index);

  /// Free a list of objects from object store.
  ///
  /// \param object_ids the The list of ObjectIDs to be deleted.
  /// \return Void.
  void FreeObjects(const std::vector<ObjectID> &object_ids);

  /// Returns debug string for class.
  ///
  /// \return string.
  std::string DebugString() const;

 private:
  /// Abort the create operation associated with an object. This destroys the buffer
  /// state, including create operations in progress for all chunks of the object.
  void AbortCreate(const ObjectID &object_id);

  /// Abort the get operation associated with an object.
  void AbortGet(const ObjectID &object_id);

  /// Splits an object into ceil(data_size/chunk_size) chunks, which will
  /// either be read or written to in parallel.
  std::vector<ChunkInfo> BuildChunks(const ObjectID &object_id, uint8_t *data,
                                     uint64_t data_size);

  /// Holds the state of a get buffer.
  struct GetBufferState {
    GetBufferState() {}
    GetBufferState(std::vector<ChunkInfo> chunk_info) : chunk_info(chunk_info) {}
    /// A vector maintaining information about the chunks which comprise
    /// an object.
    std::vector<ChunkInfo> chunk_info;
    /// The number of references that currently rely on this buffer.
    /// Once this reaches 0, the buffer is released and this object is erased
    /// from get_buffer_state_.
    uint64_t references = 0;
  };

  /// The state of a chunk associated with a create operation.
  enum class CreateChunkState : unsigned int { AVAILABLE = 0, REFERENCED, SEALED };

  /// Holds the state of a create buffer.
  struct CreateBufferState {
    CreateBufferState() {}
    CreateBufferState(std::vector<ChunkInfo> chunk_info)
        : chunk_info(chunk_info),
          chunk_state(chunk_info.size(), CreateChunkState::AVAILABLE),
          num_seals_remaining(chunk_info.size()) {}
    /// A vector maintaining information about the chunks which comprise
    /// an object.
    std::vector<ChunkInfo> chunk_info;
    /// The state of each chunk, which is used to enforce strict state
    /// transitions of each chunk.
    std::vector<CreateChunkState> chunk_state;
    /// The number of chunks left to seal before the buffer is sealed.
    uint64_t num_seals_remaining;
  };

  /// Returned when GetChunk or CreateChunk fails.
  const ChunkInfo errored_chunk_ = {0, nullptr, 0};

  /// Mutex on public methods for thread-safe operations on
  /// get_buffer_state_, create_buffer_state_, and store_client_.
  mutable std::mutex pool_mutex_;
  /// Determines the maximum chunk size to be transferred by a single thread.
  const uint64_t default_chunk_size_;
  /// The state of a buffer that's currently being used.
  std::unordered_map<ray::ObjectID, GetBufferState> get_buffer_state_;
  /// The state of a buffer that's currently being used.
  std::unordered_map<ray::ObjectID, CreateBufferState> create_buffer_state_;

  /// Plasma client pool.
  plasma::PlasmaClient store_client_;
  /// Socket name of plasma store.
  std::string store_socket_name_;
};

}  // namespace ray
