#ifndef RAY_OBJECT_MANAGER_OBJECT_BUFFER_POOL_H
#define RAY_OBJECT_MANAGER_OBJECT_BUFFER_POOL_H

#include <list>
#include <memory>
#include <mutex>
#include <vector>

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/bind.hpp>

#include "plasma/client.h"
#include "plasma/events.h"
#include "plasma/plasma.h"

#include "ray/id.h"
#include "ray/status.h"

namespace ray {

/// \class ObjectBufferPool Exposes chunks of object buffers for use by the ObjectManager.
class ObjectBufferPool {
 public:
  /// Information needed about each object chunk.
  /// This is the structure returned whenever an object chunk is
  /// retrieved.
  struct ChunkInfo {
    ChunkInfo() {}
    ChunkInfo(uint64_t chunk_index,
              uint8_t *data,
              uint64_t buffer_length)
        : chunk_index(chunk_index),
          data(data),
          buffer_length(buffer_length){};
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
  ObjectBufferPool(const std::string &store_socket_name, const uint64_t chunk_size, const int release_delay);

  ~ObjectBufferPool();

  /// This object cannot be copied due to pool_mutex.
  RAY_DISALLOW_COPY_AND_ASSIGN(ObjectBufferPool);

  /// \param data_size The size of the object + metadata.
  /// \return The number of chunks into which the object will be split.
  uint64_t GetNumChunks(uint64_t data_size);

  /// \param chunk_index The chunk index for which to obtain the buffer length.
  /// \param data_size The size of the object + metadata.
  /// \return The number of chunks into which the object will be split.
  uint64_t GetBufferLength(uint64_t chunk_index, uint64_t data_size);

  /// Returns a chunk of an object at the given chunk_index. The object chunk serves
  /// as the data that is to be written to a connection as part of sending an object to
  /// a remote node.
  ///
  /// \param object_id The ObjectID.
  /// \param data_size The sum of the object size and metadata size.
  /// \param metadata_size The size of the metadata.
  /// \param chunk_index The index of the chunk.
  /// \return A pair consisting of a ChunkInfo struct and status of invoking this method.
  std::pair<const ObjectBufferPool::ChunkInfo &, ray::Status> GetChunk(const ObjectID &object_id, uint64_t data_size,
                            uint64_t metadata_size, uint64_t chunk_index);

  /// When a chunk is done being used as part of a get, this method is invoked
  /// to indicate that the buffer associated with a chunk is no longer needed.
  ///
  /// \param object_id The object_id of the buffer to release.
  /// \return The status of invoking this method.
  ray::Status ReleaseGetChunk(const ObjectID &object_id, uint64_t chunk_index);

  /// Returns a chunk of an empty object at the given chunk_index. The object chunk
  /// serves as the data that is to be written to by a connection receiving an object
  /// from a remote node.
  ///
  /// \param object_id The ObjectID.
  /// \param data_size The sum of the object size and metadata size.
  /// \param metadata_size The size of the metadata.
  /// \param chunk_index The index of the chunk.
  /// \return A ChunkInfo struct.
  std::pair<const ObjectBufferPool::ChunkInfo &, ray::Status> CreateChunk(const ObjectID &object_id, uint64_t data_size,
                               uint64_t metadata_size, uint64_t chunk_index);

  ray::Status ReleaseCreateChunk(const ObjectID &object_id, uint64_t chunk_index);

  /// Seal the object associated with a create operation.
  ///
  /// \param object_id The ObjectID.
  /// \return The status of invoking this method.
  ray::Status SealChunk(const ObjectID &object_id, uint64_t chunk_index);

  /// Abort the create operation associated with an object.
  ray::Status AbortCreate(const ObjectID &object_id);

 private:

  /// Abort the get operation associated with an object.
  ray::Status AbortGet(const ObjectID &object_id);

  /// Builds the chunk vector for an object, and store it by object_id in chunk_info_.
  /// Returns the number of chunks into which object is split.
  std::vector<ChunkInfo> BuildChunks(const ObjectID &object_id, uint8_t *data, uint64_t data_size,
                                     uint64_t metadata_size);

  /// Holds the state of a get buffer.
  struct GetBufferState {
    GetBufferState() {}
    GetBufferState(std::vector<ChunkInfo> chunk_info)
        : chunk_info(chunk_info),
          chunk_references(chunk_info.size(), 0){
    }
    /// A vector maintaining information about the chunks which comprise
    /// an object.
    std::vector<ChunkInfo> chunk_info;
    /// Reference counts for each chunk.
    std::vector<uint64_t> chunk_references;
    /// The number of references that currently rely on this buffer.
    /// We expect this many calls to Release or SealOrAbortBuffer.
    uint64_t references = 0;
  };

  /// Holds the state of a create buffer.
  struct CreateBufferState {
    CreateBufferState() {}
    CreateBufferState(std::vector<ChunkInfo> chunk_info)
        : chunk_info(chunk_info),
          chunk_references(chunk_info.size(), 0),
          num_chunks_remaining(chunk_info.size()) {}
    /// A vector maintaining information about the chunks which comprise
    /// an object.
    std::vector<ChunkInfo> chunk_info;
    /// Reference counts for each chunk.
    std::vector<uint64_t> chunk_references;
    /// The number of references that currently rely on this buffer.
    /// We expect this many calls to Release or SealOrAbortBuffer.
    uint64_t num_chunks_remaining;
  };

  /// Returned when Get fails.
  ChunkInfo errored_chunk_ = {0, nullptr, 0};

  /// Mutex for thread-safe operations.
  std::mutex pool_mutex_;
  /// Determines the maximum chunk size to be transferred by a single thread.
  uint64_t chunk_size_;
  /// The state of a buffer that's currently being used.
  std::unordered_map<ray::ObjectID, GetBufferState, ray::UniqueIDHasher> get_buffer_state_;
  /// The state of a buffer that's currently being used.
  std::unordered_map<ray::ObjectID, CreateBufferState, ray::UniqueIDHasher> create_buffer_state_;

  /// Plasma client pool.
  plasma::PlasmaClient store_client_;
  /// Socket name of plasma store.
  std::string store_socket_name_;
};

}  // namespace ray

#endif  // RAY_OBJECT_MANAGER_OBJECT_BUFFER_POOL_H
