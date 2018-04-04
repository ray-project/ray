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
    ChunkInfo(uint8_t *data, uint64_t buffer_length, uint64_t data_size,
              uint64_t metadata_size, ray::Status status)
        : data(data),
          buffer_length(buffer_length),
          data_size(data_size),
          metadata_size(metadata_size),
          status(status){};
    /// A pointer to the start position of this object chunk.
    uint8_t *data;
    /// The size of this object chunk.
    uint64_t buffer_length;
    /// The size of the entire object + metadata.
    uint64_t data_size;
    /// The size of the metadata.
    uint64_t metadata_size;
    /// The status of the returned chunk.
    ray::Status status;
  };

  /// Constructor.
  ///
  /// \param store_socket_name The socket name of the store to which plasma clients
  /// connect.
  /// \param chunk_size The chunk size into which objects are to be split.
  ObjectBufferPool(const std::string &store_socket_name, const uint64_t chunk_size);

  /// This object cannot be copied due to pool_mutex.
  RAY_DISALLOW_COPY_AND_ASSIGN(ObjectBufferPool);

  /// \param data_size The size of the object + metadata.
  /// \return The number of chunks into which the object will be split.
  uint64_t GetNumChunks(uint64_t data_size);

  /// Returns a chunk of an object at the given chunk_index. The object chunk serves
  /// as the data that is to be written to a connection as part of sending an object to
  /// a remote node.
  ///
  /// \param object_id The ObjectID.
  /// \param data_size The sum of the object size and metadata size.
  /// \param metadata_size The size of the metadata.
  /// \param chunk_index The index of the chunk.
  /// \return A ChunkInfo struct.
  const ChunkInfo &GetChunk(const ObjectID &object_id, uint64_t data_size,
                            uint64_t metadata_size, uint64_t chunk_index);

  /// When a chunk is done being used as part of a get, this method is invoked
  /// to indicate that the buffer associated with a chunk is no longer needed.
  ///
  /// \param object_id The object_id of the buffer to release.
  /// \return The status of invoking this method.
  ray::Status ReleaseBuffer(const ObjectID &object_id);

  /// Returns a chunk of an empty object at the given chunk_index. The object chunk
  /// serves as the data that is to be written to by a connection receiving an object
  /// from a remote node.
  ///
  /// \param object_id The ObjectID.
  /// \param data_size The sum of the object size and metadata size.
  /// \param metadata_size The size of the metadata.
  /// \param chunk_index The index of the chunk.
  /// \return A ChunkInfo struct.
  const ChunkInfo &CreateChunk(const ObjectID &object_id, uint64_t data_size,
                               uint64_t metadata_size, uint64_t chunk_index);

  /// When a chunk is done being used as part of a create, this method is invoked
  /// to indicate that the buffer associated with a chunk is either ready to be
  /// sealed or aborted. Creation is aborted if the call to plasma client Create failed.
  ///
  /// \param object_id The ObjectID.
  /// \param succeeded Whether the operation on the chunk succeeded. If it failed,
  /// object creation will be aborted.
  /// \return The status of invoking this method.
  ray::Status SealOrAbortBuffer(const ObjectID &object_id, bool succeeded);

  /// Terminates this object.
  /// This must be called after remote client connections have been terminated.
  void Terminate();

 private:
  /// Seal the object associated with a create operation.
  ray::Status SealCreate(const ObjectID &object_id);

  /// Abort the create operation associated with an object.
  ray::Status AbortCreate(const ObjectID &object_id);

  /// Abort the get operation associated with an object.
  ray::Status AbortGet(const ObjectID &object_id);

  /// Builds the chunk vector for an object, and store it by object_id in chunk_info_.
  /// Returns the number of chunks into which object is split.
  uint64_t BuildChunks(const ObjectID &object_id, uint8_t *data, uint64_t data_size,
                       uint64_t metadata_size);

  /// Get an object from the object store pool.
  std::shared_ptr<plasma::PlasmaClient> GetObjectStore();

  /// Release an object from the object store pool.
  void ReleaseObjectStore(std::shared_ptr<plasma::PlasmaClient> client);

  /// Adds a client to the client pool and mark it as available.
  void Add();

  /// The type of a buffer state.
  /// Used to detect whether an object buffer is
  /// both a get and create buffer.
  enum class BufferStateType : int { GET = 0, CREATE };

  /// Holds the state of a buffer.
  struct BufferState {
    BufferState() {}
    BufferState(std::shared_ptr<plasma::PlasmaClient> client, uint64_t references,
                BufferStateType state_type)
        : client(client), references(references), state_type(state_type) {}
    /// The plasma client used by this buffer.
    std::shared_ptr<plasma::PlasmaClient> client;
    /// The number of references that currently rely on this buffer.
    /// We expect this many calls to Release or SealOrAbortBuffer.
    uint64_t references;
    /// The type of buffer; either GET or CREATE.
    BufferStateType state_type;
    /// Whether a single failure has occurred.
    bool one_failed = false;
  };

  /// Returned when Get fails.
  ChunkInfo errored_chunk_ = {
      nullptr, 0, 0, 0,
      ray::Status::IOError("Unable to obtain object chunk, object not local.")};

  /// Mutex for thread-safe operations.
  std::mutex pool_mutex_;
  /// Determines the maximum chunk size to be transferred by a single thread.
  uint64_t chunk_size_;
  /// A vector for each object maintaining information about the chunks which comprise
  /// an object. ChunkInfo is used to transfer
  std::unordered_map<ray::ObjectID, std::vector<ChunkInfo>, ray::UniqueIDHasher>
      chunk_info_;
  /// The state of a buffer that's currently being used.
  std::unordered_map<ray::ObjectID, BufferState, ray::UniqueIDHasher> buffer_state_;
  /// A set of vectors into which data is read for objects that failed to be created.
  std::unordered_map<ray::ObjectID, std::vector<uint8_t>, ray::UniqueIDHasher>
      create_failure_buffers_;
  /// Tracks the number of calls to GetChunk that should fail after the first call
  /// fails to obtain the buffer.
  std::unordered_map<ray::ObjectID, uint64_t, ray::UniqueIDHasher> get_failed_count_;

  /// Available plasma clients.
  std::vector<std::shared_ptr<plasma::PlasmaClient>> available_clients;
  /// Plasma client pool.
  std::vector<std::shared_ptr<plasma::PlasmaClient>> clients;
  /// Socket name of plasma store.
  std::string store_socket_name_;
};

}  // namespace ray

#endif  // RAY_OBJECT_MANAGER_OBJECT_BUFFER_POOL_H
