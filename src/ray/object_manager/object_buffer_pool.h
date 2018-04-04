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

class ObjectBufferPool {
 public:
  enum class BufferStateType : int { GET = 0, CREATE };

  struct ChunkInfo {
    ChunkInfo(){}
    ChunkInfo(uint8_t *data,
    uint64_t buffer_length,
    uint64_t data_size,
    uint64_t metadata_size,
    ray::Status status) :
        data(data),
        buffer_length(buffer_length),
        data_size(data_size),
        metadata_size(metadata_size),
        status(status) {};
    uint8_t *data;
    uint64_t buffer_length;
    uint64_t data_size;
    uint64_t metadata_size;
    ray::Status status;
  };

  ObjectBufferPool(const std::string &store_socket_name, const uint64_t chunk_size) {
    store_socket_name_ = store_socket_name;
    chunk_size_ = chunk_size;
  }

  /// This object cannot be copied due to pool_mutex.
  RAY_DISALLOW_COPY_AND_ASSIGN(ObjectBufferPool);

  uint64_t GetNumChunks(uint64_t data_size){
    return ceil(static_cast<float>(data_size)/chunk_size_);
  }

  const ChunkInfo &GetChunk(const ObjectID &object_id, uint64_t data_size, uint64_t metadata_size, uint64_t chunk_index) {
    std::lock_guard<std::mutex> lock(pool_mutex_);
    if (get_failed_count_.count(object_id) != 0){
      // Ensure all gets fail if the first get fails.
      get_failed_count_[object_id]--;
      if (get_failed_count_[object_id] == 0){
        get_failed_count_.erase(object_id);
      }
      return errored_chunk_;
    }
    RAY_LOG(DEBUG) << "GetChunk "
                   << object_id << " "
                   << data_size << " "
                   << metadata_size;
    if (chunk_info_.count(object_id) == 0) {
      std::shared_ptr<plasma::PlasmaClient> store_client = GetObjectStore();
      plasma::ObjectBuffer object_buffer;
      plasma::ObjectID plasma_id = ObjectID(object_id).to_plasma_id();
      ARROW_CHECK_OK(store_client->Get(&plasma_id, 1, 0, &object_buffer));
      if (object_buffer.data_size == -1) {
        ReleaseObjectStore(store_client);
        RAY_LOG(ERROR) << "Failed to get object";
        // Ensure all gets fail if the first get fails.
        get_failed_count_[object_id] = GetNumChunks(data_size) - 1;
        return errored_chunk_;
      }
      RAY_CHECK(object_buffer.metadata->data() ==
                object_buffer.data->data() + object_buffer.data_size);
      RAY_CHECK(data_size == static_cast<uint64_t>(object_buffer.data_size + object_buffer.metadata_size));
      auto *data = const_cast<uint8_t *>(object_buffer.data->data());
      uint64_t num_chunks = BuildChunks(object_id, data, data_size, metadata_size);
      buffer_state_.emplace(std::piecewise_construct,
                            std::forward_as_tuple(object_id),
                            std::forward_as_tuple(store_client, num_chunks, BufferStateType::GET));
    }
    RAY_CHECK(buffer_state_[object_id].state_type == BufferStateType::GET);
    return chunk_info_[object_id][chunk_index];
  }

  ray::Status ReleaseBuffer(const ObjectID &object_id) {
    std::lock_guard<std::mutex> lock(pool_mutex_);
    buffer_state_[object_id].references--;
    if (buffer_state_[object_id].references == 0) {
      chunk_info_.erase(object_id);
      std::shared_ptr<plasma::PlasmaClient> store_client =
          buffer_state_[object_id].client;
      ARROW_CHECK_OK(store_client->Release(ObjectID(object_id).to_plasma_id()));
      ReleaseObjectStore(store_client);
      buffer_state_.erase(object_id);
    }
    return ray::Status::OK();
  }

  ray::Status AbortGet(const ObjectID &object_id) {
    std::lock_guard<std::mutex> lock(pool_mutex_);
    chunk_info_.erase(object_id);
    std::shared_ptr<plasma::PlasmaClient> store_client = buffer_state_[object_id].client;
    ARROW_CHECK_OK(store_client->Release(ObjectID(object_id).to_plasma_id()));
    ReleaseObjectStore(store_client);
    buffer_state_.erase(object_id);
    return ray::Status::OK();
  }

  const ChunkInfo &CreateChunk(const ObjectID &object_id, uint64_t data_size,
                               uint64_t metadata_size, uint64_t chunk_index) {
    std::lock_guard<std::mutex> lock(pool_mutex_);
    RAY_LOG(DEBUG) << "CreateChunk "
                   << object_id << " "
                   << data_size << " "
                   << metadata_size;
    if (chunk_info_.count(object_id) == 0) {
      const plasma::ObjectID plasma_id = ObjectID(object_id).to_plasma_id();
      int64_t object_size = data_size-metadata_size;
      // Try to create shared buffer.
      std::shared_ptr<Buffer> data;
      std::shared_ptr<plasma::PlasmaClient> store_client = GetObjectStore();
      arrow::Status s =
          store_client->Create(plasma_id, object_size, NULL, metadata_size, &data);
      std::vector<boost::asio::mutable_buffer> buffer;
      uint8_t *mutable_data;
      if (s.ok()) {
        // Read object into store.
        mutable_data = data->mutable_data();
      } else {
        RAY_LOG(ERROR) << "Buffer Create Failed: " << s.message();
        // Read object into empty buffer.
        std::vector<uint8_t> mutable_vec;
        mutable_vec.resize(data_size);
        mutable_data = mutable_vec.data();
        create_failure_buffers_[object_id] = mutable_vec;
      }
      uint64_t num_chunks = BuildChunks(object_id, mutable_data, data_size, metadata_size);
      buffer_state_.emplace(std::piecewise_construct,
                            std::forward_as_tuple(object_id),
                            std::forward_as_tuple(store_client, num_chunks, BufferStateType::CREATE));
    }
    RAY_CHECK(buffer_state_[object_id].state_type == BufferStateType::CREATE);
    return chunk_info_[object_id][chunk_index];
  }

  ray::Status SealOrAbortBuffer(const ObjectID &object_id, bool succeeded) {
    std::lock_guard<std::mutex> lock(pool_mutex_);
    buffer_state_[object_id].references--;
    RAY_LOG(DEBUG) << "SealOrAbortBuffer "
                   << object_id << " "
                   << buffer_state_[object_id].references;
    if (!succeeded) {
      buffer_state_[object_id].one_failed = true;
    }
    if (buffer_state_[object_id].references == 0) {
      if (buffer_state_[object_id].one_failed) {
        AbortCreate(object_id);
      } else {
        SealCreate(object_id);
      }
    }
    return ray::Status::OK();
  }

  /// Terminates this object.
  /// This should be called after remote client connections have been terminated.
  void Terminate() {
    // Abort everything in progress.
    auto buf_state_copy = buffer_state_;
    for (const auto &pair : buf_state_copy) {
      switch (pair.second.state_type) {
      case BufferStateType::GET: {
        AbortGet(pair.first);
      } break;
      case BufferStateType::CREATE: {
        AbortCreate(pair.first);
      } break;
      }
    }
    RAY_CHECK(chunk_info_.empty());
    RAY_CHECK(buffer_state_.empty());
    RAY_CHECK(create_failure_buffers_.empty());
    // Disconnect plasma clients.
    for (const auto &client : clients) {
      ARROW_CHECK_OK(client->Disconnect());
    }
    available_clients.clear();
    clients.clear();
  }

 private:

  struct BufferState {
    BufferState(){}
    BufferState(std::shared_ptr<plasma::PlasmaClient> client,
                uint64_t references,
                BufferStateType state_type)
        : client(client),
          references(references),
          state_type(state_type){}
    std::shared_ptr<plasma::PlasmaClient> client;
    uint64_t references;
    BufferStateType state_type;
    bool one_failed = false;
  };

  ray::Status SealCreate(const ObjectID &object_id){
    const plasma::ObjectID plasma_id = ObjectID(object_id).to_plasma_id();
    chunk_info_.erase(object_id);
    std::shared_ptr<plasma::PlasmaClient> store_client =
        buffer_state_[object_id].client;
    if (create_failure_buffers_.count(object_id) == 0) {
      // This is a successful create.
      ARROW_CHECK_OK(store_client->Seal(plasma_id));
      ARROW_CHECK_OK(store_client->Release(plasma_id));
    } else {
      // This is a failed create due to failure to allocate buffer.
      create_failure_buffers_.erase(object_id);
      RAY_LOG(ERROR) << "Receive Failed";
    }
    ReleaseObjectStore(store_client);
    buffer_state_.erase(object_id);
    return ray::Status::OK();
  }

  ray::Status AbortCreate(const ObjectID &object_id) {
    const plasma::ObjectID plasma_id = ObjectID(object_id).to_plasma_id();
    chunk_info_.erase(object_id);
    std::shared_ptr<plasma::PlasmaClient> store_client = buffer_state_[object_id].client;
    if (create_failure_buffers_.count(object_id) == 0) {
      // This is a failed create due to error on receiving data.
      ARROW_CHECK_OK(store_client->Release(plasma_id));
      ARROW_CHECK_OK(store_client->Abort(plasma_id));
    } else {
      // This is a failed create due to failure to allocate buffer.
      create_failure_buffers_.erase(object_id);
      RAY_LOG(ERROR) << "Receive Failed";
    }
    ReleaseObjectStore(store_client);
    buffer_state_.erase(object_id);
    return ray::Status::OK();
  }

  uint64_t BuildChunks(const ObjectID &object_id,
                       uint8_t *data,
                       uint64_t data_size,
                       uint64_t metadata_size) {
    // RAY_LOG(INFO) << "BuildChunks " << object_id;
    uint64_t space_remaining = data_size;
    std::vector<ChunkInfo> chunks;
    int64_t position = 0;
    while (space_remaining) {
      position = data_size - space_remaining;
      if (space_remaining < chunk_size_) {
        // RAY_LOG(INFO) << chunks.size() << " pos=" << position << " size=" << space_remaining;
        chunks.emplace_back(data + position, space_remaining, data_size, metadata_size, ray::Status::OK());
        space_remaining = 0;
      } else {
        // RAY_LOG(INFO) << chunks.size() << " pos=" << position << " size=" << chunk_size_;
        chunks.emplace_back(data + position, chunk_size_, data_size, metadata_size, ray::Status::OK());
        space_remaining -= chunk_size_;
      }
    }
    chunk_info_[object_id] = chunks;
    return chunk_info_[object_id].size();
  }

  std::shared_ptr<plasma::PlasmaClient> GetObjectStore() {
    if (available_clients.empty()) {
      Add();
    }
    std::shared_ptr<plasma::PlasmaClient> client = available_clients.back();
    available_clients.pop_back();
    return client;
  }

  void ReleaseObjectStore(std::shared_ptr<plasma::PlasmaClient> client) {
    available_clients.push_back(client);
  }

  /// Adds a client to the client pool and mark it as available.
  void Add() {
    clients.emplace_back(new plasma::PlasmaClient());
    ARROW_CHECK_OK(clients.back()->Connect(store_socket_name_.c_str(), "",
                                           PLASMA_DEFAULT_RELEASE_DELAY));
    available_clients.push_back(clients.back());
  }

  ChunkInfo errored_chunk_ = {
      nullptr, 0, 0, 0,
      ray::Status::IOError("Unable to transfer object "
                           "to requesting plasma manager, object not local.")};

  std::mutex pool_mutex_;
  uint64_t chunk_size_;
  std::unordered_map<ray::ObjectID, std::vector<ChunkInfo>, ray::UniqueIDHasher>
      chunk_info_;
  std::unordered_map<ray::ObjectID, BufferState, ray::UniqueIDHasher> buffer_state_;
  std::unordered_map<ray::ObjectID, std::vector<uint8_t>, ray::UniqueIDHasher>
      create_failure_buffers_;
  std::unordered_map<ray::ObjectID, uint64_t, ray::UniqueIDHasher>
      get_failed_count_;

  /// Available plasma clients.
  std::vector<std::shared_ptr<plasma::PlasmaClient>> available_clients;
  /// Plasma client pool.
  std::vector<std::shared_ptr<plasma::PlasmaClient>> clients;
  /// Socket name of plasma store.
  std::string store_socket_name_;
};

}  // namespace ray

#endif  // RAY_OBJECT_MANAGER_OBJECT_BUFFER_POOL_H
