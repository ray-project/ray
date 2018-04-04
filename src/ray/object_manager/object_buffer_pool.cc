#include "ray/object_manager/object_buffer_pool.h"

namespace ray {

ObjectBufferPool::ObjectBufferPool(const std::string &store_socket_name,
                                   const uint64_t chunk_size) {
  store_socket_name_ = store_socket_name;
  chunk_size_ = chunk_size;
}

uint64_t ObjectBufferPool::GetNumChunks(uint64_t data_size) {
  return ceil(static_cast<float>(data_size) / chunk_size_);
}

const ObjectBufferPool::ChunkInfo &ObjectBufferPool::GetChunk(const ObjectID &object_id,
                                                              uint64_t data_size,
                                                              uint64_t metadata_size,
                                                              uint64_t chunk_index) {
  std::lock_guard<std::mutex> lock(pool_mutex_);
  if (get_failed_count_.count(object_id) != 0) {
    // Ensure all gets fail if the first get fails.
    get_failed_count_[object_id]--;
    if (get_failed_count_[object_id] == 0) {
      get_failed_count_.erase(object_id);
    }
    return errored_chunk_;
  }
  RAY_LOG(DEBUG) << "GetChunk " << object_id << " " << data_size << " " << metadata_size;
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
    RAY_CHECK(data_size == static_cast<uint64_t>(object_buffer.data_size +
                                                 object_buffer.metadata_size));
    auto *data = const_cast<uint8_t *>(object_buffer.data->data());
    uint64_t num_chunks = BuildChunks(object_id, data, data_size, metadata_size);
    buffer_state_.emplace(
        std::piecewise_construct, std::forward_as_tuple(object_id),
        std::forward_as_tuple(store_client, num_chunks, BufferStateType::GET));
  }
  RAY_CHECK(buffer_state_[object_id].state_type == BufferStateType::GET);
  return chunk_info_[object_id][chunk_index];
}

ray::Status ObjectBufferPool::ReleaseBuffer(const ObjectID &object_id) {
  std::lock_guard<std::mutex> lock(pool_mutex_);
  buffer_state_[object_id].references--;
  if (buffer_state_[object_id].references == 0) {
    chunk_info_.erase(object_id);
    std::shared_ptr<plasma::PlasmaClient> store_client = buffer_state_[object_id].client;
    ARROW_CHECK_OK(store_client->Release(ObjectID(object_id).to_plasma_id()));
    ReleaseObjectStore(store_client);
    buffer_state_.erase(object_id);
  }
  return ray::Status::OK();
}

ray::Status ObjectBufferPool::AbortGet(const ObjectID &object_id) {
  std::lock_guard<std::mutex> lock(pool_mutex_);
  chunk_info_.erase(object_id);
  std::shared_ptr<plasma::PlasmaClient> store_client = buffer_state_[object_id].client;
  ARROW_CHECK_OK(store_client->Release(ObjectID(object_id).to_plasma_id()));
  ReleaseObjectStore(store_client);
  buffer_state_.erase(object_id);
  return ray::Status::OK();
}

const ObjectBufferPool::ChunkInfo &ObjectBufferPool::CreateChunk(
    const ObjectID &object_id, uint64_t data_size, uint64_t metadata_size,
    uint64_t chunk_index) {
  std::lock_guard<std::mutex> lock(pool_mutex_);
  RAY_LOG(DEBUG) << "CreateChunk " << object_id << " " << data_size << " "
                 << metadata_size;
  if (chunk_info_.count(object_id) == 0) {
    const plasma::ObjectID plasma_id = ObjectID(object_id).to_plasma_id();
    int64_t object_size = data_size - metadata_size;
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
    buffer_state_.emplace(
        std::piecewise_construct, std::forward_as_tuple(object_id),
        std::forward_as_tuple(store_client, num_chunks, BufferStateType::CREATE));
  }
  RAY_CHECK(buffer_state_[object_id].state_type == BufferStateType::CREATE);
  return chunk_info_[object_id][chunk_index];
}

ray::Status ObjectBufferPool::SealOrAbortBuffer(const ObjectID &object_id,
                                                bool succeeded) {
  std::lock_guard<std::mutex> lock(pool_mutex_);
  buffer_state_[object_id].references--;
  RAY_LOG(DEBUG) << "SealOrAbortBuffer " << object_id << " "
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

void ObjectBufferPool::Terminate() {
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

ray::Status ObjectBufferPool::SealCreate(const ObjectID &object_id) {
  const plasma::ObjectID plasma_id = ObjectID(object_id).to_plasma_id();
  chunk_info_.erase(object_id);
  std::shared_ptr<plasma::PlasmaClient> store_client = buffer_state_[object_id].client;
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

ray::Status ObjectBufferPool::AbortCreate(const ObjectID &object_id) {
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

uint64_t ObjectBufferPool::BuildChunks(const ObjectID &object_id, uint8_t *data,
                                       uint64_t data_size, uint64_t metadata_size) {
  uint64_t space_remaining = data_size;
  std::vector<ChunkInfo> chunks;
  int64_t position = 0;
  while (space_remaining) {
    position = data_size - space_remaining;
    if (space_remaining < chunk_size_) {
      chunks.emplace_back(data + position, space_remaining, data_size, metadata_size,
                          ray::Status::OK());
      space_remaining = 0;
    } else {
      chunks.emplace_back(data + position, chunk_size_, data_size, metadata_size,
                          ray::Status::OK());
      space_remaining -= chunk_size_;
    }
  }
  chunk_info_[object_id] = chunks;
  return chunk_info_[object_id].size();
}

std::shared_ptr<plasma::PlasmaClient> ObjectBufferPool::GetObjectStore() {
  if (available_clients.empty()) {
    Add();
  }
  std::shared_ptr<plasma::PlasmaClient> client = available_clients.back();
  available_clients.pop_back();
  return client;
}

void ObjectBufferPool::ReleaseObjectStore(std::shared_ptr<plasma::PlasmaClient> client) {
  available_clients.push_back(client);
}

/// Adds a client to the client pool and mark it as available.
void ObjectBufferPool::Add() {
  clients.emplace_back(new plasma::PlasmaClient());
  ARROW_CHECK_OK(clients.back()->Connect(store_socket_name_.c_str(), "",
                                         PLASMA_DEFAULT_RELEASE_DELAY));
  available_clients.push_back(clients.back());
}

}  // namespace ray
