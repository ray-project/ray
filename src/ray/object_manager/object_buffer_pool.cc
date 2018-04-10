#include "ray/object_manager/object_buffer_pool.h"

namespace ray {

ObjectBufferPool::ObjectBufferPool(const std::string &store_socket_name,
                                   const uint64_t chunk_size) {
  store_socket_name_ = store_socket_name;
  chunk_size_ = chunk_size;
}

ObjectBufferPool::~ObjectBufferPool(){
  // Abort everything in progress.
  auto get_buf_state_copy = get_buffer_state_;
  for (const auto &pair : get_buf_state_copy) {
    AbortGet(pair.first);
  }
  auto create_buf_state_copy = create_buffer_state_;
  for (const auto &pair : create_buf_state_copy) {
    AbortCreate(pair.first);
  }
  RAY_CHECK(get_buffer_state_.empty());
  RAY_CHECK(create_buffer_state_.empty());
  // Disconnect plasma clients.
  for (const auto &client : clients) {
    ARROW_CHECK_OK(client->Disconnect());
  }
}

uint64_t ObjectBufferPool::GetNumChunks(uint64_t data_size) {
  return static_cast<uint64_t>(ceil(static_cast<double>(data_size) / chunk_size_));
}

uint64_t ObjectBufferPool::GetBufferLength(uint64_t chunk_index, uint64_t data_size){
  return (chunk_index + 1) * chunk_size_ > data_size
         ? data_size % chunk_size_
         : chunk_size_;
}

std::pair<const ObjectBufferPool::ChunkInfo &, ray::Status> ObjectBufferPool::GetChunk(const ObjectID &object_id,
                                                              uint64_t data_size,
                                                              uint64_t metadata_size,
                                                              uint64_t chunk_index) {
  std::lock_guard<std::mutex> lock(pool_mutex_);
  RAY_LOG(DEBUG) << "GetChunk " << object_id << " " << data_size << " " << metadata_size;
  if (get_buffer_state_.count(object_id) == 0) {
    std::shared_ptr<plasma::PlasmaClient> store_client = GetObjectStore();
    plasma::ObjectBuffer object_buffer;
    plasma::ObjectID plasma_id = ObjectID(object_id).to_plasma_id();
    ARROW_CHECK_OK(store_client->Get(&plasma_id, 1, 0, &object_buffer));
    if (object_buffer.data_size == -1) {
      ReleaseObjectStore(store_client);
      RAY_LOG(ERROR) << "Failed to get object";
      return std::pair<const ObjectBufferPool::ChunkInfo &, ray::Status>(
          errored_chunk_,
          ray::Status::IOError("Unable to obtain object chunk, object not local."));
    }
    RAY_CHECK(object_buffer.metadata->data() ==
              object_buffer.data->data() + object_buffer.data_size);
    RAY_CHECK(data_size == static_cast<uint64_t>(object_buffer.data_size +
                                                 object_buffer.metadata_size));
    auto *data = const_cast<uint8_t *>(object_buffer.data->data());
    uint64_t num_chunks = GetNumChunks(data_size);
    get_buffer_state_.emplace(
        std::piecewise_construct, std::forward_as_tuple(object_id),
        std::forward_as_tuple(store_client, BuildChunks(object_id, data, data_size, metadata_size)));
    RAY_CHECK(get_buffer_state_[object_id].chunk_info.size() == num_chunks);
  }
  get_buffer_state_[object_id].references++;
  get_buffer_state_[object_id].chunk_references[chunk_index]++;
  return std::pair<const ObjectBufferPool::ChunkInfo &, ray::Status>(
      get_buffer_state_[object_id].chunk_info[chunk_index], ray::Status::OK());
}

ray::Status ObjectBufferPool::ReleaseGetChunk(const ObjectID &object_id, uint64_t chunk_index) {
  std::lock_guard<std::mutex> lock(pool_mutex_);
  GetBufferState &buffer_state = get_buffer_state_[object_id];
  buffer_state.references--;
  get_buffer_state_[object_id].chunk_references[chunk_index]--;
  if (buffer_state.references == 0) {
    std::shared_ptr<plasma::PlasmaClient> store_client = get_buffer_state_[object_id].client;
    ARROW_CHECK_OK(store_client->Release(ObjectID(object_id).to_plasma_id()));
    ReleaseObjectStore(store_client);
    get_buffer_state_.erase(object_id);
  }
  return ray::Status::OK();
}

ray::Status ObjectBufferPool::AbortGet(const ObjectID &object_id) {
  std::lock_guard<std::mutex> lock(pool_mutex_);
  std::shared_ptr<plasma::PlasmaClient> store_client = get_buffer_state_[object_id].client;
  ARROW_CHECK_OK(store_client->Release(ObjectID(object_id).to_plasma_id()));
  ReleaseObjectStore(store_client);
  get_buffer_state_.erase(object_id);
  return ray::Status::OK();
}

std::pair<const ObjectBufferPool::ChunkInfo &, ray::Status> ObjectBufferPool::CreateChunk(
    const ObjectID &object_id, uint64_t data_size, uint64_t metadata_size,
    uint64_t chunk_index) {
  std::lock_guard<std::mutex> lock(pool_mutex_);
  RAY_LOG(DEBUG) << "CreateChunk " << object_id << " " << data_size << " "
                 << metadata_size;
  if (create_buffer_state_.count(object_id) == 0) {
    const plasma::ObjectID plasma_id = ObjectID(object_id).to_plasma_id();
    int64_t object_size = data_size - metadata_size;
    // Try to create shared buffer.
    std::shared_ptr<Buffer> data;
    std::shared_ptr<plasma::PlasmaClient> store_client = GetObjectStore();
    arrow::Status s =
        store_client->Create(plasma_id, object_size, NULL, metadata_size, &data);
    std::vector<boost::asio::mutable_buffer> buffer;
    if (!s.ok()) {
      // Create failed. The object may already exist locally. If something else went
      // wrong, another chunk will succeed in creating the buffer, and this
      // chunk will eventually make it here via pull requests.
      ReleaseObjectStore(store_client);
      return std::pair<const ObjectBufferPool::ChunkInfo &, ray::Status> (
          errored_chunk_,
          ray::Status::IOError(s.message())
      );
    }
    // Read object into store.
    uint8_t *mutable_data = data->mutable_data();
    uint64_t num_chunks = GetNumChunks(data_size);
    create_buffer_state_.emplace(
        std::piecewise_construct, std::forward_as_tuple(object_id),
        std::forward_as_tuple(store_client,
                              BuildChunks(object_id, mutable_data, data_size, metadata_size)));
    RAY_CHECK(create_buffer_state_[object_id].chunk_info.size() == num_chunks);
  }
  if (create_buffer_state_[object_id].chunk_references[chunk_index] != 0){
    // There can be only one reference to this chunk at any given time.
    return std::pair<const ObjectBufferPool::ChunkInfo &, ray::Status> (
        errored_chunk_,
        ray::Status::IOError("Chunk already referenced by another thread.")
    );
  }
  create_buffer_state_[object_id].chunk_references[chunk_index]++;
  return std::pair<const ObjectBufferPool::ChunkInfo &, ray::Status> (
      create_buffer_state_[object_id].chunk_info[chunk_index],
      ray::Status::OK());
}

ray::Status ObjectBufferPool::ReleaseCreateChunk(const ObjectID &object_id,
                                                 const uint64_t chunk_index) {
  std::lock_guard<std::mutex> lock(pool_mutex_);
  create_buffer_state_[object_id].chunk_references[chunk_index]--;
  // Make sure ReleaseCreateChunk OR SealChunk is called.
  RAY_CHECK(create_buffer_state_[object_id].chunk_references[chunk_index] >= 0);
  return ray::Status::OK();
}

ray::Status ObjectBufferPool::SealChunk(const ObjectID &object_id, const uint64_t chunk_index) {
  std::lock_guard<std::mutex> lock(pool_mutex_);
  create_buffer_state_[object_id].chunk_references[chunk_index]--;
  // Make sure ReleaseCreateChunk OR SealChunk is called.
  RAY_CHECK(create_buffer_state_[object_id].chunk_references[chunk_index] >= 0);
  create_buffer_state_[object_id].num_chunks_remaining--;
  RAY_LOG(DEBUG) << "SealChunk" << object_id << " "
                 << create_buffer_state_[object_id].num_chunks_remaining;
  if (create_buffer_state_[object_id].num_chunks_remaining == 0) {
    const plasma::ObjectID plasma_id = ObjectID(object_id).to_plasma_id();
    std::shared_ptr<plasma::PlasmaClient> store_client = create_buffer_state_[object_id].client;
    ARROW_CHECK_OK(store_client->Seal(plasma_id));
    ARROW_CHECK_OK(store_client->Release(plasma_id));
    ReleaseObjectStore(store_client);
    create_buffer_state_.erase(object_id);
  }
  return ray::Status::OK();
}

ray::Status ObjectBufferPool::AbortCreate(const ObjectID &object_id) {
  const plasma::ObjectID plasma_id = ObjectID(object_id).to_plasma_id();
  std::shared_ptr<plasma::PlasmaClient> store_client = create_buffer_state_[object_id].client;
  // This is a failed create due to error on receiving data.
  ARROW_CHECK_OK(store_client->Release(plasma_id));
  ARROW_CHECK_OK(store_client->Abort(plasma_id));
  ReleaseObjectStore(store_client);
  create_buffer_state_.erase(object_id);
  return ray::Status::OK();
}

std::vector<ObjectBufferPool::ChunkInfo> ObjectBufferPool::BuildChunks(const ObjectID &object_id, uint8_t *data,
                                       uint64_t data_size, uint64_t metadata_size) {
  uint64_t space_remaining = data_size;
  std::vector<ChunkInfo> chunks;
  int64_t position = 0;
  while (space_remaining) {
    position = data_size - space_remaining;
    if (space_remaining < chunk_size_) {
      chunks.emplace_back(chunks.size(),
                          data + position,
                          space_remaining);
      space_remaining = 0;
    } else {
      chunks.emplace_back(chunks.size(),
                          data + position,
                          chunk_size_);
      space_remaining -= chunk_size_;
    }
  }
  return chunks;
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
