#include "ray/object_manager/object_buffer_pool.h"

namespace ray {

ObjectBufferPool::ObjectBufferPool(const std::string &store_socket_name,
                                   uint64_t chunk_size, int release_delay)
    : default_chunk_size_(chunk_size) {
  store_socket_name_ = store_socket_name;
  ARROW_CHECK_OK(store_client_.Connect(store_socket_name_.c_str(), "", release_delay));
}

ObjectBufferPool::~ObjectBufferPool() {
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
  ARROW_CHECK_OK(store_client_.Disconnect());
}

uint64_t ObjectBufferPool::GetNumChunks(uint64_t data_size) {
  return (data_size + default_chunk_size_ - 1) / default_chunk_size_;
}

uint64_t ObjectBufferPool::GetBufferLength(uint64_t chunk_index, uint64_t data_size) {
  return (chunk_index + 1) * default_chunk_size_ > data_size
             ? data_size % default_chunk_size_
             : default_chunk_size_;
}

std::pair<const ObjectBufferPool::ChunkInfo &, ray::Status> ObjectBufferPool::GetChunk(
    const ObjectID &object_id, uint64_t data_size, uint64_t metadata_size,
    uint64_t chunk_index) {
  std::lock_guard<std::mutex> lock(pool_mutex_);
  RAY_LOG(DEBUG) << "GetChunk " << object_id << " " << data_size << " " << metadata_size;
  if (get_buffer_state_.count(object_id) == 0) {
    plasma::ObjectBuffer object_buffer;
    plasma::ObjectID plasma_id = object_id.to_plasma_id();
    ARROW_CHECK_OK(store_client_.Get(&plasma_id, 1, 0, &object_buffer));
    if (object_buffer.data == nullptr) {
      RAY_LOG(ERROR) << "Failed to get object";
      return std::pair<const ObjectBufferPool::ChunkInfo &, ray::Status>(
          errored_chunk_,
          ray::Status::IOError("Unable to obtain object chunk, object not local."));
    }
    RAY_CHECK(object_buffer.metadata->data() ==
              object_buffer.data->data() + object_buffer.data->size());
    RAY_CHECK(data_size == static_cast<uint64_t>(object_buffer.data->size() +
                                                 object_buffer.metadata->size()));
    auto *data = const_cast<uint8_t *>(object_buffer.data->data());
    uint64_t num_chunks = GetNumChunks(data_size);
    get_buffer_state_.emplace(
        std::piecewise_construct, std::forward_as_tuple(object_id),
        std::forward_as_tuple(BuildChunks(object_id, data, data_size)));
    RAY_CHECK(get_buffer_state_[object_id].chunk_info.size() == num_chunks);
  }
  get_buffer_state_[object_id].references++;
  return std::pair<const ObjectBufferPool::ChunkInfo &, ray::Status>(
      get_buffer_state_[object_id].chunk_info[chunk_index], ray::Status::OK());
}

void ObjectBufferPool::ReleaseGetChunk(const ObjectID &object_id, uint64_t chunk_index) {
  std::lock_guard<std::mutex> lock(pool_mutex_);
  GetBufferState &buffer_state = get_buffer_state_[object_id];
  buffer_state.references--;
  RAY_LOG(DEBUG) << "ReleaseBuffer " << object_id << " " << buffer_state.references;
  if (buffer_state.references == 0) {
    ARROW_CHECK_OK(store_client_.Release(object_id.to_plasma_id()));
    get_buffer_state_.erase(object_id);
  }
}

void ObjectBufferPool::AbortGet(const ObjectID &object_id) {
  std::lock_guard<std::mutex> lock(pool_mutex_);
  ARROW_CHECK_OK(store_client_.Release(object_id.to_plasma_id()));
  get_buffer_state_.erase(object_id);
}

std::pair<const ObjectBufferPool::ChunkInfo &, ray::Status> ObjectBufferPool::CreateChunk(
    const ObjectID &object_id, uint64_t data_size, uint64_t metadata_size,
    uint64_t chunk_index) {
  std::lock_guard<std::mutex> lock(pool_mutex_);
  RAY_LOG(DEBUG) << "CreateChunk " << object_id << " " << data_size << " "
                 << metadata_size;
  if (create_buffer_state_.count(object_id) == 0) {
    const plasma::ObjectID plasma_id = object_id.to_plasma_id();
    int64_t object_size = data_size - metadata_size;
    // Try to create shared buffer.
    std::shared_ptr<Buffer> data;
    arrow::Status s =
        store_client_.Create(plasma_id, object_size, NULL, metadata_size, &data);
    std::vector<boost::asio::mutable_buffer> buffer;
    if (!s.ok()) {
      // Create failed. The object may already exist locally. If something else went
      // wrong, another chunk will succeed in creating the buffer, and this
      // chunk will eventually make it here via pull requests.
      return std::pair<const ObjectBufferPool::ChunkInfo &, ray::Status>(
          errored_chunk_, ray::Status::IOError(s.message()));
    }
    // Read object into store.
    uint8_t *mutable_data = data->mutable_data();
    uint64_t num_chunks = GetNumChunks(data_size);
    create_buffer_state_.emplace(
        std::piecewise_construct, std::forward_as_tuple(object_id),
        std::forward_as_tuple(BuildChunks(object_id, mutable_data, data_size)));
    RAY_CHECK(create_buffer_state_[object_id].chunk_info.size() == num_chunks);
  }
  if (create_buffer_state_[object_id].chunk_state[chunk_index] !=
      CreateChunkState::AVAILABLE) {
    // There can be only one reference to this chunk at any given time.
    return std::pair<const ObjectBufferPool::ChunkInfo &, ray::Status>(
        errored_chunk_,
        ray::Status::IOError("Chunk already referenced by another thread."));
  }
  create_buffer_state_[object_id].chunk_state[chunk_index] = CreateChunkState::REFERENCED;
  return std::pair<const ObjectBufferPool::ChunkInfo &, ray::Status>(
      create_buffer_state_[object_id].chunk_info[chunk_index], ray::Status::OK());
}

void ObjectBufferPool::AbortCreateChunk(const ObjectID &object_id,
                                        const uint64_t chunk_index) {
  std::lock_guard<std::mutex> lock(pool_mutex_);
  RAY_CHECK(create_buffer_state_[object_id].chunk_state[chunk_index] ==
            CreateChunkState::REFERENCED);
  create_buffer_state_[object_id].chunk_state[chunk_index] = CreateChunkState::AVAILABLE;
  if (create_buffer_state_[object_id].num_seals_remaining ==
      create_buffer_state_[object_id].chunk_state.size()) {
    // If chunk_state is AVAILABLE at every chunk_index and
    // num_seals_remaining == num_chunks, this is back to the initial state
    // right before the first CreateChunk.
    bool abort = true;
    for (auto chunk_state : create_buffer_state_[object_id].chunk_state) {
      abort &= chunk_state == CreateChunkState::AVAILABLE;
    }
    if (abort) {
      AbortCreate(object_id);
    }
  }
}

void ObjectBufferPool::SealChunk(const ObjectID &object_id, const uint64_t chunk_index) {
  std::lock_guard<std::mutex> lock(pool_mutex_);
  RAY_CHECK(create_buffer_state_[object_id].chunk_state[chunk_index] ==
            CreateChunkState::REFERENCED);
  create_buffer_state_[object_id].chunk_state[chunk_index] = CreateChunkState::SEALED;
  create_buffer_state_[object_id].num_seals_remaining--;
  RAY_LOG(DEBUG) << "SealChunk" << object_id << " "
                 << create_buffer_state_[object_id].num_seals_remaining;
  if (create_buffer_state_[object_id].num_seals_remaining == 0) {
    const plasma::ObjectID plasma_id = object_id.to_plasma_id();
    ARROW_CHECK_OK(store_client_.Seal(plasma_id));
    ARROW_CHECK_OK(store_client_.Release(plasma_id));
    create_buffer_state_.erase(object_id);
  }
}

void ObjectBufferPool::AbortCreate(const ObjectID &object_id) {
  const plasma::ObjectID plasma_id = object_id.to_plasma_id();
  ARROW_CHECK_OK(store_client_.Release(plasma_id));
  ARROW_CHECK_OK(store_client_.Abort(plasma_id));
  create_buffer_state_.erase(object_id);
}

std::vector<ObjectBufferPool::ChunkInfo> ObjectBufferPool::BuildChunks(
    const ObjectID &object_id, uint8_t *data, uint64_t data_size) {
  uint64_t space_remaining = data_size;
  std::vector<ChunkInfo> chunks;
  int64_t position = 0;
  while (space_remaining) {
    position = data_size - space_remaining;
    if (space_remaining < default_chunk_size_) {
      chunks.emplace_back(chunks.size(), data + position, space_remaining);
      space_remaining = 0;
    } else {
      chunks.emplace_back(chunks.size(), data + position, default_chunk_size_);
      space_remaining -= default_chunk_size_;
    }
  }
  return chunks;
}

}  // namespace ray
