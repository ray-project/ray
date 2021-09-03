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

#include "ray/object_manager/object_buffer_pool.h"

#include "ray/common/status.h"
#include "ray/util/logging.h"

namespace ray {

ObjectBufferPool::ObjectBufferPool(const std::string &store_socket_name,
                                   uint64_t chunk_size)
    : default_chunk_size_(chunk_size) {
  store_socket_name_ = store_socket_name;
  RAY_CHECK_OK(store_client_.Connect(store_socket_name_.c_str(), "", 0, 300));
}

ObjectBufferPool::~ObjectBufferPool() {
  // Abort everything in progress.
  auto create_buf_state_copy = create_buffer_state_;
  for (const auto &pair : create_buf_state_copy) {
    AbortCreate(pair.first);
  }
  RAY_CHECK(create_buffer_state_.empty());
  RAY_CHECK_OK(store_client_.Disconnect());
}

uint64_t ObjectBufferPool::GetNumChunks(uint64_t data_size) {
  return (data_size + default_chunk_size_ - 1) / default_chunk_size_;
}

uint64_t ObjectBufferPool::GetBufferLength(uint64_t chunk_index, uint64_t data_size) {
  return (chunk_index + 1) * default_chunk_size_ > data_size
             ? data_size % default_chunk_size_
             : default_chunk_size_;
}

std::pair<std::shared_ptr<MemoryObjectReader>, ray::Status>
ObjectBufferPool::CreateObjectReader(const ObjectID &object_id,
                                     rpc::Address owner_address) {
  std::lock_guard<std::mutex> lock(pool_mutex_);

  std::vector<ObjectID> object_ids{object_id};
  std::vector<plasma::ObjectBuffer> object_buffers(1);
  RAY_CHECK_OK(
      store_client_.Get(object_ids, 0, &object_buffers, /*is_from_worker=*/false));
  if (object_buffers[0].data == nullptr) {
    RAY_LOG(INFO)
        << "Failed to get a chunk of the object: " << object_id
        << ". This is most likely because the object was evicted or spilled before the "
           "pull request was received. The caller will retry the pull request after a "
           "timeout.";
    return std::pair<std::shared_ptr<MemoryObjectReader>, ray::Status>(
        nullptr,
        ray::Status::IOError("Unable to obtain object chunk, object not local."));
  }

  return std::pair<std::shared_ptr<MemoryObjectReader>, ray::Status>(
      std::make_shared<MemoryObjectReader>(std::move(object_buffers[0]),
                                           std::move(owner_address)),
      ray::Status::OK());
}

ray::Status ObjectBufferPool::CreateChunk(const ObjectID &object_id,
                                          const rpc::Address &owner_address,
                                          uint64_t data_size, uint64_t metadata_size,
                                          uint64_t chunk_index) {
  std::unique_lock<std::mutex> lock(pool_mutex_);
  if (create_buffer_state_.count(object_id) == 0) {
    int64_t object_size = data_size - metadata_size;
    // Try to create shared buffer.
    std::shared_ptr<Buffer> data;

    // Release the buffer pool lock during the blocking create call.
    lock.unlock();
    Status s = store_client_.CreateAndSpillIfNeeded(
        object_id, owner_address, object_size, NULL, metadata_size, &data,
        plasma::flatbuf::ObjectSource::ReceivedFromRemoteRaylet);
    lock.lock();

    // Another thread may have succeeded in creating the chunk while the lock
    // was released. In that case skip the remainder of the creation block.
    if (create_buffer_state_.count(object_id) == 0) {
      std::vector<boost::asio::mutable_buffer> buffer;
      if (!s.ok()) {
        // Create failed. The object may already exist locally. If something else went
        // wrong, another chunk will succeed in creating the buffer, and this
        // chunk will eventually make it here via pull requests.
        return ray::Status::IOError(s.message());
      }
      // Read object into store.
      uint8_t *mutable_data = data->Data();
      uint64_t num_chunks = GetNumChunks(data_size);
      create_buffer_state_.emplace(
          std::piecewise_construct, std::forward_as_tuple(object_id),
          std::forward_as_tuple(BuildChunks(object_id, mutable_data, data_size, data)));
      RAY_LOG(DEBUG) << "Created object " << object_id
                     << " in plasma store, number of chunks: " << num_chunks
                     << ", chunk index: " << chunk_index;
      RAY_CHECK(create_buffer_state_[object_id].chunk_info.size() == num_chunks);
    }
  }
  if (create_buffer_state_[object_id].chunk_state[chunk_index] !=
      CreateChunkState::AVAILABLE) {
    // There can be only one reference to this chunk at any given time.
    return ray::Status::IOError("Chunk already received by a different thread.");
  }
  create_buffer_state_[object_id].chunk_state[chunk_index] = CreateChunkState::REFERENCED;
  return ray::Status::OK();
}

void ObjectBufferPool::WriteChunk(const ObjectID &object_id, const uint64_t chunk_index,
                                  const std::string &data) {
  std::lock_guard<std::mutex> lock(pool_mutex_);
  auto it = create_buffer_state_.find(object_id);
  if (it == create_buffer_state_.end() ||
      it->second.chunk_state.at(chunk_index) != CreateChunkState::REFERENCED) {
    RAY_LOG(DEBUG) << "Object " << object_id << " aborted due to OOM before chunk "
                   << chunk_index << " could be sealed";
    return;
  }
  RAY_CHECK(it->second.chunk_info.size() > chunk_index);
  auto &chunk_info = it->second.chunk_info.at(chunk_index);
  RAY_CHECK(data.size() == chunk_info.buffer_length)
      << "size mismatch!  data size: " << data.size()
      << " chunk size: " << chunk_info.buffer_length;
  std::memcpy(chunk_info.data, data.data(), chunk_info.buffer_length);
  it->second.chunk_state.at(chunk_index) = CreateChunkState::SEALED;
  it->second.num_seals_remaining--;
  if (it->second.num_seals_remaining == 0) {
    RAY_CHECK_OK(store_client_.Seal(object_id));
    RAY_CHECK_OK(store_client_.Release(object_id));
    create_buffer_state_.erase(it);
    RAY_LOG(DEBUG) << "Have received all chunks for object " << object_id
                   << ", last chunk index: " << chunk_index;
  }
}

void ObjectBufferPool::AbortCreate(const ObjectID &object_id) {
  std::lock_guard<std::mutex> lock(pool_mutex_);
  auto it = create_buffer_state_.find(object_id);
  if (it != create_buffer_state_.end()) {
    RAY_LOG(INFO) << "Not enough memory to create requested object " << object_id
                  << ", aborting";
    RAY_CHECK_OK(store_client_.Release(object_id));
    RAY_CHECK_OK(store_client_.Abort(object_id));
    create_buffer_state_.erase(object_id);
  }
}

std::vector<ObjectBufferPool::ChunkInfo> ObjectBufferPool::BuildChunks(
    const ObjectID &object_id, uint8_t *data, uint64_t data_size,
    std::shared_ptr<Buffer> buffer_ref) {
  uint64_t space_remaining = data_size;
  std::vector<ChunkInfo> chunks;
  int64_t position = 0;
  while (space_remaining) {
    position = data_size - space_remaining;
    if (space_remaining < default_chunk_size_) {
      chunks.emplace_back(chunks.size(), data + position, space_remaining, buffer_ref);
      space_remaining = 0;
    } else {
      chunks.emplace_back(chunks.size(), data + position, default_chunk_size_,
                          buffer_ref);
      space_remaining -= default_chunk_size_;
    }
  }
  return chunks;
}

void ObjectBufferPool::FreeObjects(const std::vector<ObjectID> &object_ids) {
  std::lock_guard<std::mutex> lock(pool_mutex_);
  RAY_CHECK_OK(store_client_.Delete(object_ids));
}

std::string ObjectBufferPool::DebugString() const {
  std::lock_guard<std::mutex> lock(pool_mutex_);
  std::stringstream result;
  result << "BufferPool:";
  result << "\n- create buffer state map size: " << create_buffer_state_.size();
  return result.str();
}

}  // namespace ray
