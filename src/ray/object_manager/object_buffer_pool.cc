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

#include "absl/time/time.h"
#include "ray/common/status.h"
#include "ray/util/logging.h"

namespace ray {

ObjectBufferPool::ObjectBufferPool(
    std::shared_ptr<plasma::PlasmaClientInterface> store_client, uint64_t chunk_size)
    : store_client_(store_client), default_chunk_size_(chunk_size) {}

ObjectBufferPool::~ObjectBufferPool() {
  absl::MutexLock lock(&pool_mutex_);
  auto inflight_ops = create_buffer_ops_;
  pool_mutex_.Unlock();

  for (auto &itr : inflight_ops) {
    itr.second->SignalAll();
  }
  auto no_inflight = [this]() {
    pool_mutex_.AssertReaderHeld();
    return create_buffer_ops_.empty();
  };
  // Assume no request would arrive, acquire pool_mutex_ when there is no inflight
  // operation. Otherwise print an error.
  if (!pool_mutex_.LockWhenWithTimeout(absl::Condition(&no_inflight), absl::Seconds(5))) {
    RAY_LOG(ERROR)
        << create_buffer_ops_.size() << " remaining inflight create buffer operations "
        << "during ObjectBufferPool destruction. Either abort these operations before "
        << "destroying ObjectBufferPool, or refactor ObjectBufferPool to make it "
           "unnecessary to wait for the operations' completion.";
  }

  // Abort unfinished buffers in progress.
  for (auto it = create_buffer_state_.begin(); it != create_buffer_state_.end(); it++) {
    RAY_CHECK_OK(store_client_->Release(it->first));
    RAY_CHECK_OK(store_client_->Abort(it->first));
    create_buffer_state_.erase(it);
  }

  RAY_CHECK(create_buffer_state_.empty());
  RAY_CHECK_OK(store_client_->Disconnect());
}

uint64_t ObjectBufferPool::GetNumChunks(uint64_t data_size) const {
  return (data_size + default_chunk_size_ - 1) / default_chunk_size_;
}

uint64_t ObjectBufferPool::GetBufferLength(uint64_t chunk_index,
                                           uint64_t data_size) const {
  return (chunk_index + 1) * default_chunk_size_ > data_size
             ? data_size % default_chunk_size_
             : default_chunk_size_;
}

std::pair<std::shared_ptr<MemoryObjectReader>, ray::Status>
ObjectBufferPool::CreateObjectReader(const ObjectID &object_id,
                                     rpc::Address owner_address) {
  absl::MutexLock lock(&pool_mutex_);

  std::vector<ObjectID> object_ids{object_id};
  std::vector<plasma::ObjectBuffer> object_buffers(1);
  RAY_CHECK_OK(
      store_client_->Get(object_ids, 0, &object_buffers, /*is_from_worker=*/false));
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
                                          uint64_t data_size,
                                          uint64_t metadata_size,
                                          uint64_t chunk_index) {
  absl::MutexLock lock(&pool_mutex_);
  RAY_RETURN_NOT_OK(EnsureBufferExists(
      object_id, owner_address, data_size, metadata_size, chunk_index));
  auto &state = create_buffer_state_.at(object_id);
  if (chunk_index >= state.chunk_state.size()) {
    return ray::Status::IOError("Object size mismatch");
  }
  if (state.chunk_state[chunk_index] != CreateChunkState::AVAILABLE) {
    // There can be only one reference to this chunk at any given time.
    return ray::Status::IOError("Chunk already received by a different thread.");
  }
  state.chunk_state[chunk_index] = CreateChunkState::REFERENCED;
  return ray::Status::OK();
}

void ObjectBufferPool::WriteChunk(const ObjectID &object_id,
                                  uint64_t data_size,
                                  uint64_t metadata_size,
                                  const uint64_t chunk_index,
                                  const std::string &data) {
  absl::MutexLock lock(&pool_mutex_);
  auto it = create_buffer_state_.find(object_id);
  if (it == create_buffer_state_.end() || chunk_index >= it->second.chunk_state.size() ||
      it->second.chunk_state.at(chunk_index) != CreateChunkState::REFERENCED) {
    RAY_LOG(DEBUG) << "Object " << object_id << " aborted before chunk " << chunk_index
                   << " could be sealed";
    return;
  }
  if (it->second.data_size != data_size || it->second.metadata_size != metadata_size) {
    RAY_LOG(DEBUG) << "Object " << object_id << " size mismatch, rejecting chunk";
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
    RAY_CHECK_OK(store_client_->Seal(object_id));
    RAY_CHECK_OK(store_client_->Release(object_id));
    create_buffer_state_.erase(it);
    RAY_LOG(DEBUG) << "Have received all chunks for object " << object_id
                   << ", last chunk index: " << chunk_index;
  }
}

void ObjectBufferPool::AbortCreate(const ObjectID &object_id) {
  absl::MutexLock lock(&pool_mutex_);
  AbortCreateInternal(object_id);
}

void ObjectBufferPool::AbortCreateInternal(const ObjectID &object_id) {
  auto it = create_buffer_state_.find(object_id);
  if (it != create_buffer_state_.end()) {
    RAY_CHECK_OK(store_client_->Release(object_id));
    RAY_CHECK_OK(store_client_->Abort(object_id));
    create_buffer_state_.erase(object_id);
  }
}

std::vector<ObjectBufferPool::ChunkInfo> ObjectBufferPool::BuildChunks(
    const ObjectID &object_id,
    uint8_t *data,
    uint64_t data_size,
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
      chunks.emplace_back(
          chunks.size(), data + position, default_chunk_size_, buffer_ref);
      space_remaining -= default_chunk_size_;
    }
  }
  return chunks;
}

ray::Status ObjectBufferPool::EnsureBufferExists(const ObjectID &object_id,
                                                 const rpc::Address &owner_address,
                                                 uint64_t data_size,
                                                 uint64_t metadata_size,
                                                 uint64_t chunk_index) {
  while (true) {
    // Buffer for object_id already exists and the size matches ours.
    {
      auto it = create_buffer_state_.find(object_id);
      if (it != create_buffer_state_.end() && it->second.data_size == data_size &&
          it->second.metadata_size == metadata_size) {
        return ray::Status::OK();
      }
    }

    auto it = create_buffer_ops_.find(object_id);
    if (it == create_buffer_ops_.end()) {
      // No inflight create buffer operation, proceed to start one.
      break;
    }

    auto cond_var = it->second;
    // Release pool_mutex_ while waiting, until the current inflight create buffer
    // operation finishes.
    cond_var->Wait(&pool_mutex_);
  }

  // Indicate that there is an inflight create buffer operation, by inserting into
  // create_buffer_ops_.
  RAY_CHECK(
      create_buffer_ops_.insert({object_id, std::make_shared<absl::CondVar>()}).second);

  // If the buffer currently exists, its size must be different. Abort the
  // created buffer so we can recreate it with the correct size.
  {
    auto it = create_buffer_state_.find(object_id);
    if (it != create_buffer_state_.end()) {
      RAY_CHECK(it->second.data_size != data_size ||
                it->second.metadata_size != metadata_size);
      RAY_LOG(WARNING) << "Object " << object_id << " size (" << data_size
                       << ") differs from the original (" << it->second.data_size
                       << "). This is likely due to re-execution of a task with a "
                          "nondeterministic output. Recreating object with size "
                       << data_size << ".";
      AbortCreateInternal(it->first);
    }
  }

  const int64_t object_size =
      static_cast<int64_t>(data_size) - static_cast<int64_t>(metadata_size);
  std::shared_ptr<Buffer> data;

  // Release pool_mutex_ during the blocking create call.
  pool_mutex_.Unlock();
  Status s = store_client_->CreateAndSpillIfNeeded(
      object_id,
      owner_address,
      static_cast<int64_t>(object_size),
      nullptr,
      static_cast<int64_t>(metadata_size),
      &data,
      plasma::flatbuf::ObjectSource::ReceivedFromRemoteRaylet);

  pool_mutex_.Lock();

  // No other thread could have created the buffer.
  RAY_CHECK(!create_buffer_state_.contains(object_id));

  // Remove object_id from create_buffer_ops_ to indicate to the waiting ops that the
  // inflight operation has finished. Wake up waiters so they can either start another
  // create buffer op, or proceed after the buffer has been created.
  {
    auto it = create_buffer_ops_.find(object_id);
    it->second->SignalAll();
    create_buffer_ops_.erase(it);
  }

  if (!s.ok()) {
    if (s.IsOutOfDisk()) {
      return s;
    }
    // Create failed. Buffer creation will be tried by another chunk.
    // And this chunk will eventually make it here via retried pull requests.
    return ray::Status::IOError(s.message());
  }

  // Read object into store.
  uint8_t *mutable_data = data->Data();
  uint64_t num_chunks = GetNumChunks(data_size);
  auto inserted = create_buffer_state_.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(object_id),
      std::forward_as_tuple(metadata_size,
                            data_size,
                            BuildChunks(object_id, mutable_data, data_size, data)));
  RAY_CHECK(inserted.first->second.chunk_info.size() == num_chunks);
  RAY_LOG(DEBUG) << "Created object " << object_id
                 << " in plasma store, number of chunks: " << num_chunks
                 << ", chunk index: " << chunk_index;

  return ray::Status::OK();
}

void ObjectBufferPool::FreeObjects(const std::vector<ObjectID> &object_ids) {
  absl::MutexLock lock(&pool_mutex_);
  RAY_CHECK_OK(store_client_->Delete(object_ids));
}

std::string ObjectBufferPool::DebugString() const {
  absl::MutexLock lock(&pool_mutex_);
  std::stringstream result;
  result << "BufferPool:";
  result << "\n- create buffer state map size: " << create_buffer_state_.size();
  return result.str();
}

}  // namespace ray
