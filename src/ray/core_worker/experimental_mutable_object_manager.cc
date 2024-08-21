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

#include "ray/core_worker/experimental_mutable_object_manager.h"

#include "absl/strings/str_format.h"
#include "ray/object_manager/common.h"

namespace ray {
namespace experimental {

#if defined(__APPLE__) || defined(__linux__)

namespace {

std::string GetSemaphoreObjectName(const std::string &name) {
  std::string ret = absl::StrCat("obj", name);
  RAY_CHECK_LE(name.size(), PSEMNAMLEN);
  return ret;
}

std::string GetSemaphoreHeaderName(const std::string &name) {
  std::string ret = absl::StrCat("hdr", name);
  RAY_CHECK_LE(name.size(), PSEMNAMLEN);
  return ret;
}

}  // namespace

Status MutableObjectManager::RegisterChannel(
    const ObjectID &object_id,
    std::unique_ptr<plasma::MutableObject> mutable_object,
    bool reader) {
  absl::MutexLock guard(&channel_lock_);
  const auto &[channel_pair, success] =
      channels_.emplace(object_id, std::move(mutable_object));
  Channel &channel = channel_pair->second;
  RAY_CHECK(channel.mutable_object);

  if (!success) {
    if ((reader && channel.reader_registered) || (!reader && channel.writer_registered)) {
      return Status::OK();
    }
  }

  if (reader) {
    channel.reader_registered = true;
  } else {
    channel.writer_registered = true;
  }

  OpenSemaphores(object_id, channel.mutable_object->header);
  return Status::OK();
}

MutableObjectManager::Channel *MutableObjectManager::GetChannel(
    const ObjectID &object_id) {
  absl::MutexLock guard(&channel_lock_);
  auto entry = channels_.find(object_id);
  if (entry == channels_.end()) {
    return nullptr;
  }
  return &entry->second;
}

MutableObjectManager::~MutableObjectManager() {
  destructor_lock_.Lock();

  // Copy `semaphores_` into `tmp` because `DestroySemaphores()` mutates `semaphores_`.
  absl::flat_hash_map<ObjectID, PlasmaObjectHeader::Semaphores> tmp = semaphores_;
  for (const auto &[object_id, _] : tmp) {
    (void)SetErrorInternal(object_id);
    DestroySemaphores(object_id);
  }
  channels_.clear();
  destructor_lock_.Unlock();
}

PlasmaObjectHeader *MutableObjectManager::GetHeader(const ObjectID &object_id) {
  Channel *channel = GetChannel(object_id);
  if (channel) {
    return channel->mutable_object->header;
  }
  RAY_CHECK(false);
  return nullptr;
}

std::string MutableObjectManager::GetSemaphoreName(PlasmaObjectHeader *header) {
  std::string name = std::string(header->unique_name);
  RAY_CHECK_LE(name.size(), PSEMNAMLEN);
  return name;
}

bool MutableObjectManager::GetSemaphores(const ObjectID &object_id,
                                         PlasmaObjectHeader::Semaphores &sem) {
  auto it = semaphores_.find(object_id);
  if (it == semaphores_.end()) {
    return false;
  }
  // We return a copy instead of a reference because absl::flat_hash_map does not provide
  // pointer stability. In other words, a reference could be invalidated if the map is
  // mutated.
  sem = it->second;
  return true;
}

void MutableObjectManager::OpenSemaphores(const ObjectID &object_id,
                                          PlasmaObjectHeader *header) {
  if (semaphores_.count(object_id)) {
    // The semaphore already exists.
    return;
  }

  bool create = false;
  PlasmaObjectHeader::SemaphoresCreationLevel level =
      header->semaphores_created.load(std::memory_order_relaxed);
  // The first thread to call `OpenSemaphores()` initializes the semaphores. This makes it
  // easier to set up the channel as the different processes calling this function do not
  // need to coordinate with each other (beyond this) when constructing the channel.
  if (level == PlasmaObjectHeader::SemaphoresCreationLevel::kUnitialized) {
    create = header->semaphores_created.compare_exchange_strong(
        /*expected=*/level,
        /*desired=*/PlasmaObjectHeader::SemaphoresCreationLevel::kInitializing,
        std::memory_order_relaxed);
  }

  PlasmaObjectHeader::Semaphores semaphores;
  std::string name = GetSemaphoreName(header);
  if (create) {
    // This channel is being initialized.
    // Attempt to unlink the semaphores just in case they were not cleaned up by a
    // previous test run that crashed.
    sem_unlink(GetSemaphoreHeaderName(name).c_str());
    sem_unlink(GetSemaphoreObjectName(name).c_str());

    semaphores.object_sem = sem_open(GetSemaphoreObjectName(name).c_str(),
                                     /*oflag=*/O_CREAT | O_EXCL,
                                     /*mode=*/0644,
                                     /*value=*/1);
    semaphores.header_sem = sem_open(GetSemaphoreHeaderName(name).c_str(),
                                     /*oflag=*/O_CREAT | O_EXCL,
                                     /*mode=*/0644,
                                     /*value=*/1);
    header->semaphores_created.store(PlasmaObjectHeader::SemaphoresCreationLevel::kDone,
                                     std::memory_order_release);
  } else {
    // Wait for another thread to initialize the channel.
    while (header->semaphores_created.load(std::memory_order_acquire) !=
           PlasmaObjectHeader::SemaphoresCreationLevel::kDone) {
      sched_yield();
    }
    semaphores.object_sem = sem_open(GetSemaphoreObjectName(name).c_str(), /*oflag=*/0);
    semaphores.header_sem = sem_open(GetSemaphoreHeaderName(name).c_str(), /*oflag=*/0);
  }
  RAY_CHECK_NE(semaphores.object_sem, SEM_FAILED);
  RAY_CHECK_NE(semaphores.header_sem, SEM_FAILED);

  semaphores_[object_id] = semaphores;
}

void MutableObjectManager::DestroySemaphores(const ObjectID &object_id) {
  RAY_LOG(DEBUG) << "Destroy " << object_id;
  PlasmaObjectHeader::Semaphores sem;
  if (!GetSemaphores(object_id, sem)) {
    return;
  }
  RAY_CHECK_EQ(sem_close(sem.header_sem), 0);
  RAY_CHECK_EQ(sem_close(sem.object_sem), 0);

  std::string name = GetSemaphoreName(GetHeader(object_id));
  // The core worker and the raylet each have their own MutableObjectManager instance, and
  // when both a reader and a writer are on the same machine, the reader and writer will
  // each register the same object with separate MutableObjectManager instances. Thus,
  // when the second instance is destructed, it will call sem_unlink() on the same two
  // semaphores below. As the two semaphores have already been unlinked by the first
  // instance, the sem_unlink() calls below will both fail with ENOENT.
  int ret = sem_unlink(GetSemaphoreHeaderName(name).c_str());
  if (ret) {
    RAY_CHECK_EQ(errno, ENOENT);
  }
  ret = sem_unlink(GetSemaphoreObjectName(name).c_str());
  if (ret) {
    RAY_CHECK_EQ(errno, ENOENT);
  }

  semaphores_.erase(object_id);
}

Status MutableObjectManager::WriteAcquire(const ObjectID &object_id,
                                          int64_t data_size,
                                          const uint8_t *metadata,
                                          int64_t metadata_size,
                                          int64_t num_readers,
                                          std::shared_ptr<Buffer> &data,
                                          int64_t timeout_ms) {
  RAY_LOG(DEBUG) << "WriteAcquire " << object_id;
  absl::ReaderMutexLock guard(&destructor_lock_);

  Channel *channel = GetChannel(object_id);
  if (!channel) {
    return Status::ChannelError("Channel has not been registered");
  }

  std::unique_ptr<plasma::MutableObject> &object = channel->mutable_object;
  int64_t total_size = data_size + metadata_size;
  if (total_size > object->allocated_size) {
    return Status::InvalidArgument(
        absl::StrFormat("Serialized size of mutable data (%ld) + metadata size (%ld) "
                        "is larger than allocated buffer size (%ld)",
                        data_size,
                        metadata_size,
                        object->allocated_size));
  }

  PlasmaObjectHeader::Semaphores sem;
  if (!GetSemaphores(object_id, sem)) {
    return Status::ChannelError(
        "Channel has not been registered (cannot get semaphores)");
  }
  // Check whether the channel has an error set before checking that WriteRelease() was
  // called. If the channel is already closed, then it's OK to WriteAcquire and
  // WriteRelease in any order.
  RAY_RETURN_NOT_OK(object->header->CheckHasError());
  RAY_CHECK(!channel->written) << "You must call WriteRelease() before WriteAcquire()";

  auto timeout_point = ToTimeoutPoint(timeout_ms);
  RAY_RETURN_NOT_OK(object->header->WriteAcquire(
      sem, data_size, metadata_size, num_readers, timeout_point));
  data = SharedMemoryBuffer::Slice(object->buffer, 0, data_size);
  if (metadata) {
    // Copy the metadata to the buffer.
    memcpy(data->Data() + data_size, metadata, metadata_size);
  }
  channel->written = true;
  return Status::OK();
}

Status MutableObjectManager::GetObjectBackingStore(const ObjectID &object_id,
                                                   int64_t data_size,
                                                   int64_t metadata_size,
                                                   std::shared_ptr<Buffer> &data) {
  RAY_LOG(DEBUG) << "WriteGetObjectBackingStore " << object_id;
  absl::ReaderMutexLock guard(&destructor_lock_);

  Channel *channel = GetChannel(object_id);
  if (!channel) {
    return Status::ChannelError("Channel has not been registered");
  }
  RAY_CHECK(channel->written);

  std::unique_ptr<plasma::MutableObject> &object = channel->mutable_object;
  int64_t total_size = data_size + metadata_size;
  data = SharedMemoryBuffer::Slice(object->buffer, 0, total_size);
  return Status::OK();
}

Status MutableObjectManager::WriteRelease(const ObjectID &object_id) {
  RAY_LOG(DEBUG) << "WriteRelease " << object_id;
  absl::ReaderMutexLock guard(&destructor_lock_);

  Channel *channel = GetChannel(object_id);
  if (!channel) {
    return Status::ChannelError("Channel has not been registered");
  }
  PlasmaObjectHeader::Semaphores sem;
  if (!GetSemaphores(object_id, sem)) {
    return Status::ChannelError(
        "Channel has not been registered (cannot get semaphores)");
  }
  std::unique_ptr<plasma::MutableObject> &object = channel->mutable_object;
  // Check whether the channel has an error set before checking that WriteAcquire() was
  // called. If the channel is already closed, then it's OK to WriteAcquire and
  // WriteRelease in any order.
  RAY_RETURN_NOT_OK(object->header->CheckHasError());
  RAY_CHECK(channel->written) << "You must call WriteAcquire() before WriteRelease()";

  RAY_RETURN_NOT_OK(object->header->WriteRelease(sem));
  channel->written = false;
  return Status::OK();
}

Status MutableObjectManager::ReadAcquire(const ObjectID &object_id,
                                         std::shared_ptr<RayObject> &result,
                                         int64_t timeout_ms)
    ABSL_NO_THREAD_SAFETY_ANALYSIS {
  RAY_LOG(DEBUG) << "ReadAcquire " << object_id;
  absl::ReaderMutexLock guard(&destructor_lock_);

  Channel *channel = GetChannel(object_id);
  if (!channel) {
    return Status::ChannelError("Channel has not been registered");
  }
  PlasmaObjectHeader::Semaphores sem;
  if (!GetSemaphores(object_id, sem)) {
    return Status::ChannelError(
        "Channel has not been registered (cannot get semaphores)");
  }

  std::unique_ptr<plasma::MutableObject> &object = channel->mutable_object;
  // Check whether the channel has an error set before checking that we are the only
  // reader. If the channel is already closed, then it's OK to ReadAcquire and
  // ReadRelease in any order.

  auto timeout_point = ToTimeoutPoint(timeout_ms);
  bool locked = false;
  bool expired = false;
  do {
    RAY_RETURN_NOT_OK(object->header->CheckHasError());
    // The channel is still open. This lock ensures that there is only one reader
    // at a time. The lock is released in `ReadRelease()`.
    locked = channel->lock->try_lock();
    expired = timeout_point && std::chrono::steady_clock::now() >= *timeout_point;
  } while (!locked && !expired);
  if (!locked) {
    // If timeout_ms == 0, we want to try once to get the lock,
    // therefore we check locked rather than expired.
    return Status::ChannelTimeoutError("Timed out acquiring the read lock.");
  }

  channel->reading = true;
  int64_t version_read = 0;
  Status s = object->header->ReadAcquire(
      sem, channel->next_version_to_read, version_read, timeout_point);
  if (!s.ok()) {
    RAY_LOG(DEBUG) << "ReadAcquire error was set, returning " << object_id;
    // Failed because the error bit was set on the mutable object.
    channel->reading = false;
    channel->lock->unlock();
    return s;
  }
  RAY_CHECK_GT(version_read, 0);
  channel->next_version_to_read = version_read;

  size_t total_size = object->header->data_size + object->header->metadata_size;
  RAY_CHECK_LE(static_cast<int64_t>(total_size), channel->mutable_object->allocated_size);
  std::shared_ptr<MutableObjectBuffer> channel_buffer =
      std::make_shared<MutableObjectBuffer>(
          shared_from_this(), channel->mutable_object->buffer, object_id);
  std::shared_ptr<SharedMemoryBuffer> data_buf =
      SharedMemoryBuffer::Slice(channel_buffer,
                                /*offset=*/0,
                                /*size=*/object->header->data_size);
  std::shared_ptr<SharedMemoryBuffer> metadata_buf =
      SharedMemoryBuffer::Slice(channel_buffer,
                                /*offset=*/object->header->data_size,
                                /*size=*/object->header->metadata_size);

  result = std::make_shared<RayObject>(
      std::move(data_buf), std::move(metadata_buf), std::vector<rpc::ObjectReference>());
  if (result->IsException()) {
    // If the result is an exception, currently we use zero-copy
    // deserialization to create the frontend exception object. This makes it
    // easy for the frontend code to leak the shared_ptr<MutableObjectBuffer>,
    // which will block future values from being written. Therefore, we make a
    // copy of the buffer here so that the MutableObjectBuffer can be
    // immediately released.
    std::shared_ptr<LocalMemoryBuffer> data_copy = std::make_shared<LocalMemoryBuffer>(
        channel_buffer->Data(),
        /*size=*/channel->mutable_object->header->data_size,
        /*copy_data=*/true);
    std::shared_ptr<LocalMemoryBuffer> metadata_copy =
        std::make_shared<LocalMemoryBuffer>(
            channel_buffer->Data() + channel->mutable_object->header->data_size,
            /*size=*/channel->mutable_object->header->metadata_size,
            /*copy_data=*/true);
    result = std::make_shared<RayObject>(std::move(data_copy),
                                         std::move(metadata_copy),
                                         std::vector<rpc::ObjectReference>());
  }
  RAY_LOG(DEBUG) << "ReadAcquire returning buffer " << object_id;
  return Status::OK();
}

Status MutableObjectManager::ReadRelease(const ObjectID &object_id)
    ABSL_NO_THREAD_SAFETY_ANALYSIS {
  RAY_LOG(DEBUG) << "ReadRelease " << object_id;
  absl::ReaderMutexLock guard(&destructor_lock_);

  Channel *channel = GetChannel(object_id);
  if (!channel) {
    return Status::ChannelError("Channel has not been registered");
  }

  PlasmaObjectHeader::Semaphores sem;
  RAY_CHECK(GetSemaphores(object_id, sem));

  // Check whether the channel has an error set before checking that we called
  // ReadAcquire. If the channel is already closed, then it's OK to ReadAcquire
  // and ReadRelease in any order.
  std::unique_ptr<plasma::MutableObject> &object = channel->mutable_object;
  RAY_RETURN_NOT_OK(object->header->CheckHasError());
  // The channel is still open. Make sure that we called ReadAcquire first.
  if (!channel->reading) {
    return Status::ChannelError(
        "Must call ReadAcquire() on the channel before ReadRelease()");
  }

  Status s = object->header->ReadRelease(sem, channel->next_version_to_read);
  if (!s.ok()) {
    RAY_LOG(DEBUG) << "ReadRelease error was set, returning: " << object_id;
    // Failed because the error bit was set on the mutable object.
    channel->reading = false;
    channel->lock->unlock();
    return s;
  }
  // The next read needs to read at least this version.
  channel->next_version_to_read++;

  // This lock ensures that there is only one reader at a time. The lock is acquired in
  // `ReadAcquire()`.
  channel->reading = false;
  channel->lock->unlock();
  return Status::OK();
}

Status MutableObjectManager::SetError(const ObjectID &object_id) {
  RAY_LOG(DEBUG) << "SetError " << object_id;
  absl::ReaderMutexLock guard(&destructor_lock_);
  return SetErrorInternal(object_id);
}

Status MutableObjectManager::SetErrorInternal(const ObjectID &object_id) {
  Channel *channel = GetChannel(object_id);
  if (channel) {
    PlasmaObjectHeader::Semaphores sem;
    if (!GetSemaphores(object_id, sem)) {
      return Status::ChannelError(
          "Channel has not been registered (cannot get semaphores)");
    }
    channel->mutable_object->header->SetErrorUnlocked(sem);
    channel->reader_registered = false;
    channel->writer_registered = false;
    // TODO(jhumphri): Free the channel.
  } else {
    return Status::ChannelError("Channel has not been registered");
  }
  return Status::OK();
}

Status MutableObjectManager::SetErrorAll() {
  absl::ReaderMutexLock guard(&destructor_lock_);

  Status ret = Status::OK();
  for (const auto &[object_id, _] : channels_) {
    ret = SetErrorInternal(object_id);
    if (ret.code() != StatusCode::OK) {
      break;
    }
  }
  return ret;
}

std::unique_ptr<std::chrono::steady_clock::time_point>
MutableObjectManager::ToTimeoutPoint(int64_t timeout_ms) {
  if (timeout_ms == -1) {
    return nullptr;
  }
  auto now = std::chrono::steady_clock::now();
  auto timeout_duration = std::chrono::milliseconds(timeout_ms);
  auto timeout_point =
      std::make_unique<std::chrono::steady_clock::time_point>(now + timeout_duration);
  return timeout_point;
}

Status MutableObjectManager::GetChannelStatus(const ObjectID &object_id, bool is_reader) {
  Channel *channel = GetChannel(object_id);
  if (!channel) {
    return Status::NotFound(
        absl::StrFormat("Could not find channel for object ID %s.", object_id.Hex()));
  }
  if ((is_reader && channel->reader_registered) ||
      (!is_reader && channel->writer_registered)) {
    return Status::OK();
  }
  return channel->mutable_object->header->CheckHasError();
}

#else  // defined(__APPLE__) || defined(__linux__)

MutableObjectManager::~MutableObjectManager() {}

Status MutableObjectManager::RegisterChannel(
    const ObjectID &object_id,
    std::unique_ptr<plasma::MutableObject> mutable_object,
    bool reader) {
  return Status::NotImplemented("Not supported on Windows.");
}

Status MutableObjectManager::GetObjectBackingStore(const ObjectID &object_id,
                                                   int64_t data_size,
                                                   int64_t metadata_size,
                                                   std::shared_ptr<Buffer> &data) {
  return Status::NotImplemented("Not supported on Windows.");
}

MutableObjectManager::Channel *MutableObjectManager::GetChannel(
    const ObjectID &object_id) {
  return nullptr;
}

PlasmaObjectHeader *MutableObjectManager::GetHeader(const ObjectID &object_id) {
  return nullptr;
}

std::string MutableObjectManager::GetSemaphoreName(PlasmaObjectHeader *header) {
  return "";
}

bool MutableObjectManager::GetSemaphores(const ObjectID &object_id,
                                         PlasmaObjectHeader::Semaphores &sem) {
  return false;
}

void MutableObjectManager::OpenSemaphores(const ObjectID &object_id,
                                          PlasmaObjectHeader *header) {}

void MutableObjectManager::DestroySemaphores(const ObjectID &object_id) {}

Status MutableObjectManager::WriteAcquire(const ObjectID &object_id,
                                          int64_t data_size,
                                          const uint8_t *metadata,
                                          int64_t metadata_size,
                                          int64_t num_readers,
                                          std::shared_ptr<Buffer> &data,
                                          int64_t timeout_ms) {
  return Status::NotImplemented("Not supported on Windows.");
}

Status MutableObjectManager::WriteRelease(const ObjectID &object_id) {
  return Status::NotImplemented("Not supported on Windows.");
}

Status MutableObjectManager::ReadAcquire(const ObjectID &object_id,
                                         std::shared_ptr<RayObject> &result,
                                         int64_t timeout_ms)
    ABSL_NO_THREAD_SAFETY_ANALYSIS {
  return Status::NotImplemented("Not supported on Windows.");
}

Status MutableObjectManager::ReadRelease(const ObjectID &object_id)
    ABSL_NO_THREAD_SAFETY_ANALYSIS {
  return Status::NotImplemented("Not supported on Windows.");
}

Status MutableObjectManager::SetError(const ObjectID &object_id) {
  return Status::NotImplemented("Not supported on Windows.");
}

Status MutableObjectManager::SetErrorInternal(const ObjectID &object_id) {
  return Status::NotImplemented("Not supported on Windows.");
}

Status MutableObjectManager::SetErrorAll() {
  return Status::NotImplemented("Not supported on Windows.");
}

std::unique_ptr<std::chrono::steady_clock::time_point>
MutableObjectManager::ToTimeoutPoint(int64_t timeout_ms) {
  return nullptr;
}

Status MutableObjectManager::GetChannelStatus(const ObjectID &object_id, bool is_reader) {
  return Status::NotImplemented("Not supported on Windows.");
}

#endif

}  // namespace experimental
}  // namespace ray
