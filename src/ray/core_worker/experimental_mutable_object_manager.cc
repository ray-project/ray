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

#include "ray/object_manager/common.h"

namespace ray {
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

ExperimentalMutableObjectManager::~ExperimentalMutableObjectManager() {
  // Copy `semaphores_` into `tmp` because `DestroySemaphores()` mutates `semaphores_`.
  absl::flat_hash_map<ObjectID, PlasmaObjectHeader::Semaphores> tmp = semaphores_;
  for (const auto &[object_id, _] : tmp) {
    DestroySemaphores(object_id);
  }
}

Status ExperimentalMutableObjectManager::RegisterWriterChannel(
    const ObjectID &object_id, std::unique_ptr<plasma::MutableObject> mutable_object) {
  auto inserted =
      writer_channels_.emplace(object_id, WriterChannel(std::move(mutable_object)));
  if (!inserted.second) {
    return Status::Invalid("Writer channel already registered");
  }
  RAY_CHECK(inserted.first->second.mutable_object);
  OpenSemaphores(object_id);
  RAY_LOG(DEBUG) << "Registered writer channel " << object_id;
  return Status::OK();
}

PlasmaObjectHeader *ExperimentalMutableObjectManager::GetHeader(
    const ObjectID &object_id) {
  {
    auto it = writer_channels_.find(object_id);
    if (it != writer_channels_.end()) {
      return it->second.mutable_object->header;
    }
  }
  {
    auto it = reader_channels_.find(object_id);
    if (it != reader_channels_.end()) {
      return it->second.mutable_object->header;
    }
  }
  RAY_CHECK(false);
  return nullptr;
}

std::string ExperimentalMutableObjectManager::GetSemaphoreName(
    const ObjectID &object_id) {
  std::string name = std::string(GetHeader(object_id)->unique_name);
  RAY_CHECK_LE(name.size(), PSEMNAMLEN);
  return name;
}

PlasmaObjectHeader::Semaphores ExperimentalMutableObjectManager::GetSemaphores(
    const ObjectID &object_id) {
  auto it = semaphores_.find(object_id);
  RAY_CHECK(it != semaphores_.end());
  // We return a copy instead of a reference because absl::flat_hash_map does not provide
  // pointer stability. In other words, a reference could be invalidated if the map is
  // mutated.
  return it->second;
}

void ExperimentalMutableObjectManager::OpenSemaphores(const ObjectID &object_id) {
  if (semaphores_.count(object_id)) {
    // The semaphore already exists.
    return;
  }

  bool create = false;
  PlasmaObjectHeader *header = GetHeader(object_id);
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
  std::string name = GetSemaphoreName(object_id);
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
    // This is a reader initializing the channel.
    // Wait for another thread to initialize the channel.
    while (GetHeader(object_id)->semaphores_created.load(std::memory_order_acquire) !=
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

void ExperimentalMutableObjectManager::DestroySemaphores(const ObjectID &object_id) {
  PlasmaObjectHeader::Semaphores semaphores = GetSemaphores(object_id);
  RAY_CHECK_EQ(sem_close(semaphores.header_sem), 0);
  RAY_CHECK_EQ(sem_close(semaphores.object_sem), 0);

  std::string name = GetSemaphoreName(object_id);
  RAY_CHECK_EQ(sem_unlink(GetSemaphoreHeaderName(name).c_str()), 0);
  RAY_CHECK_EQ(sem_unlink(GetSemaphoreObjectName(name).c_str()), 0);

  semaphores_.erase(object_id);
}

Status ExperimentalMutableObjectManager::WriteAcquire(const ObjectID &object_id,
                                                      int64_t data_size,
                                                      const uint8_t *metadata,
                                                      int64_t metadata_size,
                                                      int64_t num_readers,
                                                      std::shared_ptr<Buffer> *data) {
  auto writer_channel_entry = writer_channels_.find(object_id);
  if (writer_channel_entry == writer_channels_.end()) {
    return Status::ObjectNotFound("Writer channel has not been registered");
  }

  auto &channel = writer_channel_entry->second;
  if (!channel.is_sealed) {
    return Status::Invalid("Must WriteRelease before writing again to a mutable object");
  }

  if (data_size + metadata_size > channel.mutable_object->allocated_size) {
    return Status::InvalidArgument(
        "Serialized size of mutable data (" + std::to_string(data_size) +
        ") + metadata size (" + std::to_string(metadata_size) +
        ") is larger than allocated buffer size " +
        std::to_string(channel.mutable_object->allocated_size));
  }

  RAY_LOG(DEBUG) << "Write mutable object " << object_id;
  PlasmaObjectHeader::Semaphores sem = GetSemaphores(object_id);
  RAY_RETURN_NOT_OK(channel.mutable_object->header->WriteAcquire(
      sem, data_size, metadata_size, num_readers));
  channel.is_sealed = false;

  *data = SharedMemoryBuffer::Slice(
      channel.mutable_object->buffer, 0, channel.mutable_object->header->data_size);
  if (metadata != NULL) {
    // Copy the metadata to the buffer.
    memcpy((*data)->Data() + channel.mutable_object->header->data_size,
           metadata,
           metadata_size);
  }
  return Status::OK();
}

Status ExperimentalMutableObjectManager::WriteRelease(const ObjectID &object_id) {
  auto writer_channel_entry = writer_channels_.find(object_id);
  if (writer_channel_entry == writer_channels_.end()) {
    return Status::ObjectNotFound("Writer channel has not been registered");
  }

  auto &channel = writer_channel_entry->second;
  if (channel.is_sealed) {
    return Status::Invalid("Must WriteAcquire before WriteRelease on a mutable object");
  }

  PlasmaObjectHeader::Semaphores sem = GetSemaphores(object_id);
  RAY_RETURN_NOT_OK(channel.mutable_object->header->WriteRelease(sem));
  channel.is_sealed = true;

  return Status::OK();
}

Status ExperimentalMutableObjectManager::RegisterReaderChannel(
    const ObjectID &object_id, std::unique_ptr<plasma::MutableObject> mutable_object) {
  auto inserted =
      reader_channels_.emplace(object_id, ReaderChannel(std::move(mutable_object)));
  if (!inserted.second) {
    return Status::Invalid("Reader channel already registered");
  }
  OpenSemaphores(object_id);
  RAY_LOG(DEBUG) << "Registered reader channel " << object_id;
  return Status::OK();
}

bool ExperimentalMutableObjectManager::ReaderChannelRegistered(
    const ObjectID &object_id) const {
  return reader_channels_.count(object_id);
}

Status ExperimentalMutableObjectManager::ReadAcquire(const ObjectID &object_id,
                                                     std::shared_ptr<RayObject> *result)
    ABSL_NO_THREAD_SAFETY_ANALYSIS {
  RAY_LOG(DEBUG) << "ReadAcquire  " << object_id;
  auto reader_channel_entry = reader_channels_.find(object_id);
  if (reader_channel_entry == reader_channels_.end()) {
    return Status::ObjectNotFound("Reader channel has not been registered");
  }
  auto &channel = reader_channel_entry->second;

  // This lock ensures that there is only one reader at a time. The lock is released in
  // `ReadRelease()`.
  channel.lock->Lock();

  int64_t version_read = 0;
  RAY_LOG(DEBUG) << "ReadAcquire " << object_id
                 << " version: " << channel.next_version_to_read;
  PlasmaObjectHeader::Semaphores sem = GetSemaphores(object_id);
  RAY_RETURN_NOT_OK(channel.mutable_object->header->ReadAcquire(
      sem, channel.next_version_to_read, &version_read));
  RAY_CHECK(version_read > 0);
  channel.next_version_to_read = version_read;

  RAY_CHECK(static_cast<int64_t>(channel.mutable_object->header->data_size +
                                 channel.mutable_object->header->metadata_size) <=
            channel.mutable_object->allocated_size);
  auto data_buf = SharedMemoryBuffer::Slice(
      channel.mutable_object->buffer, 0, channel.mutable_object->header->data_size);
  auto metadata_buf =
      SharedMemoryBuffer::Slice(channel.mutable_object->buffer,
                                channel.mutable_object->header->data_size,
                                channel.mutable_object->header->metadata_size);

  *result = std::make_shared<RayObject>(
      std::move(data_buf), std::move(metadata_buf), std::vector<rpc::ObjectReference>());

  return Status::OK();
}

Status ExperimentalMutableObjectManager::ReadRelease(const ObjectID &object_id)
    ABSL_NO_THREAD_SAFETY_ANALYSIS {
  auto reader_channel_entry = reader_channels_.find(object_id);
  if (reader_channel_entry == reader_channels_.end()) {
    return Status::ObjectNotFound("Reader channel has not been registered");
  }

  auto &channel = reader_channel_entry->second;
  channel.lock->AssertHeld();

  PlasmaObjectHeader::Semaphores sem = GetSemaphores(object_id);
  RAY_RETURN_NOT_OK(
      channel.mutable_object->header->ReadRelease(sem, channel.next_version_to_read));
  // The next read needs to read at least this version.
  channel.next_version_to_read++;
  // This lock ensures that there is only one reader at a time. The lock is acquired in
  // `ReadAcquire()`.
  channel.lock->Unlock();

  return Status::OK();
}

Status ExperimentalMutableObjectManager::SetError(const ObjectID &object_id) {
  PlasmaObjectHeader *header = nullptr;
  auto reader_channel_entry = reader_channels_.find(object_id);
  if (reader_channel_entry != reader_channels_.end()) {
    header = reader_channel_entry->second.mutable_object->header;
  } else {
    auto writer_channel_entry = writer_channels_.find(object_id);
    if (writer_channel_entry != writer_channels_.end()) {
      header = writer_channel_entry->second.mutable_object->header;
    }
  }

  if (!header) {
    return Status::ObjectNotFound("Writer or reader channel has not been registered");
  }

  PlasmaObjectHeader::Semaphores sem = GetSemaphores(object_id);
  header->SetErrorUnlocked(sem);
  return Status::OK();
}

}  // namespace ray
