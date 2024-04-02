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

template <typename T>
Status MutableObjectManager::RegisterChannel(
    absl::flat_hash_map<ObjectID, T> &channels,
    const ObjectID &object_id,
    std::unique_ptr<plasma::MutableObject> &mutable_object) {
  const auto &[channel_pair, success] =
      channels.emplace(object_id, T(std::move(mutable_object)));
  if (!success) {
    return Status::Invalid("Channel already registered");
  }
  const T &channel = channel_pair->second;
  RAY_CHECK(channel.mutable_object);

  OpenSemaphores(object_id);
  return Status::OK();
}

Status MutableObjectManager::RegisterWriterChannel(
    const ObjectID &object_id, std::unique_ptr<plasma::MutableObject> mutable_object) {
  return RegisterChannel<WriterChannel>(writer_channels_, object_id, mutable_object);
}

Status MutableObjectManager::RegisterReaderChannel(
    const ObjectID &object_id, std::unique_ptr<plasma::MutableObject> mutable_object) {
  return RegisterChannel<ReaderChannel>(reader_channels_, object_id, mutable_object);
}

MutableObjectManager::~MutableObjectManager() {
  // Copy `semaphores_` into `tmp` because `DestroySemaphores()` mutates `semaphores_`.
  absl::flat_hash_map<ObjectID, PlasmaObjectHeader::Semaphores> tmp = semaphores_;
  for (const auto &[object_id, _] : tmp) {
    DestroySemaphores(object_id);
  }
}

PlasmaObjectHeader *MutableObjectManager::GetHeader(const ObjectID &object_id) {
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

std::string MutableObjectManager::GetSemaphoreName(const ObjectID &object_id) {
  std::string name = std::string(GetHeader(object_id)->unique_name);
  RAY_CHECK_LE(name.size(), PSEMNAMLEN);
  return name;
}

PlasmaObjectHeader::Semaphores MutableObjectManager::GetSemaphores(
    const ObjectID &object_id) {
  auto it = semaphores_.find(object_id);
  RAY_CHECK(it != semaphores_.end());
  // We return a copy instead of a reference because absl::flat_hash_map does not provide
  // pointer stability. In other words, a reference could be invalidated if the map is
  // mutated.
  return it->second;
}

void MutableObjectManager::OpenSemaphores(const ObjectID &object_id) {
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

void MutableObjectManager::DestroySemaphores(const ObjectID &object_id) {
  PlasmaObjectHeader::Semaphores semaphores = GetSemaphores(object_id);
  RAY_CHECK_EQ(sem_close(semaphores.header_sem), 0);
  RAY_CHECK_EQ(sem_close(semaphores.object_sem), 0);

  std::string name = GetSemaphoreName(object_id);
  RAY_CHECK_EQ(sem_unlink(GetSemaphoreHeaderName(name).c_str()), 0);
  RAY_CHECK_EQ(sem_unlink(GetSemaphoreObjectName(name).c_str()), 0);

  semaphores_.erase(object_id);
}

Status MutableObjectManager::WriteAcquire(const ObjectID &object_id,
                                          int64_t data_size,
                                          const uint8_t *metadata,
                                          int64_t metadata_size,
                                          int64_t num_readers,
                                          std::shared_ptr<Buffer> &data) {
  WriterChannel *channel = GetChannel<WriterChannel>(writer_channels_, object_id);
  if (!channel) {
    return Status::ObjectNotFound("Writer channel has not been registered");
  }
  RAY_CHECK(!channel->written) << "You must call WriteAcquire() before WriteRelease()";

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

  PlasmaObjectHeader::Semaphores sem = GetSemaphores(object_id);
  RAY_RETURN_NOT_OK(
      object->header->WriteAcquire(sem, data_size, metadata_size, num_readers));
  data = SharedMemoryBuffer::Slice(object->buffer, 0, data_size);
  if (metadata) {
    // Copy the metadata to the buffer.
    memcpy(data->Data() + data_size, metadata, metadata_size);
  }
  channel->written = true;
  return Status::OK();
}

Status MutableObjectManager::WriteRelease(const ObjectID &object_id) {
  WriterChannel *channel = GetChannel<WriterChannel>(writer_channels_, object_id);
  if (!channel) {
    return Status::ObjectNotFound("Writer channel has not been registered");
  }
  RAY_CHECK(channel->written) << "You must call WriteAcquire() before WriteRelease()";

  PlasmaObjectHeader::Semaphores sem = GetSemaphores(object_id);
  std::unique_ptr<plasma::MutableObject> &object = channel->mutable_object;
  RAY_RETURN_NOT_OK(object->header->WriteRelease(sem));
  channel->written = false;
  return Status::OK();
}

Status MutableObjectManager::ReadAcquire(const ObjectID &object_id,
                                         std::shared_ptr<RayObject> &result)
    ABSL_NO_THREAD_SAFETY_ANALYSIS {
  ReaderChannel *channel = GetChannel<ReaderChannel>(reader_channels_, object_id);
  if (!channel) {
    return Status::ObjectNotFound("Reader channel has not been registered");
  }
  // This lock ensures that there is only one reader at a time. The lock is released in
  // `ReadRelease()`.
  channel->lock->Lock();

  PlasmaObjectHeader::Semaphores sem = GetSemaphores(object_id);
  int64_t version_read = 0;
  RAY_RETURN_NOT_OK(channel->mutable_object->header->ReadAcquire(
      sem, channel->next_version_to_read, &version_read));
  RAY_CHECK_GT(version_read, 0);
  channel->next_version_to_read = version_read;

  size_t total_size = channel->mutable_object->header->data_size +
                      channel->mutable_object->header->metadata_size;
  RAY_CHECK_LE(static_cast<int64_t>(total_size), channel->mutable_object->allocated_size);
  std::shared_ptr<SharedMemoryBuffer> data_buf =
      SharedMemoryBuffer::Slice(channel->mutable_object->buffer,
                                /*offset=*/0,
                                /*size=*/channel->mutable_object->header->data_size);
  std::shared_ptr<SharedMemoryBuffer> metadata_buf =
      SharedMemoryBuffer::Slice(channel->mutable_object->buffer,
                                /*offset=*/channel->mutable_object->header->data_size,
                                /*size=*/channel->mutable_object->header->metadata_size);

  result = std::make_shared<RayObject>(
      std::move(data_buf), std::move(metadata_buf), std::vector<rpc::ObjectReference>());
  return Status::OK();
}

Status MutableObjectManager::ReadRelease(const ObjectID &object_id)
    ABSL_NO_THREAD_SAFETY_ANALYSIS {
  ReaderChannel *channel = GetChannel<ReaderChannel>(reader_channels_, object_id);
  if (!channel) {
    return Status::ObjectNotFound("Reader channel has not been registered");
  }

  PlasmaObjectHeader::Semaphores sem = GetSemaphores(object_id);
  RAY_RETURN_NOT_OK(
      channel->mutable_object->header->ReadRelease(sem, channel->next_version_to_read));
  // The next read needs to read at least this version.
  channel->next_version_to_read++;

  // This lock ensures that there is only one reader at a time. The lock is acquired in
  // `ReadAcquire()`.
  channel->lock->Unlock();
  return Status::OK();
}

Status MutableObjectManager::SetError(const ObjectID &object_id) {
  Channel *reader = GetChannel<ReaderChannel>(reader_channels_, object_id);
  Channel *writer = GetChannel<WriterChannel>(writer_channels_, object_id);
  PlasmaObjectHeader::Semaphores sem = GetSemaphores(object_id);
  if (reader) {
    reader->mutable_object->header->SetErrorUnlocked(sem);
  }
  if (writer) {
    writer->mutable_object->header->SetErrorUnlocked(sem);
  }

  if (!reader && !writer) {
    return Status::ObjectNotFound("Writer or reader channel has not been registered");
  }
  return Status::OK();
}

#else  // defined(__APPLE__) || defined(__linux__)

MutableObjectManager::~MutableObjectManager() {}

template <typename T>
Status MutableObjectManager::RegisterChannel(
    absl::flat_hash_map<ObjectID, T> &channels,
    const ObjectID &object_id,
    std::unique_ptr<plasma::MutableObject> &mutable_object) {
  return Status::NotImplemented("Not supported on Windows.");
}

Status MutableObjectManager::RegisterWriterChannel(
    const ObjectID &object_id, std::unique_ptr<plasma::MutableObject> mutable_object) {
  return Status::NotImplemented("Not supported on Windows.");
}

Status MutableObjectManager::RegisterReaderChannel(
    const ObjectID &object_id, std::unique_ptr<plasma::MutableObject> mutable_object) {
  return Status::NotImplemented("Not supported on Windows.");
}

PlasmaObjectHeader *MutableObjectManager::GetHeader(const ObjectID &object_id) {
  return nullptr;
}

std::string MutableObjectManager::GetSemaphoreName(const ObjectID &object_id) {
  return "";
}

PlasmaObjectHeader::Semaphores MutableObjectManager::GetSemaphores(
    const ObjectID &object_id) {
  return {};
}

void MutableObjectManager::OpenSemaphores(const ObjectID &object_id) {}

void MutableObjectManager::DestroySemaphores(const ObjectID &object_id) {}

Status MutableObjectManager::WriteAcquire(const ObjectID &object_id,
                                          int64_t data_size,
                                          const uint8_t *metadata,
                                          int64_t metadata_size,
                                          int64_t num_readers,
                                          std::shared_ptr<Buffer> &data) {
  return Status::NotImplemented("Not supported on Windows.");
}

Status MutableObjectManager::WriteRelease(const ObjectID &object_id) {
  return Status::NotImplemented("Not supported on Windows.");
}

Status MutableObjectManager::ReadAcquire(const ObjectID &object_id,
                                         std::shared_ptr<RayObject> &result)
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

#endif

}  // namespace experimental
}  // namespace ray
