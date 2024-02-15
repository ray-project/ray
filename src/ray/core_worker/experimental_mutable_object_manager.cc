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

namespace ray {

Status ExperimentalMutableObjectManager::RegisterWriterChannel(
    const ObjectID &object_id, std::unique_ptr<plasma::MutableObject> mutable_object) {
#ifdef __linux__
  auto inserted =
      writer_channels_.emplace(object_id, WriterChannel(std::move(mutable_object)));
  if (!inserted.second) {
    return Status::Invalid("Writer channel already registered");
  }
  RAY_CHECK(inserted.first->second.mutable_object);
  RAY_LOG(DEBUG) << "Registered writer channel " << object_id;
#endif
  return Status::OK();
}

Status ExperimentalMutableObjectManager::WriteAcquire(const ObjectID &object_id,
                                                      int64_t data_size,
                                                      const uint8_t *metadata,
                                                      int64_t metadata_size,
                                                      int64_t num_readers,
                                                      std::shared_ptr<Buffer> *data) {
#ifdef __linux__
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
  RAY_RETURN_NOT_OK(channel.mutable_object->header->WriteAcquire(
      data_size, metadata_size, num_readers));
  channel.is_sealed = false;

  *data = SharedMemoryBuffer::Slice(
      channel.mutable_object->buffer, 0, channel.mutable_object->header->data_size);
  if (metadata != NULL) {
    // Copy the metadata to the buffer.
    memcpy((*data)->Data() + channel.mutable_object->header->data_size,
           metadata,
           metadata_size);
  }
#endif
  return Status::OK();
}

Status ExperimentalMutableObjectManager::WriteRelease(const ObjectID &object_id) {
#ifdef __linux__
  auto writer_channel_entry = writer_channels_.find(object_id);
  if (writer_channel_entry == writer_channels_.end()) {
    return Status::ObjectNotFound("Writer channel has not been registered");
  }

  auto &channel = writer_channel_entry->second;
  if (channel.is_sealed) {
    return Status::Invalid("Must WriteAcquire before WriteRelease on a mutable object");
  }

  RAY_RETURN_NOT_OK(channel.mutable_object->header->WriteRelease());
  channel.is_sealed = true;

#endif
  return Status::OK();
}

Status ExperimentalMutableObjectManager::RegisterReaderChannel(
    const ObjectID &object_id, std::unique_ptr<plasma::MutableObject> mutable_object) {
#ifdef __linux__
  auto inserted =
      reader_channels_.emplace(object_id, ReaderChannel(std::move(mutable_object)));
  if (!inserted.second) {
    return Status::Invalid("Reader channel already registered");
  }
  RAY_LOG(DEBUG) << "Registered reader channel " << object_id;
#endif
  return Status::OK();
}

bool ExperimentalMutableObjectManager::ReaderChannelRegistered(
    const ObjectID &object_id) const {
  return reader_channels_.count(object_id);
}

Status ExperimentalMutableObjectManager::ReadAcquire(const ObjectID &object_id,
                                                     std::shared_ptr<RayObject> *result) {
#ifdef __linux__
  auto reader_channel_entry = reader_channels_.find(object_id);
  if (reader_channel_entry == reader_channels_.end()) {
    return Status::ObjectNotFound("Reader channel has not been registered");
  }
  auto &channel = reader_channel_entry->second;
  RAY_RETURN_NOT_OK(EnsureGetAcquired(channel));

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

#endif
  return Status::OK();
}

Status ExperimentalMutableObjectManager::ReadRelease(const ObjectID &object_id) {
#ifdef __linux__
  auto reader_channel_entry = reader_channels_.find(object_id);
  if (reader_channel_entry == reader_channels_.end()) {
    return Status::ObjectNotFound("Reader channel has not been registered");
  }

  auto &channel = reader_channel_entry->second;
  RAY_RETURN_NOT_OK(EnsureGetAcquired(channel));

  RAY_RETURN_NOT_OK(
      channel.mutable_object->header->ReadRelease(channel.next_version_to_read));
  // The next read needs to read at least this version.
  channel.next_version_to_read++;
  channel.read_acquired = false;

#endif
  return Status::OK();
}

Status ExperimentalMutableObjectManager::EnsureGetAcquired(ReaderChannel &channel) {
#ifdef __linux__
  if (channel.read_acquired) {
    return Status::OK();
  }

  int64_t version_read = 0;
  RAY_RETURN_NOT_OK(channel.mutable_object->header->ReadAcquire(
      channel.next_version_to_read, &version_read));
  RAY_CHECK(version_read > 0);
  channel.next_version_to_read = version_read;
  channel.read_acquired = true;
#endif
  return Status::OK();
}

Status ExperimentalMutableObjectManager::SetError(const ObjectID &object_id) {
#ifdef __linux__
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

  header->SetErrorUnlocked();
#endif
  return Status::OK();
}

}  // namespace ray
