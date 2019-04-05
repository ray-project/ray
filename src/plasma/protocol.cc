// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "plasma/protocol.h"

#include <utility>

#include "flatbuffers/flatbuffers.h"
#include "plasma/plasma_generated.h"

#include "plasma/common.h"
#include "plasma/io.h"

#ifdef PLASMA_CUDA
#include "arrow/gpu/cuda_api.h"
#endif

namespace fb = plasma::flatbuf;

namespace plasma {

using fb::MessageType;
using fb::PlasmaError;
using fb::PlasmaObjectSpec;

using flatbuffers::uoffset_t;

#define PLASMA_CHECK_ENUM(x, y) \
  static_assert(static_cast<int>(x) == static_cast<int>(y), "protocol mismatch")

flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>>
ToFlatbuffer(flatbuffers::FlatBufferBuilder* fbb, const ObjectID* object_ids,
             int64_t num_objects) {
  std::vector<flatbuffers::Offset<flatbuffers::String>> results;
  for (int64_t i = 0; i < num_objects; i++) {
    results.push_back(fbb->CreateString(object_ids[i].binary()));
  }
  return fbb->CreateVector(results);
}

Status PlasmaReceive(int sock, MessageType message_type, std::vector<uint8_t>* buffer) {
  MessageType type;
  RETURN_NOT_OK(ReadMessage(sock, &type, buffer));
  ARROW_CHECK(type == message_type)
      << "type = " << static_cast<int64_t>(type)
      << ", message_type = " << static_cast<int64_t>(message_type);
  return Status::OK();
}

// Helper function to create a vector of elements from Data (Request/Reply struct).
// The Getter function is used to extract one element from Data.
template <typename T, typename Data, typename Getter>
void ToVector(const Data& request, std::vector<T>* out, const Getter& getter) {
  int count = request.count();
  out->clear();
  out->reserve(count);
  for (int i = 0; i < count; ++i) {
    out->push_back(getter(request, i));
  }
}

template <typename Message>
Status PlasmaSend(int sock, MessageType message_type, flatbuffers::FlatBufferBuilder* fbb,
                  const Message& message) {
  fbb->Finish(message);
  return WriteMessage(sock, message_type, fbb->GetSize(), fbb->GetBufferPointer());
}

Status PlasmaErrorStatus(fb::PlasmaError plasma_error) {
  switch (plasma_error) {
    case fb::PlasmaError::OK:
      return Status::OK();
    case fb::PlasmaError::ObjectExists:
      return Status::PlasmaObjectExists("object already exists in the plasma store");
    case fb::PlasmaError::ObjectNonexistent:
      return Status::PlasmaObjectNonexistent("object does not exist in the plasma store");
    case fb::PlasmaError::OutOfMemory:
      return Status::PlasmaStoreFull("object does not fit in the plasma store");
    default:
      ARROW_LOG(FATAL) << "unknown plasma error code " << static_cast<int>(plasma_error);
  }
  return Status::OK();
}

// Create messages.

Status SendCreateRequest(int sock, ObjectID object_id, int64_t data_size,
                         int64_t metadata_size, int device_num) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaCreateRequest(fbb, fbb.CreateString(object_id.binary()),
                                               data_size, metadata_size, device_num);
  return PlasmaSend(sock, MessageType::PlasmaCreateRequest, &fbb, message);
}

Status ReadCreateRequest(uint8_t* data, size_t size, ObjectID* object_id,
                         int64_t* data_size, int64_t* metadata_size, int* device_num) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaCreateRequest>(data);
  DCHECK(VerifyFlatbuffer(message, data, size));
  *data_size = message->data_size();
  *metadata_size = message->metadata_size();
  *object_id = ObjectID::from_binary(message->object_id()->str());
  *device_num = message->device_num();
  return Status::OK();
}

Status SendCreateReply(int sock, ObjectID object_id, PlasmaObject* object,
                       PlasmaError error_code, int64_t mmap_size) {
  flatbuffers::FlatBufferBuilder fbb;
  PlasmaObjectSpec plasma_object(object->store_fd, object->data_offset, object->data_size,
                                 object->metadata_offset, object->metadata_size,
                                 object->device_num);
  auto object_string = fbb.CreateString(object_id.binary());
#ifdef PLASMA_CUDA
  flatbuffers::Offset<fb::CudaHandle> ipc_handle;
  if (object->device_num != 0) {
    std::shared_ptr<arrow::Buffer> handle;
    RETURN_NOT_OK(object->ipc_handle->Serialize(arrow::default_memory_pool(), &handle));
    ipc_handle =
        fb::CreateCudaHandle(fbb, fbb.CreateVector(handle->data(), handle->size()));
  }
#endif
  fb::PlasmaCreateReplyBuilder crb(fbb);
  crb.add_error(static_cast<PlasmaError>(error_code));
  crb.add_plasma_object(&plasma_object);
  crb.add_object_id(object_string);
  crb.add_store_fd(object->store_fd);
  crb.add_mmap_size(mmap_size);
  if (object->device_num != 0) {
#ifdef PLASMA_CUDA
    crb.add_ipc_handle(ipc_handle);
#else
    ARROW_LOG(FATAL) << "This should be unreachable.";
#endif
  }
  auto message = crb.Finish();
  return PlasmaSend(sock, MessageType::PlasmaCreateReply, &fbb, message);
}

Status ReadCreateReply(uint8_t* data, size_t size, ObjectID* object_id,
                       PlasmaObject* object, int* store_fd, int64_t* mmap_size) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaCreateReply>(data);
  DCHECK(VerifyFlatbuffer(message, data, size));
  *object_id = ObjectID::from_binary(message->object_id()->str());
  object->store_fd = message->plasma_object()->segment_index();
  object->data_offset = message->plasma_object()->data_offset();
  object->data_size = message->plasma_object()->data_size();
  object->metadata_offset = message->plasma_object()->metadata_offset();
  object->metadata_size = message->plasma_object()->metadata_size();

  *store_fd = message->store_fd();
  *mmap_size = message->mmap_size();

  object->device_num = message->plasma_object()->device_num();
#ifdef PLASMA_CUDA
  if (object->device_num != 0) {
    RETURN_NOT_OK(CudaIpcMemHandle::FromBuffer(message->ipc_handle()->handle()->data(),
                                               &object->ipc_handle));
  }
#endif
  return PlasmaErrorStatus(message->error());
}

Status SendCreateAndSealRequest(int sock, const ObjectID& object_id,
                                const std::string& data, const std::string& metadata,
                                unsigned char* digest) {
  flatbuffers::FlatBufferBuilder fbb;
  auto digest_string = fbb.CreateString(reinterpret_cast<char*>(digest), kDigestSize);
  auto message = fb::CreatePlasmaCreateAndSealRequest(
      fbb, fbb.CreateString(object_id.binary()), fbb.CreateString(data),
      fbb.CreateString(metadata), digest_string);
  return PlasmaSend(sock, MessageType::PlasmaCreateAndSealRequest, &fbb, message);
}

Status ReadCreateAndSealRequest(uint8_t* data, size_t size, ObjectID* object_id,
                                std::string* object_data, std::string* metadata,
                                unsigned char* digest) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaCreateAndSealRequest>(data);
  DCHECK(VerifyFlatbuffer(message, data, size));

  *object_id = ObjectID::from_binary(message->object_id()->str());
  *object_data = message->data()->str();
  *metadata = message->metadata()->str();
  ARROW_CHECK(message->digest()->size() == kDigestSize);
  memcpy(digest, message->digest()->data(), kDigestSize);
  return Status::OK();
}

Status SendCreateAndSealReply(int sock, PlasmaError error) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaCreateAndSealReply(fbb, static_cast<PlasmaError>(error));
  return PlasmaSend(sock, MessageType::PlasmaCreateAndSealReply, &fbb, message);
}

Status ReadCreateAndSealReply(uint8_t* data, size_t size) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaCreateAndSealReply>(data);
  DCHECK(VerifyFlatbuffer(message, data, size));
  return PlasmaErrorStatus(message->error());
}

Status SendAbortRequest(int sock, ObjectID object_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaAbortRequest(fbb, fbb.CreateString(object_id.binary()));
  return PlasmaSend(sock, MessageType::PlasmaAbortRequest, &fbb, message);
}

Status ReadAbortRequest(uint8_t* data, size_t size, ObjectID* object_id) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaAbortRequest>(data);
  DCHECK(VerifyFlatbuffer(message, data, size));
  *object_id = ObjectID::from_binary(message->object_id()->str());
  return Status::OK();
}

Status SendAbortReply(int sock, ObjectID object_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaAbortReply(fbb, fbb.CreateString(object_id.binary()));
  return PlasmaSend(sock, MessageType::PlasmaAbortReply, &fbb, message);
}

Status ReadAbortReply(uint8_t* data, size_t size, ObjectID* object_id) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaAbortReply>(data);
  DCHECK(VerifyFlatbuffer(message, data, size));
  *object_id = ObjectID::from_binary(message->object_id()->str());
  return Status::OK();
}

// Seal messages.

Status SendSealRequest(int sock, ObjectID object_id, unsigned char* digest) {
  flatbuffers::FlatBufferBuilder fbb;
  auto digest_string = fbb.CreateString(reinterpret_cast<char*>(digest), kDigestSize);
  auto message = fb::CreatePlasmaSealRequest(fbb, fbb.CreateString(object_id.binary()),
                                             digest_string);
  return PlasmaSend(sock, MessageType::PlasmaSealRequest, &fbb, message);
}

Status ReadSealRequest(uint8_t* data, size_t size, ObjectID* object_id,
                       unsigned char* digest) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaSealRequest>(data);
  DCHECK(VerifyFlatbuffer(message, data, size));
  *object_id = ObjectID::from_binary(message->object_id()->str());
  ARROW_CHECK(message->digest()->size() == kDigestSize);
  memcpy(digest, message->digest()->data(), kDigestSize);
  return Status::OK();
}

Status SendSealReply(int sock, ObjectID object_id, PlasmaError error) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message =
      fb::CreatePlasmaSealReply(fbb, fbb.CreateString(object_id.binary()), error);
  return PlasmaSend(sock, MessageType::PlasmaSealReply, &fbb, message);
}

Status ReadSealReply(uint8_t* data, size_t size, ObjectID* object_id) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaSealReply>(data);
  DCHECK(VerifyFlatbuffer(message, data, size));
  *object_id = ObjectID::from_binary(message->object_id()->str());
  return PlasmaErrorStatus(message->error());
}

// Release messages.

Status SendReleaseRequest(int sock, ObjectID object_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message =
      fb::CreatePlasmaReleaseRequest(fbb, fbb.CreateString(object_id.binary()));
  return PlasmaSend(sock, MessageType::PlasmaReleaseRequest, &fbb, message);
}

Status ReadReleaseRequest(uint8_t* data, size_t size, ObjectID* object_id) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaReleaseRequest>(data);
  DCHECK(VerifyFlatbuffer(message, data, size));
  *object_id = ObjectID::from_binary(message->object_id()->str());
  return Status::OK();
}

Status SendReleaseReply(int sock, ObjectID object_id, PlasmaError error) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message =
      fb::CreatePlasmaReleaseReply(fbb, fbb.CreateString(object_id.binary()), error);
  return PlasmaSend(sock, MessageType::PlasmaReleaseReply, &fbb, message);
}

Status ReadReleaseReply(uint8_t* data, size_t size, ObjectID* object_id) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaReleaseReply>(data);
  DCHECK(VerifyFlatbuffer(message, data, size));
  *object_id = ObjectID::from_binary(message->object_id()->str());
  return PlasmaErrorStatus(message->error());
}

// Delete objects messages.

Status SendDeleteRequest(int sock, const std::vector<ObjectID>& object_ids) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaDeleteRequest(
      fbb, static_cast<int32_t>(object_ids.size()),
      ToFlatbuffer(&fbb, &object_ids[0], object_ids.size()));
  return PlasmaSend(sock, MessageType::PlasmaDeleteRequest, &fbb, message);
}

Status ReadDeleteRequest(uint8_t* data, size_t size, std::vector<ObjectID>* object_ids) {
  using fb::PlasmaDeleteRequest;

  DCHECK(data);
  DCHECK(object_ids);
  auto message = flatbuffers::GetRoot<PlasmaDeleteRequest>(data);
  DCHECK(VerifyFlatbuffer(message, data, size));
  ToVector(*message, object_ids, [](const PlasmaDeleteRequest& request, int i) {
    return ObjectID::from_binary(request.object_ids()->Get(i)->str());
  });
  return Status::OK();
}

Status SendDeleteReply(int sock, const std::vector<ObjectID>& object_ids,
                       const std::vector<PlasmaError>& errors) {
  DCHECK(object_ids.size() == errors.size());
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaDeleteReply(
      fbb, static_cast<int32_t>(object_ids.size()),
      ToFlatbuffer(&fbb, &object_ids[0], object_ids.size()),
      fbb.CreateVector(reinterpret_cast<const int32_t*>(&errors[0]), object_ids.size()));
  return PlasmaSend(sock, MessageType::PlasmaDeleteReply, &fbb, message);
}

Status ReadDeleteReply(uint8_t* data, size_t size, std::vector<ObjectID>* object_ids,
                       std::vector<PlasmaError>* errors) {
  using fb::PlasmaDeleteReply;

  DCHECK(data);
  DCHECK(object_ids);
  DCHECK(errors);
  auto message = flatbuffers::GetRoot<PlasmaDeleteReply>(data);
  DCHECK(VerifyFlatbuffer(message, data, size));
  ToVector(*message, object_ids, [](const PlasmaDeleteReply& request, int i) {
    return ObjectID::from_binary(request.object_ids()->Get(i)->str());
  });
  ToVector(*message, errors, [](const PlasmaDeleteReply& request, int i) {
    return static_cast<PlasmaError>(request.errors()->data()[i]);
  });
  return Status::OK();
}

// Contains messages.

Status SendContainsRequest(int sock, ObjectID object_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message =
      fb::CreatePlasmaContainsRequest(fbb, fbb.CreateString(object_id.binary()));
  return PlasmaSend(sock, MessageType::PlasmaContainsRequest, &fbb, message);
}

Status ReadContainsRequest(uint8_t* data, size_t size, ObjectID* object_id) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaContainsRequest>(data);
  DCHECK(VerifyFlatbuffer(message, data, size));
  *object_id = ObjectID::from_binary(message->object_id()->str());
  return Status::OK();
}

Status SendContainsReply(int sock, ObjectID object_id, bool has_object) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaContainsReply(fbb, fbb.CreateString(object_id.binary()),
                                               has_object);
  return PlasmaSend(sock, MessageType::PlasmaContainsReply, &fbb, message);
}

Status ReadContainsReply(uint8_t* data, size_t size, ObjectID* object_id,
                         bool* has_object) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaContainsReply>(data);
  DCHECK(VerifyFlatbuffer(message, data, size));
  *object_id = ObjectID::from_binary(message->object_id()->str());
  *has_object = message->has_object();
  return Status::OK();
}

// List messages.

Status SendListRequest(int sock) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaListRequest(fbb);
  return PlasmaSend(sock, MessageType::PlasmaListRequest, &fbb, message);
}

Status ReadListRequest(uint8_t* data, size_t size) { return Status::OK(); }

Status SendListReply(int sock, const ObjectTable& objects) {
  flatbuffers::FlatBufferBuilder fbb;
  std::vector<flatbuffers::Offset<fb::ObjectInfo>> object_infos;
  for (auto const& entry : objects) {
    auto digest = entry.second->state == ObjectState::PLASMA_CREATED
                      ? fbb.CreateString("")
                      : fbb.CreateString(reinterpret_cast<char*>(entry.second->digest),
                                         kDigestSize);
    auto info = fb::CreateObjectInfo(fbb, fbb.CreateString(entry.first.binary()),
                                     entry.second->data_size, entry.second->metadata_size,
                                     entry.second->ref_count, entry.second->create_time,
                                     entry.second->construct_duration, digest);
    object_infos.push_back(info);
  }
  auto message = fb::CreatePlasmaListReply(fbb, fbb.CreateVector(object_infos));
  return PlasmaSend(sock, MessageType::PlasmaListReply, &fbb, message);
}

Status ReadListReply(uint8_t* data, size_t size, ObjectTable* objects) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaListReply>(data);
  DCHECK(VerifyFlatbuffer(message, data, size));
  for (auto const& object : *message->objects()) {
    ObjectID object_id = ObjectID::from_binary(object->object_id()->str());
    auto entry = std::unique_ptr<ObjectTableEntry>(new ObjectTableEntry());
    entry->data_size = object->data_size();
    entry->metadata_size = object->metadata_size();
    entry->ref_count = object->ref_count();
    entry->create_time = object->create_time();
    entry->construct_duration = object->construct_duration();
    entry->state = object->digest()->size() == 0 ? ObjectState::PLASMA_CREATED
                                                 : ObjectState::PLASMA_SEALED;
    (*objects)[object_id] = std::move(entry);
  }
  return Status::OK();
}

// Connect messages.

Status SendConnectRequest(int sock) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaConnectRequest(fbb);
  return PlasmaSend(sock, MessageType::PlasmaConnectRequest, &fbb, message);
}

Status ReadConnectRequest(uint8_t* data) { return Status::OK(); }

Status SendConnectReply(int sock, int64_t memory_capacity) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaConnectReply(fbb, memory_capacity);
  return PlasmaSend(sock, MessageType::PlasmaConnectReply, &fbb, message);
}

Status ReadConnectReply(uint8_t* data, size_t size, int64_t* memory_capacity) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaConnectReply>(data);
  DCHECK(VerifyFlatbuffer(message, data, size));
  *memory_capacity = message->memory_capacity();
  return Status::OK();
}

// Evict messages.

Status SendEvictRequest(int sock, int64_t num_bytes) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaEvictRequest(fbb, num_bytes);
  return PlasmaSend(sock, MessageType::PlasmaEvictRequest, &fbb, message);
}

Status ReadEvictRequest(uint8_t* data, size_t size, int64_t* num_bytes) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaEvictRequest>(data);
  DCHECK(VerifyFlatbuffer(message, data, size));
  *num_bytes = message->num_bytes();
  return Status::OK();
}

Status SendEvictReply(int sock, int64_t num_bytes) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaEvictReply(fbb, num_bytes);
  return PlasmaSend(sock, MessageType::PlasmaEvictReply, &fbb, message);
}

Status ReadEvictReply(uint8_t* data, size_t size, int64_t& num_bytes) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaEvictReply>(data);
  DCHECK(VerifyFlatbuffer(message, data, size));
  num_bytes = message->num_bytes();
  return Status::OK();
}

// Get messages.

Status SendGetRequest(int sock, const ObjectID* object_ids, int64_t num_objects,
                      int64_t timeout_ms) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaGetRequest(
      fbb, ToFlatbuffer(&fbb, object_ids, num_objects), timeout_ms);
  return PlasmaSend(sock, MessageType::PlasmaGetRequest, &fbb, message);
}

Status ReadGetRequest(uint8_t* data, size_t size, std::vector<ObjectID>& object_ids,
                      int64_t* timeout_ms) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaGetRequest>(data);
  DCHECK(VerifyFlatbuffer(message, data, size));
  for (uoffset_t i = 0; i < message->object_ids()->size(); ++i) {
    auto object_id = message->object_ids()->Get(i)->str();
    object_ids.push_back(ObjectID::from_binary(object_id));
  }
  *timeout_ms = message->timeout_ms();
  return Status::OK();
}

Status SendGetReply(int sock, ObjectID object_ids[],
                    std::unordered_map<ObjectID, PlasmaObject>& plasma_objects,
                    int64_t num_objects, const std::vector<int>& store_fds,
                    const std::vector<int64_t>& mmap_sizes) {
  flatbuffers::FlatBufferBuilder fbb;
  std::vector<PlasmaObjectSpec> objects;

  std::vector<flatbuffers::Offset<fb::CudaHandle>> handles;
  for (int64_t i = 0; i < num_objects; ++i) {
    const PlasmaObject& object = plasma_objects[object_ids[i]];
    objects.push_back(PlasmaObjectSpec(object.store_fd, object.data_offset,
                                       object.data_size, object.metadata_offset,
                                       object.metadata_size, object.device_num));
#ifdef PLASMA_CUDA
    if (object.device_num != 0) {
      std::shared_ptr<arrow::Buffer> handle;
      RETURN_NOT_OK(object.ipc_handle->Serialize(arrow::default_memory_pool(), &handle));
      handles.push_back(
          fb::CreateCudaHandle(fbb, fbb.CreateVector(handle->data(), handle->size())));
    }
#endif
  }
  auto message = fb::CreatePlasmaGetReply(
      fbb, ToFlatbuffer(&fbb, object_ids, num_objects),
      fbb.CreateVectorOfStructs(objects.data(), num_objects), fbb.CreateVector(store_fds),
      fbb.CreateVector(mmap_sizes), fbb.CreateVector(handles));
  return PlasmaSend(sock, MessageType::PlasmaGetReply, &fbb, message);
}

Status ReadGetReply(uint8_t* data, size_t size, ObjectID object_ids[],
                    PlasmaObject plasma_objects[], int64_t num_objects,
                    std::vector<int>& store_fds, std::vector<int64_t>& mmap_sizes) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaGetReply>(data);
#ifdef PLASMA_CUDA
  int handle_pos = 0;
#endif
  DCHECK(VerifyFlatbuffer(message, data, size));
  for (uoffset_t i = 0; i < num_objects; ++i) {
    object_ids[i] = ObjectID::from_binary(message->object_ids()->Get(i)->str());
  }
  for (uoffset_t i = 0; i < num_objects; ++i) {
    const PlasmaObjectSpec* object = message->plasma_objects()->Get(i);
    plasma_objects[i].store_fd = object->segment_index();
    plasma_objects[i].data_offset = object->data_offset();
    plasma_objects[i].data_size = object->data_size();
    plasma_objects[i].metadata_offset = object->metadata_offset();
    plasma_objects[i].metadata_size = object->metadata_size();
    plasma_objects[i].device_num = object->device_num();
#ifdef PLASMA_CUDA
    if (object->device_num() != 0) {
      const void* ipc_handle = message->handles()->Get(handle_pos)->handle()->data();
      RETURN_NOT_OK(
          CudaIpcMemHandle::FromBuffer(ipc_handle, &plasma_objects[i].ipc_handle));
      handle_pos++;
    }
#endif
  }
  ARROW_CHECK(message->store_fds()->size() == message->mmap_sizes()->size());
  for (uoffset_t i = 0; i < message->store_fds()->size(); i++) {
    store_fds.push_back(message->store_fds()->Get(i));
    mmap_sizes.push_back(message->mmap_sizes()->Get(i));
  }
  return Status::OK();
}

// Subscribe messages.

Status SendSubscribeRequest(int sock) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaSubscribeRequest(fbb);
  return PlasmaSend(sock, MessageType::PlasmaSubscribeRequest, &fbb, message);
}

// Data messages.

Status SendDataRequest(int sock, ObjectID object_id, const char* address, int port) {
  flatbuffers::FlatBufferBuilder fbb;
  auto addr = fbb.CreateString(address, strlen(address));
  auto message =
      fb::CreatePlasmaDataRequest(fbb, fbb.CreateString(object_id.binary()), addr, port);
  return PlasmaSend(sock, MessageType::PlasmaDataRequest, &fbb, message);
}

Status ReadDataRequest(uint8_t* data, size_t size, ObjectID* object_id, char** address,
                       int* port) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaDataRequest>(data);
  DCHECK(VerifyFlatbuffer(message, data, size));
  DCHECK(message->object_id()->size() == sizeof(ObjectID));
  *object_id = ObjectID::from_binary(message->object_id()->str());
  *address = strdup(message->address()->c_str());
  *port = message->port();
  return Status::OK();
}

Status SendDataReply(int sock, ObjectID object_id, int64_t object_size,
                     int64_t metadata_size) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaDataReply(fbb, fbb.CreateString(object_id.binary()),
                                           object_size, metadata_size);
  return PlasmaSend(sock, MessageType::PlasmaDataReply, &fbb, message);
}

Status ReadDataReply(uint8_t* data, size_t size, ObjectID* object_id,
                     int64_t* object_size, int64_t* metadata_size) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaDataReply>(data);
  DCHECK(VerifyFlatbuffer(message, data, size));
  *object_id = ObjectID::from_binary(message->object_id()->str());
  *object_size = static_cast<int64_t>(message->object_size());
  *metadata_size = static_cast<int64_t>(message->metadata_size());
  return Status::OK();
}

}  // namespace plasma
