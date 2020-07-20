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

#include "ray/object_manager/plasma/protocol.h"

#include <utility>

#include "flatbuffers/flatbuffers.h"

#include "ray/object_manager/plasma/common.h"
#include "ray/object_manager/plasma/connection.h"
#include "ray/object_manager/plasma/plasma_generated.h"

#ifdef PLASMA_CUDA
#include "arrow/gpu/cuda_api.h"
#endif
#include "arrow/util/ubsan.h"

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
    results.push_back(fbb->CreateString(object_ids[i].Binary()));
  }
  return fbb->CreateVector(arrow::util::MakeNonNull(results.data()), results.size());
}

flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>>
ToFlatbuffer(flatbuffers::FlatBufferBuilder* fbb,
             const std::vector<std::string>& strings) {
  std::vector<flatbuffers::Offset<flatbuffers::String>> results;
  for (size_t i = 0; i < strings.size(); i++) {
    results.push_back(fbb->CreateString(strings[i]));
  }

  return fbb->CreateVector(arrow::util::MakeNonNull(results.data()), results.size());
}

flatbuffers::Offset<flatbuffers::Vector<int64_t>> ToFlatbuffer(
    flatbuffers::FlatBufferBuilder* fbb, const std::vector<int64_t>& data) {
  return fbb->CreateVector(arrow::util::MakeNonNull(data.data()), data.size());
}

Status PlasmaReceive(const std::shared_ptr<StoreConn> &store_conn, MessageType message_type, std::vector<uint8_t>* buffer) {
  if (!store_conn) {
    return Status::IOError("Connection is closed.");
  }
  return store_conn->ReadMessage(static_cast<int64_t>(message_type), buffer);
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

template <typename T, typename FlatbufferVectorPointer, typename Converter>
void ConvertToVector(const FlatbufferVectorPointer fbvector, std::vector<T>* out,
                     const Converter& converter) {
  out->clear();
  out->reserve(fbvector->size());
  for (size_t i = 0; i < fbvector->size(); ++i) {
    out->push_back(converter(*fbvector->Get(i)));
  }
}

template <typename Message>
Status PlasmaSend(const std::shared_ptr<StoreConn> &store_conn, MessageType message_type, flatbuffers::FlatBufferBuilder* fbb,
                  const Message& message) {
  if (!store_conn) {
    return Status::IOError("Connection is closed.");
  }
  fbb->Finish(message);
  return store_conn->WriteMessage(static_cast<int64_t>(message_type), fbb->GetSize(), fbb->GetBufferPointer());
}

template <typename Message>
Status PlasmaSend(const std::shared_ptr<Client> &client, MessageType message_type, flatbuffers::FlatBufferBuilder* fbb,
                  const Message& message) {
  if (!client) {
    return Status::IOError("Connection is closed.");
  }
  fbb->Finish(message);
  return client->WriteMessage(static_cast<int64_t>(message_type), fbb->GetSize(), fbb->GetBufferPointer());
}

Status PlasmaErrorStatus(fb::PlasmaError plasma_error) {
  switch (plasma_error) {
    case fb::PlasmaError::OK:
      return Status::OK();
    case fb::PlasmaError::ObjectExists:
      return Status::ObjectExists("object already exists in the plasma store");
    case fb::PlasmaError::ObjectNonexistent:
      return Status::ObjectNotFound("object does not exist in the plasma store");
    case fb::PlasmaError::OutOfMemory:
      return Status::ObjectStoreFull("object does not fit in the plasma store");
    default:
      RAY_LOG(FATAL) << "unknown plasma error code " << static_cast<int>(plasma_error);
  }
  return Status::OK();
}

// Set options messages.

Status SendSetOptionsRequest(const std::shared_ptr<StoreConn> &store_conn, const std::string& client_name,
                             int64_t output_memory_limit) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaSetOptionsRequest(fbb, fbb.CreateString(client_name),
                                                   output_memory_limit);
  return PlasmaSend(store_conn, MessageType::PlasmaSetOptionsRequest, &fbb, message);
}

Status ReadSetOptionsRequest(uint8_t* data, size_t size, std::string* client_name,
                             int64_t* output_memory_quota) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaSetOptionsRequest>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  *client_name = std::string(message->client_name()->str());
  *output_memory_quota = message->output_memory_quota();
  return Status::OK();
}

Status SendSetOptionsReply(const std::shared_ptr<Client> &client, PlasmaError error) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaSetOptionsReply(fbb, error);
  return PlasmaSend(client, MessageType::PlasmaSetOptionsReply, &fbb, message);
}

Status ReadSetOptionsReply(uint8_t* data, size_t size) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaSetOptionsReply>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  return PlasmaErrorStatus(message->error());
}

// Get debug string messages.

Status SendGetDebugStringRequest(const std::shared_ptr<StoreConn> &store_conn) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaGetDebugStringRequest(fbb);
  return PlasmaSend(store_conn, MessageType::PlasmaGetDebugStringRequest, &fbb, message);
}

Status SendGetDebugStringReply(const std::shared_ptr<Client> &client, const std::string& debug_string) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaGetDebugStringReply(fbb, fbb.CreateString(debug_string));
  return PlasmaSend(client, MessageType::PlasmaGetDebugStringReply, &fbb, message);
}

Status ReadGetDebugStringReply(uint8_t* data, size_t size, std::string* debug_string) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaGetDebugStringReply>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  *debug_string = message->debug_string()->str();
  return Status::OK();
}

// Create messages.

Status SendCreateRequest(const std::shared_ptr<StoreConn> &store_conn, ObjectID object_id,
                         const ray::rpc::Address& owner_address, bool evict_if_full,
                         int64_t data_size, int64_t metadata_size, int device_num) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message =
      fb::CreatePlasmaCreateRequest(fbb, fbb.CreateString(object_id.Binary()),
                                    fbb.CreateString(owner_address.raylet_id()),
                                    fbb.CreateString(owner_address.ip_address()),
                                    owner_address.port(),
                                    fbb.CreateString(owner_address.worker_id()),
                                    evict_if_full, data_size, metadata_size, device_num);
  return PlasmaSend(store_conn, MessageType::PlasmaCreateRequest, &fbb, message);
}

Status ReadCreateRequest(uint8_t* data, size_t size, ObjectID* object_id,
                         ClientID* owner_raylet_id, std::string* owner_ip_address,
                         int* owner_port, WorkerID* owner_worker_id, bool* evict_if_full,
                         int64_t* data_size, int64_t* metadata_size,
                         int* device_num) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaCreateRequest>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  *evict_if_full = message->evict_if_full();
  *data_size = message->data_size();
  *metadata_size = message->metadata_size();
  *object_id = ObjectID::FromBinary(message->object_id()->str());
  *owner_raylet_id = ClientID::FromBinary(message->owner_raylet_id()->str());
  *owner_ip_address = message->owner_ip_address()->str();
  *owner_port = message->owner_port();
  *owner_worker_id = WorkerID::FromBinary(message->owner_worker_id()->str());
  *device_num = message->device_num();
  return Status::OK();
}

Status SendCreateReply(const std::shared_ptr<Client> &client, ObjectID object_id, PlasmaObject* object,
                       PlasmaError error_code, int64_t mmap_size) {
  flatbuffers::FlatBufferBuilder fbb;
  PlasmaObjectSpec plasma_object(FD2INT(object->store_fd), object->data_offset, object->data_size,
                                 object->metadata_offset, object->metadata_size,
                                 object->device_num);
  auto object_string = fbb.CreateString(object_id.Binary());
#ifdef PLASMA_CUDA
  flatbuffers::Offset<fb::CudaHandle> ipc_handle;
  if (object->device_num != 0) {
    std::shared_ptr<arrow::Buffer> handle;
    ARROW_ASSIGN_OR_RAISE(handle, object->ipc_handle->Serialize());
    ipc_handle =
        fb::CreateCudaHandle(fbb, fbb.CreateVector(handle->data(), handle->size()));
  }
#endif
  fb::PlasmaCreateReplyBuilder crb(fbb);
  crb.add_error(static_cast<PlasmaError>(error_code));
  crb.add_plasma_object(&plasma_object);
  crb.add_object_id(object_string);
  crb.add_store_fd(FD2INT(object->store_fd));
  crb.add_mmap_size(mmap_size);
  if (object->device_num != 0) {
#ifdef PLASMA_CUDA
    crb.add_ipc_handle(ipc_handle);
#else
    RAY_LOG(FATAL) << "This should be unreachable.";
#endif
  }
  auto message = crb.Finish();
  return PlasmaSend(client, MessageType::PlasmaCreateReply, &fbb, message);
}

Status ReadCreateReply(uint8_t* data, size_t size, ObjectID* object_id,
                       PlasmaObject* object, MEMFD_TYPE* store_fd, int64_t* mmap_size) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaCreateReply>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  *object_id = ObjectID::FromBinary(message->object_id()->str());
  object->store_fd = INT2FD(message->plasma_object()->segment_index());
  object->data_offset = message->plasma_object()->data_offset();
  object->data_size = message->plasma_object()->data_size();
  object->metadata_offset = message->plasma_object()->metadata_offset();
  object->metadata_size = message->plasma_object()->metadata_size();

  *store_fd = INT2FD(message->store_fd());
  *mmap_size = message->mmap_size();

  object->device_num = message->plasma_object()->device_num();
#ifdef PLASMA_CUDA
  if (object->device_num != 0) {
    ARROW_ASSIGN_OR_RAISE(
        object->ipc_handle,
        CudaIpcMemHandle::FromBuffer(message->ipc_handle()->handle()->data()));
  }
#endif
  return PlasmaErrorStatus(message->error());
}

Status SendAbortRequest(const std::shared_ptr<StoreConn> &store_conn, ObjectID object_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaAbortRequest(fbb, fbb.CreateString(object_id.Binary()));
  return PlasmaSend(store_conn, MessageType::PlasmaAbortRequest, &fbb, message);
}

Status ReadAbortRequest(uint8_t* data, size_t size, ObjectID* object_id) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaAbortRequest>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  *object_id = ObjectID::FromBinary(message->object_id()->str());
  return Status::OK();
}

Status SendAbortReply(const std::shared_ptr<Client> &client, ObjectID object_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaAbortReply(fbb, fbb.CreateString(object_id.Binary()));
  return PlasmaSend(client, MessageType::PlasmaAbortReply, &fbb, message);
}

Status ReadAbortReply(uint8_t* data, size_t size, ObjectID* object_id) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaAbortReply>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  *object_id = ObjectID::FromBinary(message->object_id()->str());
  return Status::OK();
}

// Seal messages.

Status SendSealRequest(const std::shared_ptr<StoreConn> &store_conn, ObjectID object_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaSealRequest(fbb, fbb.CreateString(object_id.Binary()));
  return PlasmaSend(store_conn, MessageType::PlasmaSealRequest, &fbb, message);
}

Status ReadSealRequest(uint8_t* data, size_t size, ObjectID* object_id) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaSealRequest>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  *object_id = ObjectID::FromBinary(message->object_id()->str());
  return Status::OK();
}

Status SendSealReply(const std::shared_ptr<Client> &client, ObjectID object_id, PlasmaError error) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message =
      fb::CreatePlasmaSealReply(fbb, fbb.CreateString(object_id.Binary()), error);
  return PlasmaSend(client, MessageType::PlasmaSealReply, &fbb, message);
}

Status ReadSealReply(uint8_t* data, size_t size, ObjectID* object_id) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaSealReply>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  *object_id = ObjectID::FromBinary(message->object_id()->str());
  return PlasmaErrorStatus(message->error());
}

// Release messages.

Status SendReleaseRequest(const std::shared_ptr<StoreConn> &store_conn, ObjectID object_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message =
      fb::CreatePlasmaReleaseRequest(fbb, fbb.CreateString(object_id.Binary()));
  return PlasmaSend(store_conn, MessageType::PlasmaReleaseRequest, &fbb, message);
}

Status ReadReleaseRequest(uint8_t* data, size_t size, ObjectID* object_id) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaReleaseRequest>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  *object_id = ObjectID::FromBinary(message->object_id()->str());
  return Status::OK();
}

Status SendReleaseReply(const std::shared_ptr<Client> &client, ObjectID object_id, PlasmaError error) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message =
      fb::CreatePlasmaReleaseReply(fbb, fbb.CreateString(object_id.Binary()), error);
  return PlasmaSend(client, MessageType::PlasmaReleaseReply, &fbb, message);
}

Status ReadReleaseReply(uint8_t* data, size_t size, ObjectID* object_id) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaReleaseReply>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  *object_id = ObjectID::FromBinary(message->object_id()->str());
  return PlasmaErrorStatus(message->error());
}

// Delete objects messages.

Status SendDeleteRequest(const std::shared_ptr<StoreConn> &store_conn, const std::vector<ObjectID>& object_ids) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaDeleteRequest(
      fbb, static_cast<int32_t>(object_ids.size()),
      ToFlatbuffer(&fbb, &object_ids[0], object_ids.size()));
  return PlasmaSend(store_conn, MessageType::PlasmaDeleteRequest, &fbb, message);
}

Status ReadDeleteRequest(uint8_t* data, size_t size, std::vector<ObjectID>* object_ids) {
  using fb::PlasmaDeleteRequest;

  RAY_DCHECK(data);
  RAY_DCHECK(object_ids);
  auto message = flatbuffers::GetRoot<PlasmaDeleteRequest>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  ToVector(*message, object_ids, [](const PlasmaDeleteRequest& request, int i) {
    return ObjectID::FromBinary(request.object_ids()->Get(i)->str());
  });
  return Status::OK();
}

Status SendDeleteReply(const std::shared_ptr<Client> &client, const std::vector<ObjectID>& object_ids,
                       const std::vector<PlasmaError>& errors) {
  RAY_DCHECK(object_ids.size() == errors.size());
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaDeleteReply(
      fbb, static_cast<int32_t>(object_ids.size()),
      ToFlatbuffer(&fbb, &object_ids[0], object_ids.size()),
      fbb.CreateVector(
          arrow::util::MakeNonNull(reinterpret_cast<const int32_t*>(errors.data())),
          object_ids.size()));
  return PlasmaSend(client, MessageType::PlasmaDeleteReply, &fbb, message);
}

Status ReadDeleteReply(uint8_t* data, size_t size, std::vector<ObjectID>* object_ids,
                       std::vector<PlasmaError>* errors) {
  using fb::PlasmaDeleteReply;

  RAY_DCHECK(data);
  RAY_DCHECK(object_ids);
  RAY_DCHECK(errors);
  auto message = flatbuffers::GetRoot<PlasmaDeleteReply>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  ToVector(*message, object_ids, [](const PlasmaDeleteReply& request, int i) {
    return ObjectID::FromBinary(request.object_ids()->Get(i)->str());
  });
  ToVector(*message, errors, [](const PlasmaDeleteReply& request, int i) {
    return static_cast<PlasmaError>(request.errors()->data()[i]);
  });
  return Status::OK();
}

// Contains messages.

Status SendContainsRequest(const std::shared_ptr<StoreConn> &store_conn, ObjectID object_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message =
      fb::CreatePlasmaContainsRequest(fbb, fbb.CreateString(object_id.Binary()));
  return PlasmaSend(store_conn, MessageType::PlasmaContainsRequest, &fbb, message);
}

Status ReadContainsRequest(uint8_t* data, size_t size, ObjectID* object_id) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaContainsRequest>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  *object_id = ObjectID::FromBinary(message->object_id()->str());
  return Status::OK();
}

Status SendContainsReply(const std::shared_ptr<Client> &client, ObjectID object_id, bool has_object) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaContainsReply(fbb, fbb.CreateString(object_id.Binary()),
                                               has_object);
  return PlasmaSend(client, MessageType::PlasmaContainsReply, &fbb, message);
}

Status ReadContainsReply(uint8_t* data, size_t size, ObjectID* object_id,
                         bool* has_object) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaContainsReply>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  *object_id = ObjectID::FromBinary(message->object_id()->str());
  *has_object = message->has_object();
  return Status::OK();
}

// Connect messages.

Status SendConnectRequest(const std::shared_ptr<StoreConn> &store_conn) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaConnectRequest(fbb);
  return PlasmaSend(store_conn, MessageType::PlasmaConnectRequest, &fbb, message);
}

Status ReadConnectRequest(uint8_t* data) { return Status::OK(); }

Status SendConnectReply(const std::shared_ptr<Client> &client, int64_t memory_capacity) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaConnectReply(fbb, memory_capacity);
  return PlasmaSend(client, MessageType::PlasmaConnectReply, &fbb, message);
}

Status ReadConnectReply(uint8_t* data, size_t size, int64_t* memory_capacity) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaConnectReply>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  *memory_capacity = message->memory_capacity();
  return Status::OK();
}

// Evict messages.

Status SendEvictRequest(const std::shared_ptr<StoreConn> &store_conn, int64_t num_bytes) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaEvictRequest(fbb, num_bytes);
  return PlasmaSend(store_conn, MessageType::PlasmaEvictRequest, &fbb, message);
}

Status ReadEvictRequest(uint8_t* data, size_t size, int64_t* num_bytes) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaEvictRequest>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  *num_bytes = message->num_bytes();
  return Status::OK();
}

Status SendEvictReply(const std::shared_ptr<Client> &client, int64_t num_bytes) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaEvictReply(fbb, num_bytes);
  return PlasmaSend(client, MessageType::PlasmaEvictReply, &fbb, message);
}

Status ReadEvictReply(uint8_t* data, size_t size, int64_t& num_bytes) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaEvictReply>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  num_bytes = message->num_bytes();
  return Status::OK();
}

// Get messages.

Status SendGetRequest(const std::shared_ptr<StoreConn> &store_conn, const ObjectID* object_ids, int64_t num_objects,
                      int64_t timeout_ms) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaGetRequest(
      fbb, ToFlatbuffer(&fbb, object_ids, num_objects), timeout_ms);
  return PlasmaSend(store_conn, MessageType::PlasmaGetRequest, &fbb, message);
}

Status ReadGetRequest(uint8_t* data, size_t size, std::vector<ObjectID>& object_ids,
                      int64_t* timeout_ms) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaGetRequest>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  for (uoffset_t i = 0; i < message->object_ids()->size(); ++i) {
    auto object_id = message->object_ids()->Get(i)->str();
    object_ids.push_back(ObjectID::FromBinary(object_id));
  }
  *timeout_ms = message->timeout_ms();
  return Status::OK();
}

Status SendGetReply(const std::shared_ptr<Client> &client, ObjectID object_ids[],
                    std::unordered_map<ObjectID, PlasmaObject>& plasma_objects,
                    int64_t num_objects, const std::vector<MEMFD_TYPE>& store_fds,
                    const std::vector<int64_t>& mmap_sizes) {
  flatbuffers::FlatBufferBuilder fbb;
  std::vector<PlasmaObjectSpec> objects;

  std::vector<flatbuffers::Offset<fb::CudaHandle>> handles;
  for (int64_t i = 0; i < num_objects; ++i) {
    const PlasmaObject& object = plasma_objects[object_ids[i]];
    objects.push_back(PlasmaObjectSpec(FD2INT(object.store_fd), object.data_offset,
                                       object.data_size, object.metadata_offset,
                                       object.metadata_size, object.device_num));
#ifdef PLASMA_CUDA
    if (object.device_num != 0) {
      std::shared_ptr<arrow::Buffer> handle;
      ARROW_ASSIGN_OR_RAISE(handle, object.ipc_handle->Serialize());
      handles.push_back(
          fb::CreateCudaHandle(fbb, fbb.CreateVector(handle->data(), handle->size())));
    }
#endif
  }
  std::vector<int> store_fds_as_int;
  for (MEMFD_TYPE store_fd : store_fds) {
    store_fds_as_int.push_back(FD2INT(store_fd));
  }
  auto message = fb::CreatePlasmaGetReply(
      fbb, ToFlatbuffer(&fbb, object_ids, num_objects),
      fbb.CreateVectorOfStructs(arrow::util::MakeNonNull(objects.data()), num_objects),
      fbb.CreateVector(arrow::util::MakeNonNull(store_fds_as_int.data()), store_fds_as_int.size()),
      fbb.CreateVector(arrow::util::MakeNonNull(mmap_sizes.data()), mmap_sizes.size()),
      fbb.CreateVector(arrow::util::MakeNonNull(handles.data()), handles.size()));
  return PlasmaSend(client, MessageType::PlasmaGetReply, &fbb, message);
}

Status ReadGetReply(uint8_t* data, size_t size, ObjectID object_ids[],
                    PlasmaObject plasma_objects[], int64_t num_objects,
                    std::vector<MEMFD_TYPE>& store_fds, std::vector<int64_t>& mmap_sizes) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaGetReply>(data);
#ifdef PLASMA_CUDA
  int handle_pos = 0;
#endif
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  for (uoffset_t i = 0; i < num_objects; ++i) {
    object_ids[i] = ObjectID::FromBinary(message->object_ids()->Get(i)->str());
  }
  for (uoffset_t i = 0; i < num_objects; ++i) {
    const PlasmaObjectSpec* object = message->plasma_objects()->Get(i);
    plasma_objects[i].store_fd = INT2FD(object->segment_index());
    plasma_objects[i].data_offset = object->data_offset();
    plasma_objects[i].data_size = object->data_size();
    plasma_objects[i].metadata_offset = object->metadata_offset();
    plasma_objects[i].metadata_size = object->metadata_size();
    plasma_objects[i].device_num = object->device_num();
#ifdef PLASMA_CUDA
    if (object->device_num() != 0) {
      const void* ipc_handle = message->handles()->Get(handle_pos)->handle()->data();
      ARROW_ASSIGN_OR_RAISE(plasma_objects[i].ipc_handle,
                            CudaIpcMemHandle::FromBuffer(ipc_handle));
      handle_pos++;
    }
#endif
  }
  RAY_CHECK(message->store_fds()->size() == message->mmap_sizes()->size());
  for (uoffset_t i = 0; i < message->store_fds()->size(); i++) {
    store_fds.push_back(INT2FD(message->store_fds()->Get(i)));
    mmap_sizes.push_back(message->mmap_sizes()->Get(i));
  }
  return Status::OK();
}

// Data messages.

Status SendDataRequest(const std::shared_ptr<StoreConn> &store_conn, ObjectID object_id, const char* address, int port) {
  flatbuffers::FlatBufferBuilder fbb;
  auto addr = fbb.CreateString(address, strlen(address));
  auto message =
      fb::CreatePlasmaDataRequest(fbb, fbb.CreateString(object_id.Binary()), addr, port);
  return PlasmaSend(store_conn, MessageType::PlasmaDataRequest, &fbb, message);
}

Status ReadDataRequest(uint8_t* data, size_t size, ObjectID* object_id, char** address,
                       int* port) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaDataRequest>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  RAY_DCHECK(message->object_id()->size() == sizeof(ObjectID));
  *object_id = ObjectID::FromBinary(message->object_id()->str());
#ifdef _WIN32
  *address = _strdup(message->address()->c_str());
#else
  *address = strdup(message->address()->c_str());
#endif
  *port = message->port();
  return Status::OK();
}

Status SendDataReply(const std::shared_ptr<Client> &client, ObjectID object_id, int64_t object_size,
                     int64_t metadata_size) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaDataReply(fbb, fbb.CreateString(object_id.Binary()),
                                           object_size, metadata_size);
  return PlasmaSend(client, MessageType::PlasmaDataReply, &fbb, message);
}

Status ReadDataReply(uint8_t* data, size_t size, ObjectID* object_id,
                     int64_t* object_size, int64_t* metadata_size) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaDataReply>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  *object_id = ObjectID::FromBinary(message->object_id()->str());
  *object_size = static_cast<int64_t>(message->object_size());
  *metadata_size = static_cast<int64_t>(message->metadata_size());
  return Status::OK();
}

// RefreshLRU messages.

Status SendRefreshLRURequest(const std::shared_ptr<StoreConn> &store_conn, const std::vector<ObjectID>& object_ids) {
  flatbuffers::FlatBufferBuilder fbb;

  auto message = fb::CreatePlasmaRefreshLRURequest(
      fbb, ToFlatbuffer(&fbb, object_ids.data(), object_ids.size()));

  return PlasmaSend(store_conn, MessageType::PlasmaRefreshLRURequest, &fbb, message);
}

Status ReadRefreshLRURequest(uint8_t* data, size_t size,
                             std::vector<ObjectID>* object_ids) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaRefreshLRURequest>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  for (uoffset_t i = 0; i < message->object_ids()->size(); ++i) {
    auto object_id = message->object_ids()->Get(i)->str();
    object_ids->push_back(ObjectID::FromBinary(object_id));
  }
  return Status::OK();
}

Status SendRefreshLRUReply(const std::shared_ptr<Client> &client) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaRefreshLRUReply(fbb);
  return PlasmaSend(client, MessageType::PlasmaRefreshLRUReply, &fbb, message);
}

Status ReadRefreshLRUReply(uint8_t* data, size_t size) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaRefreshLRUReply>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  return Status::OK();
}

}  // namespace plasma
