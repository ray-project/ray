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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "flatbuffers/flatbuffers.h"
#include "ray/object_manager/plasma/common.h"
#include "ray/object_manager/plasma/connection.h"
#include "ray/object_manager/plasma/plasma_generated.h"

namespace fb = plasma::flatbuf;

namespace plasma {

using fb::MessageType;
using fb::PlasmaError;
using fb::PlasmaObjectSpec;

using flatbuffers::uoffset_t;

inline constexpr std::string_view kDebugString = "debug_string";
inline constexpr std::string_view kObjectId = "object_id";
inline constexpr std::string_view kObjectIds = "object_ids";
inline constexpr std::string_view kOwnerNodeId = "owner_node_id";
inline constexpr std::string_view kOwnerIpAddress = "owner_ip_address";
inline constexpr std::string_view kOnwerWorkerId = "owner_worker_id";

/// \brief Returns maybe_null if not null or a non-null pointer to an arbitrary memory
/// that shouldn't be dereferenced.
///
/// Memset/Memcpy are undefined when a nullptr is passed as an argument use this utility
/// method to wrap locations where this could happen.
///
/// Note: Flatbuffers has UBSan warnings if a zero length vector is passed.
/// https://github.com/google/flatbuffers/pull/5355 is trying to resolve
/// them.
template <typename T>
inline T *MakeNonNull(T *maybe_null) {
  if (RAY_PREDICT_TRUE(maybe_null != nullptr)) {
    return maybe_null;
  }
  static uint8_t non_null_filler;
  return reinterpret_cast<T *>(&non_null_filler);
}

flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>>
ToFlatbuffer(flatbuffers::FlatBufferBuilder *fbb,
             const ObjectID *object_ids,
             int64_t num_objects) {
  std::vector<flatbuffers::Offset<flatbuffers::String>> results;
  results.reserve(num_objects);
  for (int64_t i = 0; i < num_objects; i++) {
    results.push_back(fbb->CreateString(object_ids[i].Binary()));
  }
  return fbb->CreateVector(MakeNonNull(results.data()), results.size());
}

flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>>
ToFlatbuffer(flatbuffers::FlatBufferBuilder *fbb,
             const std::vector<std::string> &strings) {
  std::vector<flatbuffers::Offset<flatbuffers::String>> results;
  results.reserve(strings.size());
  for (size_t i = 0; i < strings.size(); i++) {
    results.push_back(fbb->CreateString(strings[i]));
  }

  return fbb->CreateVector(MakeNonNull(results.data()), results.size());
}

flatbuffers::Offset<flatbuffers::Vector<int64_t>> ToFlatbuffer(
    flatbuffers::FlatBufferBuilder *fbb, const std::vector<int64_t> &data) {
  return fbb->CreateVector(MakeNonNull(data.data()), data.size());
}

Status PlasmaReceive(const std::shared_ptr<StoreConn> &store_conn,
                     MessageType message_type,
                     std::vector<uint8_t> *buffer) {
  if (!store_conn) {
    return Status::IOError("Connection is closed.");
  }
  return store_conn->ReadMessage(static_cast<int64_t>(message_type), buffer);
}

namespace {

// Helper function to create a vector of elements from Data (Request/Reply struct).
// The Getter function is used to extract one element from Data.
template <typename T, typename Data, typename Getter>
void ToVector(const Data &request, std::vector<T> *out, const Getter &getter) {
  int count = request.count();
  out->clear();
  out->reserve(count);
  for (int i = 0; i < count; ++i) {
    out->push_back(getter(request, i));
  }
}

template <typename T, typename FlatbufferVectorPointer, typename Converter>
void ConvertToVector(const FlatbufferVectorPointer fbvector,
                     std::vector<T> *out,
                     const Converter &converter) {
  out->clear();
  out->reserve(fbvector->size());
  for (size_t i = 0; i < fbvector->size(); ++i) {
    out->push_back(converter(*fbvector->Get(i)));
  }
}

template <typename Message>
Status PlasmaSend(const std::shared_ptr<StoreConn> &store_conn,
                  MessageType message_type,
                  flatbuffers::FlatBufferBuilder *fbb,
                  const Message &message) {
  if (!store_conn) {
    return Status::IOError("Connection is closed.");
  }
  fbb->Finish(message);
  return store_conn->WriteMessage(
      static_cast<int64_t>(message_type), fbb->GetSize(), fbb->GetBufferPointer());
}

template <typename Message>
Status PlasmaSend(const std::shared_ptr<Client> &client,
                  MessageType message_type,
                  flatbuffers::FlatBufferBuilder *fbb,
                  const Message &message) {
  if (!client) {
    return Status::IOError("Connection is closed.");
  }
  fbb->Finish(message);
  return client->WriteMessage(
      static_cast<int64_t>(message_type), fbb->GetSize(), fbb->GetBufferPointer());
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
  case fb::PlasmaError::OutOfDisk:
    return Status::OutOfDisk("Local disk is full");
  case fb::PlasmaError::UnexpectedError:
    return Status::UnknownError(
        "an unexpected error occurred, likely due to a bug in the system or caller");
  default:
    RAY_LOG(FATAL) << "unknown plasma error code " << static_cast<int>(plasma_error);
  }
  return Status::OK();
}

}  // namespace

// Get debug string messages.

Status SendGetDebugStringRequest(const std::shared_ptr<StoreConn> &store_conn) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaGetDebugStringRequest(fbb);
  return PlasmaSend(store_conn, MessageType::PlasmaGetDebugStringRequest, &fbb, message);
}

Status SendGetDebugStringReply(const std::shared_ptr<Client> &client,
                               const std::string &debug_string) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaGetDebugStringReply(fbb, fbb.CreateString(debug_string));
  return PlasmaSend(client, MessageType::PlasmaGetDebugStringReply, &fbb, message);
}

Status ReadGetDebugStringReply(uint8_t *data, size_t size, std::string *debug_string) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaGetDebugStringReply>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  VerifyNotNullPtr(
      message->debug_string(), kDebugString, MessageType::PlasmaGetDebugStringReply);
  *debug_string = message->debug_string()->str();
  return Status::OK();
}

// Create messages.

Status SendCreateRetryRequest(const std::shared_ptr<StoreConn> &store_conn,
                              ObjectID object_id,
                              uint64_t request_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaCreateRetryRequest(
      fbb, fbb.CreateString(object_id.Binary()), request_id);
  return PlasmaSend(store_conn, MessageType::PlasmaCreateRetryRequest, &fbb, message);
}

Status SendCreateRequest(const std::shared_ptr<StoreConn> &store_conn,
                         ObjectID object_id,
                         const ray::rpc::Address &owner_address,
                         bool is_experimental_mutable_object,
                         int64_t data_size,
                         int64_t metadata_size,
                         flatbuf::ObjectSource source,
                         int device_num,
                         bool try_immediately) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message =
      fb::CreatePlasmaCreateRequest(fbb,
                                    fbb.CreateString(object_id.Binary()),
                                    fbb.CreateString(owner_address.node_id()),
                                    fbb.CreateString(owner_address.ip_address()),
                                    owner_address.port(),
                                    fbb.CreateString(owner_address.worker_id()),
                                    is_experimental_mutable_object,
                                    data_size,
                                    metadata_size,
                                    source,
                                    device_num,
                                    try_immediately);
  return PlasmaSend(store_conn, MessageType::PlasmaCreateRequest, &fbb, message);
}

void ReadCreateRequest(const uint8_t *data,
                       size_t size,
                       ray::ObjectInfo *object_info,
                       flatbuf::ObjectSource *source,
                       int *device_num) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaCreateRequest>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  object_info->is_mutable = message->is_mutable();
  object_info->data_size = message->data_size();
  object_info->metadata_size = message->metadata_size();
  VerifyNotNullPtr(message->object_id(), kObjectId, MessageType::PlasmaCreateRequest);
  object_info->object_id = ObjectID::FromBinary(message->object_id()->str());
  VerifyNotNullPtr(
      message->owner_node_id(), kOwnerNodeId, MessageType::PlasmaCreateRequest);
  object_info->owner_node_id = NodeID::FromBinary(message->owner_node_id()->str());
  VerifyNotNullPtr(
      message->owner_ip_address(), kOwnerIpAddress, MessageType::PlasmaCreateRequest);
  object_info->owner_ip_address = message->owner_ip_address()->str();
  object_info->owner_port = message->owner_port();
  VerifyNotNullPtr(
      message->owner_worker_id(), kOnwerWorkerId, MessageType::PlasmaCreateRequest);
  object_info->owner_worker_id = WorkerID::FromBinary(message->owner_worker_id()->str());
  *source = message->source();
  *device_num = message->device_num();
}

Status SendUnfinishedCreateReply(const std::shared_ptr<Client> &client,
                                 ObjectID object_id,
                                 uint64_t retry_with_request_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto object_string = fbb.CreateString(object_id.Binary());
  fb::PlasmaCreateReplyBuilder crb(fbb);
  crb.add_object_id(object_string);
  crb.add_retry_with_request_id(retry_with_request_id);
  auto message = crb.Finish();
  return PlasmaSend(client, MessageType::PlasmaCreateReply, &fbb, message);
}

Status SendCreateReply(const std::shared_ptr<Client> &client,
                       ObjectID object_id,
                       const PlasmaObject &object,
                       PlasmaError error_code) {
  flatbuffers::FlatBufferBuilder fbb;
  PlasmaObjectSpec plasma_object(FD2INT(object.store_fd.first),
                                 object.store_fd.second,
                                 object.header_offset,
                                 object.data_offset,
                                 object.data_size,
                                 object.metadata_offset,
                                 object.metadata_size,
                                 object.allocated_size,
                                 object.fallback_allocated,
                                 object.device_num,
                                 object.is_experimental_mutable_object);
  auto object_string = fbb.CreateString(object_id.Binary());
  fb::PlasmaCreateReplyBuilder crb(fbb);
  crb.add_error(static_cast<PlasmaError>(error_code));
  crb.add_plasma_object(&plasma_object);
  crb.add_object_id(object_string);
  crb.add_retry_with_request_id(0);
  crb.add_store_fd(FD2INT(object.store_fd.first));
  crb.add_unique_fd_id(object.store_fd.second);
  crb.add_mmap_size(object.mmap_size);
  if (object.device_num != 0) {
    RAY_LOG(FATAL) << "This should be unreachable.";
  }
  auto message = crb.Finish();
  return PlasmaSend(client, MessageType::PlasmaCreateReply, &fbb, message);
}

Status ReadCreateReply(uint8_t *data,
                       size_t size,
                       ObjectID *object_id,
                       uint64_t *retry_with_request_id,
                       PlasmaObject *object,
                       MEMFD_TYPE *store_fd,
                       int64_t *mmap_size) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaCreateReply>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  *object_id = ObjectID::FromBinary(message->object_id()->str());
  *retry_with_request_id = message->retry_with_request_id();
  if (*retry_with_request_id > 0) {
    // The client should retry the request.
    return Status::OK();
  }

  object->store_fd.first = INT2FD(message->plasma_object()->segment_index());
  object->store_fd.second = message->plasma_object()->unique_fd_id();
  object->header_offset = message->plasma_object()->header_offset();
  object->data_offset = message->plasma_object()->data_offset();
  object->data_size = message->plasma_object()->data_size();
  object->metadata_offset = message->plasma_object()->metadata_offset();
  object->metadata_size = message->plasma_object()->metadata_size();
  object->allocated_size = message->plasma_object()->allocated_size();
  object->fallback_allocated = message->plasma_object()->fallback_allocated();
  object->is_experimental_mutable_object =
      message->plasma_object()->is_experimental_mutable_object();

  store_fd->first = INT2FD(message->store_fd());
  store_fd->second = message->unique_fd_id();
  *mmap_size = message->mmap_size();

  object->device_num = message->plasma_object()->device_num();
  return PlasmaErrorStatus(message->error());
}

Status SendAbortRequest(const std::shared_ptr<StoreConn> &store_conn,
                        ObjectID object_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaAbortRequest(fbb, fbb.CreateString(object_id.Binary()));
  return PlasmaSend(store_conn, MessageType::PlasmaAbortRequest, &fbb, message);
}

void ReadAbortRequest(const uint8_t *data, size_t size, ObjectID *object_id) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaAbortRequest>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  VerifyNotNullPtr(message->object_id(), kObjectId, MessageType::PlasmaAbortRequest);
  *object_id = ObjectID::FromBinary(message->object_id()->str());
}

Status SendAbortReply(const std::shared_ptr<Client> &client, ObjectID object_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaAbortReply(fbb, fbb.CreateString(object_id.Binary()));
  return PlasmaSend(client, MessageType::PlasmaAbortReply, &fbb, message);
}

void ReadAbortReply(uint8_t *data, size_t size, ObjectID *object_id) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaAbortReply>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  *object_id = ObjectID::FromBinary(message->object_id()->str());
}

// Seal messages.

Status SendSealRequest(const std::shared_ptr<StoreConn> &store_conn, ObjectID object_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaSealRequest(fbb, fbb.CreateString(object_id.Binary()));
  return PlasmaSend(store_conn, MessageType::PlasmaSealRequest, &fbb, message);
}

void ReadSealRequest(const uint8_t *data, size_t size, ObjectID *object_id) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaSealRequest>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  VerifyNotNullPtr(message->object_id(), kObjectId, MessageType::PlasmaSealRequest);
  *object_id = ObjectID::FromBinary(message->object_id()->str());
}

Status SendSealReply(const std::shared_ptr<Client> &client,
                     ObjectID object_id,
                     PlasmaError error) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message =
      fb::CreatePlasmaSealReply(fbb, fbb.CreateString(object_id.Binary()), error);
  return PlasmaSend(client, MessageType::PlasmaSealReply, &fbb, message);
}

Status ReadSealReply(uint8_t *data, size_t size, ObjectID *object_id) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaSealReply>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  *object_id = ObjectID::FromBinary(message->object_id()->str());
  return PlasmaErrorStatus(message->error());
}

// Release messages.

Status SendReleaseRequest(const std::shared_ptr<StoreConn> &store_conn,
                          ObjectID object_id,
                          bool may_unmap) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaReleaseRequest(
      fbb, fbb.CreateString(object_id.Binary()), may_unmap);
  return PlasmaSend(store_conn, MessageType::PlasmaReleaseRequest, &fbb, message);
}

void ReadReleaseRequest(const uint8_t *data,
                        size_t size,
                        ObjectID *object_id,
                        bool *may_unmap) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaReleaseRequest>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  VerifyNotNullPtr(message->object_id(), kObjectId, MessageType::PlasmaReleaseRequest);
  *object_id = ObjectID::FromBinary(message->object_id()->str());
  *may_unmap = message->may_unmap();
}

Status SendReleaseReply(const std::shared_ptr<Client> &client,
                        ObjectID object_id,
                        bool should_unmap,
                        PlasmaError error) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaReleaseReply(
      fbb, fbb.CreateString(object_id.Binary()), should_unmap, error);
  return PlasmaSend(client, MessageType::PlasmaReleaseReply, &fbb, message);
}

Status ReadReleaseReply(uint8_t *data,
                        size_t size,
                        ObjectID *object_id,
                        bool *should_unmap) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaReleaseReply>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  *object_id = ObjectID::FromBinary(message->object_id()->str());
  *should_unmap = message->should_unmap();
  return PlasmaErrorStatus(message->error());
}

// Delete objects messages.

Status SendDeleteRequest(const std::shared_ptr<StoreConn> &store_conn,
                         const std::vector<ObjectID> &object_ids) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaDeleteRequest(
      fbb,
      static_cast<int32_t>(object_ids.size()),
      ToFlatbuffer(&fbb, &object_ids[0], object_ids.size()));
  return PlasmaSend(store_conn, MessageType::PlasmaDeleteRequest, &fbb, message);
}

void ReadDeleteRequest(const uint8_t *data,
                       size_t size,
                       std::vector<ObjectID> *object_ids) {
  RAY_DCHECK(data);
  RAY_DCHECK(object_ids);
  auto message = flatbuffers::GetRoot<fb::PlasmaDeleteRequest>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  VerifyNotNullPtr(message->object_ids(), kObjectIds, MessageType::PlasmaDeleteRequest);
  ToVector(*message, object_ids, [](const fb::PlasmaDeleteRequest &request, int i) {
    VerifyNotNullPtr(
        request.object_ids()->Get(i), kObjectId, MessageType::PlasmaDeleteRequest);
    return ObjectID::FromBinary(request.object_ids()->Get(i)->str());
  });
}

Status SendDeleteReply(const std::shared_ptr<Client> &client,
                       const std::vector<ObjectID> &object_ids,
                       const std::vector<PlasmaError> &errors) {
  RAY_DCHECK(object_ids.size() == errors.size());
  flatbuffers::FlatBufferBuilder fbb;
  auto message =
      fb::CreatePlasmaDeleteReply(fbb,
                                  static_cast<int32_t>(object_ids.size()),
                                  ToFlatbuffer(&fbb, &object_ids[0], object_ids.size()),
                                  fbb.CreateVector(errors.data(), errors.size()));
  return PlasmaSend(client, MessageType::PlasmaDeleteReply, &fbb, message);
}

void ReadDeleteReply(uint8_t *data,
                     size_t size,
                     std::vector<ObjectID> *object_ids,
                     std::vector<PlasmaError> *errors) {
  RAY_DCHECK(data);
  RAY_DCHECK(object_ids);
  RAY_DCHECK(errors);
  auto message = flatbuffers::GetRoot<fb::PlasmaDeleteReply>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  ToVector(*message, object_ids, [](const fb::PlasmaDeleteReply &request, int i) {
    return ObjectID::FromBinary(request.object_ids()->Get(i)->str());
  });
  ToVector(*message, errors, [](const fb::PlasmaDeleteReply &request, int i) {
    return static_cast<PlasmaError>(request.errors()->data()[i]);
  });
}

// Contains messages.

Status SendContainsRequest(const std::shared_ptr<StoreConn> &store_conn,
                           ObjectID object_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message =
      fb::CreatePlasmaContainsRequest(fbb, fbb.CreateString(object_id.Binary()));
  return PlasmaSend(store_conn, MessageType::PlasmaContainsRequest, &fbb, message);
}

void ReadContainsRequest(const uint8_t *data, size_t size, ObjectID *object_id) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaContainsRequest>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  VerifyNotNullPtr(message->object_id(), kObjectId, MessageType::PlasmaContainsRequest);
  *object_id = ObjectID::FromBinary(message->object_id()->str());
}

Status SendContainsReply(const std::shared_ptr<Client> &client,
                         ObjectID object_id,
                         bool has_object) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaContainsReply(
      fbb, fbb.CreateString(object_id.Binary()), static_cast<int32_t>(has_object));
  return PlasmaSend(client, MessageType::PlasmaContainsReply, &fbb, message);
}

void ReadContainsReply(uint8_t *data,
                       size_t size,
                       ObjectID *object_id,
                       bool *has_object) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaContainsReply>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  *object_id = ObjectID::FromBinary(message->object_id()->str());
  *has_object = message->has_object();
}

// Connect messages.

Status SendConnectRequest(const std::shared_ptr<StoreConn> &store_conn) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaConnectRequest(fbb);
  return PlasmaSend(store_conn, MessageType::PlasmaConnectRequest, &fbb, message);
}

Status SendConnectReply(const std::shared_ptr<Client> &client, int64_t memory_capacity) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaConnectReply(fbb, memory_capacity);
  return PlasmaSend(client, MessageType::PlasmaConnectReply, &fbb, message);
}

void ReadConnectReply(uint8_t *data, size_t size) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaConnectReply>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
}

// Get messages.

Status SendGetRequest(const std::shared_ptr<StoreConn> &store_conn,
                      const ObjectID *object_ids,
                      int64_t num_objects,
                      int64_t timeout_ms) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaGetRequest(
      fbb, ToFlatbuffer(&fbb, object_ids, num_objects), timeout_ms);
  return PlasmaSend(store_conn, MessageType::PlasmaGetRequest, &fbb, message);
}

void ReadGetRequest(const uint8_t *data,
                    size_t size,
                    std::vector<ObjectID> &object_ids,
                    int64_t *timeout_ms) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaGetRequest>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  VerifyNotNullPtr(message->object_ids(), kObjectIds, MessageType::PlasmaGetRequest);
  object_ids.reserve(message->object_ids()->size());
  for (uoffset_t i = 0; i < message->object_ids()->size(); ++i) {
    VerifyNotNullPtr(
        message->object_ids()->Get(i), kObjectId, MessageType::PlasmaGetRequest);
    auto object_id = message->object_ids()->Get(i)->str();
    object_ids.push_back(ObjectID::FromBinary(object_id));
  }
  *timeout_ms = message->timeout_ms();
}

Status SendGetReply(const std::shared_ptr<Client> &client,
                    ObjectID object_ids[],
                    absl::flat_hash_map<ObjectID, PlasmaObject> &plasma_objects,
                    int64_t num_objects,
                    const std::vector<MEMFD_TYPE> &store_fds,
                    const std::vector<int64_t> &mmap_sizes) {
  flatbuffers::FlatBufferBuilder fbb;
  std::vector<PlasmaObjectSpec> objects;
  objects.reserve(num_objects);

  std::vector<flatbuffers::Offset<fb::CudaHandle>> handles;
  for (int64_t i = 0; i < num_objects; ++i) {
    const PlasmaObject &object = plasma_objects[object_ids[i]];
    RAY_LOG(DEBUG) << "Sending object info, id: " << object_ids[i]
                   << " data_size: " << object.data_size
                   << " metadata_size: " << object.metadata_size;
    objects.emplace_back(FD2INT(object.store_fd.first),
                         object.store_fd.second,
                         object.header_offset,
                         object.data_offset,
                         object.data_size,
                         object.metadata_offset,
                         object.metadata_size,
                         object.allocated_size,
                         object.fallback_allocated,
                         object.device_num,
                         object.is_experimental_mutable_object);
  }
  std::vector<int> store_fds_as_int;
  std::vector<int64_t> unique_fd_ids;
  for (MEMFD_TYPE store_fd : store_fds) {
    store_fds_as_int.push_back(FD2INT(store_fd.first));
    unique_fd_ids.push_back(store_fd.second);
  }
  auto message = fb::CreatePlasmaGetReply(
      fbb,
      ToFlatbuffer(&fbb, object_ids, num_objects),
      fbb.CreateVectorOfStructs(MakeNonNull(objects.data()), num_objects),
      fbb.CreateVector(MakeNonNull(store_fds_as_int.data()), store_fds_as_int.size()),
      fbb.CreateVector(MakeNonNull(unique_fd_ids.data()), unique_fd_ids.size()),
      fbb.CreateVector(MakeNonNull(mmap_sizes.data()), mmap_sizes.size()),
      fbb.CreateVector(MakeNonNull(handles.data()), handles.size()));
  return PlasmaSend(client, MessageType::PlasmaGetReply, &fbb, message);
}

void ReadGetReply(uint8_t *data,
                  size_t size,
                  ObjectID object_ids[],
                  PlasmaObject plasma_objects[],
                  int64_t num_objects,
                  std::vector<MEMFD_TYPE> &store_fds,
                  std::vector<int64_t> &mmap_sizes) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaGetReply>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  for (uoffset_t i = 0; i < num_objects; ++i) {
    object_ids[i] = ObjectID::FromBinary(message->object_ids()->Get(i)->str());
  }
  for (uoffset_t i = 0; i < num_objects; ++i) {
    const PlasmaObjectSpec *object = message->plasma_objects()->Get(i);
    plasma_objects[i].store_fd.first = INT2FD(object->segment_index());
    plasma_objects[i].store_fd.second = object->unique_fd_id();
    plasma_objects[i].header_offset = object->header_offset();
    plasma_objects[i].data_offset = object->data_offset();
    plasma_objects[i].data_size = object->data_size();
    plasma_objects[i].metadata_offset = object->metadata_offset();
    plasma_objects[i].metadata_size = object->metadata_size();
    plasma_objects[i].allocated_size = object->allocated_size();
    plasma_objects[i].device_num = object->device_num();
    plasma_objects[i].fallback_allocated = object->fallback_allocated();
    plasma_objects[i].is_experimental_mutable_object =
        object->is_experimental_mutable_object();
  }
  RAY_CHECK(message->store_fds()->size() == message->mmap_sizes()->size());
  store_fds.reserve(message->store_fds()->size());
  mmap_sizes.reserve(message->store_fds()->size());
  for (uoffset_t i = 0; i < message->store_fds()->size(); i++) {
    store_fds.emplace_back(INT2FD(message->store_fds()->Get(i)),
                           message->unique_fd_ids()->Get(i));
    mmap_sizes.push_back(message->mmap_sizes()->Get(i));
  }
}

}  // namespace plasma
