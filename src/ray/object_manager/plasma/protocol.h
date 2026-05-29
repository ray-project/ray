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

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "ray/common/status.h"
#include "ray/object_manager/common.h"
#include "ray/object_manager/plasma/common.h"
#include "ray/object_manager/plasma/plasma.h"
#include "ray/object_manager/plasma/plasma_generated.h"
#include "src/ray/protobuf/common.pb.h"

namespace plasma {

class Client;
class StoreConn;

using ray::Status;

using flatbuf::MessageType;
using flatbuf::ObjectSource;
using flatbuf::PlasmaError;

template <class T>
bool VerifyFlatbuffer(T *object, const uint8_t *data, size_t size) {
  flatbuffers::Verifier verifier(data, size);
  return object->Verify(verifier);
}

template <class T>
void VerifyNotNullPtr(T *object, std::string_view obj_name, MessageType msg_type) {
  RAY_CHECK(object != nullptr) << "Corrupted " << EnumNameMessageType(msg_type)
                               << " message: " << obj_name << " is null. "
                               << kCorruptedRequestErrorMessage;
}

flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>>
ToFlatbuffer(flatbuffers::FlatBufferBuilder *fbb,
             const ObjectID *object_ids,
             int64_t num_objects);

flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>>
ToFlatbuffer(flatbuffers::FlatBufferBuilder *fbb,
             const std::vector<std::string> &strings);

flatbuffers::Offset<flatbuffers::Vector<int64_t>> ToFlatbuffer(
    flatbuffers::FlatBufferBuilder *fbb, const std::vector<int64_t> &data);

/* Plasma receive message. */

Status PlasmaReceive(const std::shared_ptr<StoreConn> &store_conn,
                     MessageType message_type,
                     std::vector<uint8_t> *buffer);

/* Debug string messages. */

Status SendGetDebugStringRequest(const std::shared_ptr<StoreConn> &store_conn);

Status SendGetDebugStringReply(const std::shared_ptr<Client> &client,
                               const std::string &debug_string);

Status ReadGetDebugStringReply(uint8_t *data, size_t size, std::string *debug_string);

/* Plasma Create message functions. */

Status SendCreateRetryRequest(const std::shared_ptr<StoreConn> &store_conn,
                              ObjectID object_id,
                              uint64_t request_id);

Status SendCreateRequest(const std::shared_ptr<StoreConn> &store_conn,
                         ObjectID object_id,
                         const ray::rpc::Address &owner_address,
                         bool is_mutable,
                         int64_t data_size,
                         int64_t metadata_size,
                         flatbuf::ObjectSource source,
                         int device_num,
                         bool try_immediately);

void ReadCreateRequest(const uint8_t *data,
                       size_t size,
                       ray::ObjectInfo *object_info,
                       flatbuf::ObjectSource *source,
                       int *device_num);

Status SendUnfinishedCreateReply(const std::shared_ptr<Client> &client,
                                 ObjectID object_id,
                                 uint64_t retry_with_request_id);

Status SendCreateReply(const std::shared_ptr<Client> &client,
                       ObjectID object_id,
                       const PlasmaObject &object,
                       PlasmaError error);

Status ReadCreateReply(uint8_t *data,
                       size_t size,
                       ObjectID *object_id,
                       uint64_t *retry_with_request_id,
                       PlasmaObject *object,
                       MEMFD_TYPE *store_fd,
                       int64_t *mmap_size);

Status SendAbortRequest(const std::shared_ptr<StoreConn> &store_conn, ObjectID object_id);

void ReadAbortRequest(const uint8_t *data, size_t size, ObjectID *object_id);

Status SendAbortReply(const std::shared_ptr<Client> &client, ObjectID object_id);

void ReadAbortReply(uint8_t *data, size_t size, ObjectID *object_id);

/* Plasma Seal message functions. */

Status SendSealRequest(const std::shared_ptr<StoreConn> &store_conn, ObjectID object_id);

void ReadSealRequest(const uint8_t *data, size_t size, ObjectID *object_id);

Status SendSealReply(const std::shared_ptr<Client> &client,
                     ObjectID object_id,
                     PlasmaError error);

Status ReadSealReply(uint8_t *data, size_t size, ObjectID *object_id);

/* Plasma Get message functions. */

Status SendGetRequest(const std::shared_ptr<StoreConn> &store_conn,
                      const ObjectID *object_ids,
                      int64_t num_objects,
                      int64_t timeout_ms);

void ReadGetRequest(const uint8_t *data,
                    size_t size,
                    std::vector<ObjectID> &object_ids,
                    int64_t *timeout_ms);

Status SendGetReply(const std::shared_ptr<Client> &client,
                    ObjectID object_ids[],
                    absl::flat_hash_map<ObjectID, PlasmaObject> &plasma_objects,
                    int64_t num_objects,
                    const std::vector<MEMFD_TYPE> &store_fds,
                    const std::vector<int64_t> &mmap_sizes);

void ReadGetReply(uint8_t *data,
                  size_t size,
                  ObjectID object_ids[],
                  PlasmaObject plasma_objects[],
                  int64_t num_objects,
                  std::vector<MEMFD_TYPE> &store_fds,
                  std::vector<int64_t> &mmap_sizes);

/* Plasma Release message functions. */

Status SendReleaseRequest(const std::shared_ptr<StoreConn> &store_conn,
                          ObjectID object_id,
                          bool may_unmap);

void ReadReleaseRequest(const uint8_t *data,
                        size_t size,
                        ObjectID *object_id,
                        bool *may_unmap);

Status SendReleaseReply(const std::shared_ptr<Client> &client,
                        ObjectID object_id,
                        bool should_unmap,
                        PlasmaError error);

Status ReadReleaseReply(uint8_t *data,
                        size_t size,
                        ObjectID *object_id,
                        bool *should_unmap);

/* Plasma Delete objects message functions. */

Status SendDeleteRequest(const std::shared_ptr<StoreConn> &store_conn,
                         const std::vector<ObjectID> &object_ids);

void ReadDeleteRequest(const uint8_t *data,
                       size_t size,
                       std::vector<ObjectID> *object_ids);

Status SendDeleteReply(const std::shared_ptr<Client> &client,
                       const std::vector<ObjectID> &object_ids,
                       const std::vector<PlasmaError> &errors);

void ReadDeleteReply(uint8_t *data,
                     size_t size,
                     std::vector<ObjectID> *object_ids,
                     std::vector<PlasmaError> *errors);

/* Plasma Contains message functions. */

Status SendContainsRequest(const std::shared_ptr<StoreConn> &store_conn,
                           ObjectID object_id);

void ReadContainsRequest(const uint8_t *data, size_t size, ObjectID *object_id);

Status SendContainsReply(const std::shared_ptr<Client> &client,
                         ObjectID object_id,
                         bool has_object);

void ReadContainsReply(uint8_t *data, size_t size, ObjectID *object_id, bool *has_object);

/* Plasma Connect message functions. */

Status SendConnectRequest(const std::shared_ptr<StoreConn> &store_conn);

Status SendConnectReply(const std::shared_ptr<Client> &client, int64_t memory_capacity);

void ReadConnectReply(uint8_t *data, size_t size);

}  // namespace plasma
