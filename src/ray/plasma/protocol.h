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

#ifndef PLASMA_PROTOCOL_H
#define PLASMA_PROTOCOL_H

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "arrow/status.h"
#include "plasma/plasma.h"
#include "plasma/plasma_generated.h"

namespace plasma {

using arrow::Status;

using flatbuf::MessageType;
using flatbuf::PlasmaError;

template <class T>
bool VerifyFlatbuffer(T* object, const uint8_t* data, size_t size) {
  flatbuffers::Verifier verifier(data, size);
  return object->Verify(verifier);
}

flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>>
ToFlatbuffer(flatbuffers::FlatBufferBuilder* fbb, const ObjectID* object_ids,
             int64_t num_objects);

flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>>
ToFlatbuffer(flatbuffers::FlatBufferBuilder* fbb,
             const std::vector<std::string>& strings);

flatbuffers::Offset<flatbuffers::Vector<int64_t>> ToFlatbuffer(
    flatbuffers::FlatBufferBuilder* fbb, const std::vector<int64_t>& data);

/* Plasma receive message. */

Status PlasmaReceive(int sock, MessageType message_type, std::vector<uint8_t>* buffer);

/* Set options messages. */

Status SendSetOptionsRequest(int sock, const std::string& client_name,
                             int64_t output_memory_limit);

Status ReadSetOptionsRequest(const uint8_t* data, size_t size, std::string* client_name,
                             int64_t* output_memory_quota);

Status SendSetOptionsReply(int sock, PlasmaError error);

Status ReadSetOptionsReply(const uint8_t* data, size_t size);

/* Debug string messages. */

Status SendGetDebugStringRequest(int sock);

Status SendGetDebugStringReply(int sock, const std::string& debug_string);

Status ReadGetDebugStringReply(const uint8_t* data, size_t size,
                               std::string* debug_string);

/* Plasma Create message functions. */

Status SendCreateRequest(int sock, ObjectID object_id, bool evict_if_full,
                         int64_t data_size, int64_t metadata_size, int device_num);

Status ReadCreateRequest(const uint8_t* data, size_t size, ObjectID* object_id,
                         bool* evict_if_full, int64_t* data_size, int64_t* metadata_size,
                         int* device_num);

Status SendCreateReply(int sock, ObjectID object_id, PlasmaObject* object,
                       PlasmaError error, int64_t mmap_size);

Status ReadCreateReply(const uint8_t* data, size_t size, ObjectID* object_id,
                       PlasmaObject* object, int* store_fd, int64_t* mmap_size);

Status SendCreateAndSealRequest(int sock, const ObjectID& object_id, bool evict_if_full,
                                const std::string& data, const std::string& metadata,
                                unsigned char* digest);

Status ReadCreateAndSealRequest(const uint8_t* data, size_t size, ObjectID* object_id,
                                bool* evict_if_full, std::string* object_data,
                                std::string* metadata, std::string* digest);

Status SendCreateAndSealBatchRequest(int sock, const std::vector<ObjectID>& object_ids,
                                     bool evict_if_full,
                                     const std::vector<std::string>& data,
                                     const std::vector<std::string>& metadata,
                                     const std::vector<std::string>& digests);

Status ReadCreateAndSealBatchRequest(const uint8_t* data, size_t size,
                                     std::vector<ObjectID>* object_id,
                                     bool* evict_if_full,
                                     std::vector<std::string>* object_data,
                                     std::vector<std::string>* metadata,
                                     std::vector<std::string>* digests);

Status SendCreateAndSealReply(int sock, PlasmaError error);

Status ReadCreateAndSealReply(const uint8_t* data, size_t size);

Status SendCreateAndSealBatchReply(int sock, PlasmaError error);

Status ReadCreateAndSealBatchReply(const uint8_t* data, size_t size);

Status SendAbortRequest(int sock, ObjectID object_id);

Status ReadAbortRequest(const uint8_t* data, size_t size, ObjectID* object_id);

Status SendAbortReply(int sock, ObjectID object_id);

Status ReadAbortReply(const uint8_t* data, size_t size, ObjectID* object_id);

/* Plasma Seal message functions. */

Status SendSealRequest(int sock, ObjectID object_id, const std::string& digest);

Status ReadSealRequest(const uint8_t* data, size_t size, ObjectID* object_id,
                       std::string* digest);

Status SendSealReply(int sock, ObjectID object_id, PlasmaError error);

Status ReadSealReply(const uint8_t* data, size_t size, ObjectID* object_id);

/* Plasma Get message functions. */

Status SendGetRequest(int sock, const ObjectID* object_ids, int64_t num_objects,
                      int64_t timeout_ms);

Status ReadGetRequest(const uint8_t* data, size_t size, std::vector<ObjectID>& object_ids,
                      int64_t* timeout_ms);

Status SendGetReply(int sock, ObjectID object_ids[],
                    std::unordered_map<ObjectID, PlasmaObject>& plasma_objects,
                    int64_t num_objects, const std::vector<int>& store_fds,
                    const std::vector<int64_t>& mmap_sizes);

Status ReadGetReply(const uint8_t* data, size_t size, ObjectID object_ids[],
                    PlasmaObject plasma_objects[], int64_t num_objects,
                    std::vector<int>& store_fds, std::vector<int64_t>& mmap_sizes);

/* Plasma Release message functions. */

Status SendReleaseRequest(int sock, ObjectID object_id);

Status ReadReleaseRequest(const uint8_t* data, size_t size, ObjectID* object_id);

Status SendReleaseReply(int sock, ObjectID object_id, PlasmaError error);

Status ReadReleaseReply(const uint8_t* data, size_t size, ObjectID* object_id);

/* Plasma Delete objects message functions. */

Status SendDeleteRequest(int sock, const std::vector<ObjectID>& object_ids);

Status ReadDeleteRequest(const uint8_t* data, size_t size,
                         std::vector<ObjectID>* object_ids);

Status SendDeleteReply(int sock, const std::vector<ObjectID>& object_ids,
                       const std::vector<PlasmaError>& errors);

Status ReadDeleteReply(const uint8_t* data, size_t size,
                       std::vector<ObjectID>* object_ids,
                       std::vector<PlasmaError>* errors);

/* Plasma Contains message functions. */

Status SendContainsRequest(int sock, ObjectID object_id);

Status ReadContainsRequest(const uint8_t* data, size_t size, ObjectID* object_id);

Status SendContainsReply(int sock, ObjectID object_id, bool has_object);

Status ReadContainsReply(const uint8_t* data, size_t size, ObjectID* object_id,
                         bool* has_object);

/* Plasma List message functions. */

Status SendListRequest(int sock);

Status ReadListRequest(const uint8_t* data, size_t size);

Status SendListReply(int sock, const ObjectTable& objects);

Status ReadListReply(const uint8_t* data, size_t size, ObjectTable* objects);

/* Plasma Connect message functions. */

Status SendConnectRequest(int sock);

Status ReadConnectRequest(const uint8_t* data, size_t size);

Status SendConnectReply(int sock, int64_t memory_capacity);

Status ReadConnectReply(const uint8_t* data, size_t size, int64_t* memory_capacity);

/* Plasma Evict message functions (no reply so far). */

Status SendEvictRequest(int sock, int64_t num_bytes);

Status ReadEvictRequest(const uint8_t* data, size_t size, int64_t* num_bytes);

Status SendEvictReply(int sock, int64_t num_bytes);

Status ReadEvictReply(const uint8_t* data, size_t size, int64_t& num_bytes);

/* Plasma Subscribe message functions. */

Status SendSubscribeRequest(int sock);

/* Data messages. */

Status SendDataRequest(int sock, ObjectID object_id, const char* address, int port);

Status ReadDataRequest(const uint8_t* data, size_t size, ObjectID* object_id,
                       char** address, int* port);

Status SendDataReply(int sock, ObjectID object_id, int64_t object_size,
                     int64_t metadata_size);

Status ReadDataReply(const uint8_t* data, size_t size, ObjectID* object_id,
                     int64_t* object_size, int64_t* metadata_size);

/* Plasma refresh LRU cache functions. */

Status SendRefreshLRURequest(int sock, const std::vector<ObjectID>& object_ids);

Status ReadRefreshLRURequest(const uint8_t* data, size_t size,
                             std::vector<ObjectID>* object_ids);

Status SendRefreshLRUReply(int sock);

Status ReadRefreshLRUReply(const uint8_t* data, size_t size);

}  // namespace plasma

#endif /* PLASMA_PROTOCOL */
