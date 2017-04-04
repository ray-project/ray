#include "flatbuffers/flatbuffers.h"
#include "format/plasma_generated.h"

#include "plasma_protocol.h"
#include "common_protocol.h"
#include "io.h"

#define FLATBUFFER_BUILDER_DEFAULT_SIZE 1024

protocol_builder *make_protocol_builder(void) {
  return NULL;
}

void free_protocol_builder(protocol_builder *builder) {}

uint8_t *plasma_receive(int sock, int64_t message_type) {
  int64_t type;
  int64_t length;
  uint8_t *reply_data;
  read_message(sock, &type, &length, &reply_data);
  CHECKM(type == message_type, "type = %" PRId64 ", message_type = %" PRId64,
         type, message_type);
  return reply_data;
}

/* Create messages. */

int plasma_send_CreateRequest(int sock,
                              protocol_builder *B,
                              ObjectID object_id,
                              int64_t data_size,
                              int64_t metadata_size) {
  flatbuffers::FlatBufferBuilder fbb(FLATBUFFER_BUILDER_DEFAULT_SIZE);
  auto message = CreatePlasmaCreateRequest(fbb, to_flatbuf(fbb, object_id),
                                           data_size, metadata_size);
  fbb.Finish(message);
  return write_message(sock, MessageType_PlasmaCreateRequest, fbb.GetSize(),
                       fbb.GetBufferPointer());
}

void plasma_read_CreateRequest(uint8_t *data,
                               ObjectID *object_id,
                               int64_t *data_size,
                               int64_t *metadata_size) {
  CHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaCreateRequest>(data);
  *data_size = message->data_size();
  *metadata_size = message->metadata_size();
  *object_id = from_flatbuf(message->object_id());
}

int plasma_send_CreateReply(int sock,
                            protocol_builder *B,
                            ObjectID object_id,
                            PlasmaObject *object,
                            int error_code) {
  flatbuffers::FlatBufferBuilder fbb(FLATBUFFER_BUILDER_DEFAULT_SIZE);
  PlasmaObjectSpec plasma_object(
      object->handle.store_fd, object->handle.mmap_size, object->data_offset,
      object->data_size, object->metadata_offset, object->metadata_size);
  auto message =
      CreatePlasmaCreateReply(fbb, to_flatbuf(fbb, object_id), &plasma_object,
                              (PlasmaError) error_code);
  fbb.Finish(message);
  return write_message(sock, MessageType_PlasmaCreateReply, fbb.GetSize(),
                       fbb.GetBufferPointer());
}

void plasma_read_CreateReply(uint8_t *data,
                             ObjectID *object_id,
                             PlasmaObject *object,
                             int *error_code) {
  CHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaCreateReply>(data);
  *object_id = from_flatbuf(message->object_id());
  object->handle.store_fd = message->plasma_object()->segment_index();
  object->handle.mmap_size = message->plasma_object()->mmap_size();
  object->data_offset = message->plasma_object()->data_offset();
  object->data_size = message->plasma_object()->data_size();
  object->metadata_offset = message->plasma_object()->metadata_offset();
  object->metadata_size = message->plasma_object()->metadata_size();
  *error_code = message->error();
}

/* Seal messages. */

int plasma_send_SealRequest(int sock,
                            protocol_builder *B,
                            ObjectID object_id,
                            unsigned char *digest) {
  flatbuffers::FlatBufferBuilder fbb(FLATBUFFER_BUILDER_DEFAULT_SIZE);
  auto digest_string = fbb.CreateString((char *) digest, DIGEST_SIZE);
  auto message =
      CreatePlasmaSealRequest(fbb, to_flatbuf(fbb, object_id), digest_string);
  fbb.Finish(message);
  return write_message(sock, MessageType_PlasmaSealRequest, fbb.GetSize(),
                       fbb.GetBufferPointer());
}

void plasma_read_SealRequest(uint8_t *data,
                             ObjectID *object_id,
                             unsigned char *digest) {
  CHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaSealRequest>(data);
  *object_id = from_flatbuf(message->object_id());
  CHECK(message->digest()->size() == DIGEST_SIZE);
  memcpy(digest, message->digest()->data(), DIGEST_SIZE);
}

int plasma_send_SealReply(int sock,
                          protocol_builder *B,
                          ObjectID object_id,
                          int error) {
  flatbuffers::FlatBufferBuilder fbb(FLATBUFFER_BUILDER_DEFAULT_SIZE);
  auto message = CreatePlasmaSealReply(fbb, to_flatbuf(fbb, object_id),
                                       (PlasmaError) error);
  fbb.Finish(message);
  return write_message(sock, MessageType_PlasmaSealReply, fbb.GetSize(),
                       fbb.GetBufferPointer());
}

void plasma_read_SealReply(uint8_t *data, ObjectID *object_id, int *error) {
  CHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaSealReply>(data);
  *object_id = from_flatbuf(message->object_id());
  *error = message->error();
}

/* Release messages. */

int plasma_send_ReleaseRequest(int sock,
                               protocol_builder *B,
                               ObjectID object_id) {
  flatbuffers::FlatBufferBuilder fbb(FLATBUFFER_BUILDER_DEFAULT_SIZE);
  auto message = CreatePlasmaSealRequest(fbb, to_flatbuf(fbb, object_id));
  fbb.Finish(message);
  return write_message(sock, MessageType_PlasmaReleaseRequest, fbb.GetSize(),
                       fbb.GetBufferPointer());
}

void plasma_read_ReleaseRequest(uint8_t *data, ObjectID *object_id) {
  CHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaReleaseRequest>(data);
  *object_id = from_flatbuf(message->object_id());
}

int plasma_send_ReleaseReply(int sock,
                             protocol_builder *B,
                             ObjectID object_id,
                             int error) {
  flatbuffers::FlatBufferBuilder fbb(FLATBUFFER_BUILDER_DEFAULT_SIZE);
  auto message = CreatePlasmaReleaseReply(fbb, to_flatbuf(fbb, object_id),
                                          (PlasmaError) error);
  fbb.Finish(message);
  return write_message(sock, MessageType_PlasmaReleaseReply, fbb.GetSize(),
                       fbb.GetBufferPointer());
}

void plasma_read_ReleaseReply(uint8_t *data, ObjectID *object_id, int *error) {
  CHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaReleaseReply>(data);
  *object_id = from_flatbuf(message->object_id());
  *error = message->error();
}

/* Delete messages. */

int plasma_send_DeleteRequest(int sock,
                              protocol_builder *B,
                              ObjectID object_id) {
  flatbuffers::FlatBufferBuilder fbb(FLATBUFFER_BUILDER_DEFAULT_SIZE);
  auto message = CreatePlasmaDeleteRequest(fbb, to_flatbuf(fbb, object_id));
  fbb.Finish(message);
  return write_message(sock, MessageType_PlasmaDeleteRequest, fbb.GetSize(),
                       fbb.GetBufferPointer());
}

void plasma_read_DeleteRequest(uint8_t *data, ObjectID *object_id) {
  CHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaReleaseReply>(data);
  *object_id = from_flatbuf(message->object_id());
}

int plasma_send_DeleteReply(int sock,
                            protocol_builder *B,
                            ObjectID object_id,
                            int error) {
  flatbuffers::FlatBufferBuilder fbb(FLATBUFFER_BUILDER_DEFAULT_SIZE);
  auto message = CreatePlasmaDeleteReply(fbb, to_flatbuf(fbb, object_id),
                                         (PlasmaError) error);
  fbb.Finish(message);
  return write_message(sock, MessageType_PlasmaDeleteReply, fbb.GetSize(),
                       fbb.GetBufferPointer());
}

void plasma_read_DeleteReply(uint8_t *data, ObjectID *object_id, int *error) {
  CHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaDeleteReply>(data);
  *object_id = from_flatbuf(message->object_id());
  *error = message->error();
}

/* Satus messages. */

int plasma_send_StatusRequest(int sock,
                              protocol_builder *B,
                              ObjectID object_ids[],
                              int64_t num_objects) {
  flatbuffers::FlatBufferBuilder fbb(FLATBUFFER_BUILDER_DEFAULT_SIZE);
  auto message =
      CreatePlasmaStatusRequest(fbb, to_flatbuf(fbb, object_ids, num_objects));
  fbb.Finish(message);
  return write_message(sock, MessageType_PlasmaStatusRequest, fbb.GetSize(),
                       fbb.GetBufferPointer());
}

int64_t plasma_read_StatusRequest_num_objects(uint8_t *data) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaStatusRequest>(data);
  return message->object_ids()->size();
}

void plasma_read_StatusRequest(uint8_t *data,
                               ObjectID object_ids[],
                               int64_t num_objects) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaStatusRequest>(data);
  for (int64_t i = 0; i < num_objects; ++i) {
    object_ids[i] = from_flatbuf(message->object_ids()->Get(i));
  }
}

int plasma_send_StatusReply(int sock,
                            protocol_builder *B,
                            ObjectID object_ids[],
                            int object_status[],
                            int64_t num_objects) {
  flatbuffers::FlatBufferBuilder fbb(FLATBUFFER_BUILDER_DEFAULT_SIZE);
  auto message =
      CreatePlasmaStatusReply(fbb, to_flatbuf(fbb, object_ids, num_objects),
                              fbb.CreateVector(object_status, num_objects));
  fbb.Finish(message);
  return write_message(sock, MessageType_PlasmaStatusReply, fbb.GetSize(),
                       fbb.GetBufferPointer());
}

int64_t plasma_read_StatusReply_num_objects(uint8_t *data) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaStatusReply>(data);
  return message->object_ids()->size();
}

void plasma_read_StatusReply(uint8_t *data,
                             ObjectID object_ids[],
                             int object_status[],
                             int64_t num_objects) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaStatusReply>(data);
  for (int64_t i = 0; i < num_objects; ++i) {
    object_ids[i] = from_flatbuf(message->object_ids()->Get(i));
  }
  for (int64_t i = 0; i < num_objects; ++i) {
    object_status[i] = message->status()->data()[i];
  }
}

/* Contains messages. */

int plasma_send_ContainsRequest(int sock,
                                protocol_builder *B,
                                ObjectID object_id) {
  flatbuffers::FlatBufferBuilder fbb(FLATBUFFER_BUILDER_DEFAULT_SIZE);
  auto message = CreatePlasmaContainsRequest(fbb, to_flatbuf(fbb, object_id));
  fbb.Finish(message);
  return write_message(sock, MessageType_PlasmaContainsRequest, fbb.GetSize(),
                       fbb.GetBufferPointer());
}

void plasma_read_ContainsRequest(uint8_t *data, ObjectID *object_id) {
  CHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaContainsRequest>(data);
  *object_id = from_flatbuf(message->object_id());
}

int plasma_send_ContainsReply(int sock,
                              protocol_builder *B,
                              ObjectID object_id,
                              int has_object) {
  flatbuffers::FlatBufferBuilder fbb(FLATBUFFER_BUILDER_DEFAULT_SIZE);
  auto message =
      CreatePlasmaContainsReply(fbb, to_flatbuf(fbb, object_id), has_object);
  fbb.Finish(message);
  return write_message(sock, MessageType_PlasmaContainsReply, fbb.GetSize(),
                       fbb.GetBufferPointer());
}

void plasma_read_ContainsReply(uint8_t *data,
                               ObjectID *object_id,
                               int *has_object) {
  CHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaContainsReply>(data);
  *object_id = from_flatbuf(message->object_id());
  *has_object = message->has_object();
}

/* Connect messages. */

int plasma_send_ConnectRequest(int sock, protocol_builder *B) {
  flatbuffers::FlatBufferBuilder fbb(FLATBUFFER_BUILDER_DEFAULT_SIZE);
  auto message = CreatePlasmaConnectRequest(fbb);
  fbb.Finish(message);
  return write_message(sock, MessageType_PlasmaConnectRequest, fbb.GetSize(),
                       fbb.GetBufferPointer());
}

void plasma_read_ConnectRequest(uint8_t *data) {}

int plasma_send_ConnectReply(int sock,
                             protocol_builder *B,
                             int64_t memory_capacity) {
  flatbuffers::FlatBufferBuilder fbb(FLATBUFFER_BUILDER_DEFAULT_SIZE);
  auto message = CreatePlasmaConnectReply(fbb, memory_capacity);
  fbb.Finish(message);
  return write_message(sock, MessageType_PlasmaConnectReply, fbb.GetSize(),
                       fbb.GetBufferPointer());
}

void plasma_read_ConnectReply(uint8_t *data, int64_t *memory_capacity) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaConnectReply>(data);
  *memory_capacity = message->memory_capacity();
}

/* Evict messages. */

int plasma_send_EvictRequest(int sock, protocol_builder *B, int64_t num_bytes) {
  flatbuffers::FlatBufferBuilder fbb(FLATBUFFER_BUILDER_DEFAULT_SIZE);
  auto message = CreatePlasmaEvictRequest(fbb, num_bytes);
  fbb.Finish(message);
  return write_message(sock, MessageType_PlasmaEvictRequest, fbb.GetSize(),
                       fbb.GetBufferPointer());
}

void plasma_read_EvictRequest(uint8_t *data, int64_t *num_bytes) {
  CHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaEvictRequest>(data);
  *num_bytes = message->num_bytes();
}

int plasma_send_EvictReply(int sock, protocol_builder *B, int64_t num_bytes) {
  flatbuffers::FlatBufferBuilder fbb(FLATBUFFER_BUILDER_DEFAULT_SIZE);
  auto message = CreatePlasmaEvictReply(fbb, num_bytes);
  fbb.Finish(message);
  return write_message(sock, MessageType_PlasmaEvictReply, fbb.GetSize(),
                       fbb.GetBufferPointer());
}

void plasma_read_EvictReply(uint8_t *data, int64_t *num_bytes) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaEvictReply>(data);
  *num_bytes = message->num_bytes();
}

/* Get messages. */

int plasma_send_GetRequest(int sock,
                           protocol_builder *B,
                           ObjectID object_ids[],
                           int64_t num_objects,
                           int64_t timeout_ms) {
  flatbuffers::FlatBufferBuilder fbb(FLATBUFFER_BUILDER_DEFAULT_SIZE);
  auto message = CreatePlasmaGetRequest(
      fbb, to_flatbuf(fbb, object_ids, num_objects), timeout_ms);
  fbb.Finish(message);
  return write_message(sock, MessageType_PlasmaGetRequest, fbb.GetSize(),
                       fbb.GetBufferPointer());
}

int64_t plasma_read_GetRequest_num_objects(uint8_t *data) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaGetRequest>(data);
  return message->object_ids()->size();
}

void plasma_read_GetRequest(uint8_t *data,
                            ObjectID object_ids[],
                            int64_t *timeout_ms,
                            int64_t num_objects) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaGetRequest>(data);
  for (int64_t i = 0; i < num_objects; ++i) {
    object_ids[i] = from_flatbuf(message->object_ids()->Get(i));
  }
  *timeout_ms = message->timeout_ms();
}

int plasma_send_GetReply(int sock,
                         protocol_builder *B,
                         ObjectID object_ids[],
                         PlasmaObject plasma_objects[],
                         int64_t num_objects) {
  flatbuffers::FlatBufferBuilder fbb(FLATBUFFER_BUILDER_DEFAULT_SIZE);
  std::vector<PlasmaObjectSpec> objects;

  for (int i = 0; i < num_objects; ++i) {
    PlasmaObject *object = &plasma_objects[i];
    objects.push_back(PlasmaObjectSpec(
        object->handle.store_fd, object->handle.mmap_size, object->data_offset,
        object->data_size, object->metadata_offset, object->metadata_size));
  }
  auto message = CreatePlasmaGetReply(
      fbb, to_flatbuf(fbb, object_ids, num_objects),
      fbb.CreateVectorOfStructs(objects.data(), num_objects));
  fbb.Finish(message);
  return write_message(sock, MessageType_PlasmaGetReply, fbb.GetSize(),
                       fbb.GetBufferPointer());
}

void plasma_read_GetReply(uint8_t *data,
                          ObjectID object_ids[],
                          PlasmaObject plasma_objects[],
                          int64_t num_objects) {
  CHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaGetReply>(data);
  for (int64_t i = 0; i < num_objects; ++i) {
    object_ids[i] = from_flatbuf(message->object_ids()->Get(i));
  }
  for (int64_t i = 0; i < num_objects; ++i) {
    const PlasmaObjectSpec *object = message->plasma_objects()->Get(i);
    plasma_objects[i].handle.store_fd = object->segment_index();
    plasma_objects[i].handle.mmap_size = object->mmap_size();
    plasma_objects[i].data_offset = object->data_offset();
    plasma_objects[i].data_size = object->data_size();
    plasma_objects[i].metadata_offset = object->metadata_offset();
    plasma_objects[i].metadata_size = object->metadata_size();
  }
}

/* Fetch messages. */

int plasma_send_FetchRequest(int sock,
                             protocol_builder *B,
                             ObjectID object_ids[],
                             int64_t num_objects) {
  flatbuffers::FlatBufferBuilder fbb(FLATBUFFER_BUILDER_DEFAULT_SIZE);
  auto message =
      CreatePlasmaFetchRequest(fbb, to_flatbuf(fbb, object_ids, num_objects));
  fbb.Finish(message);
  return write_message(sock, MessageType_PlasmaFetchRequest, fbb.GetSize(),
                       fbb.GetBufferPointer());
}

int64_t plasma_read_FetchRequest_num_objects(uint8_t *data) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaFetchRequest>(data);
  return message->object_ids()->size();
}

void plasma_read_FetchRequest(uint8_t *data,
                              ObjectID object_ids[],
                              int64_t num_objects) {
  CHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaFetchRequest>(data);
  for (int64_t i = 0; i < num_objects; ++i) {
    object_ids[i] = from_flatbuf(message->object_ids()->Get(i));
  }
}

/* Wait messages. */

int plasma_send_WaitRequest(int sock,
                            protocol_builder *B,
                            ObjectRequest object_requests[],
                            int num_requests,
                            int num_ready_objects,
                            int64_t timeout_ms) {
  flatbuffers::FlatBufferBuilder fbb(FLATBUFFER_BUILDER_DEFAULT_SIZE);

  std::vector<flatbuffers::Offset<ObjectRequestSpec>> object_request_specs;
  for (int i = 0; i < num_requests; i++) {
    object_request_specs.push_back(CreateObjectRequestSpec(
        fbb, to_flatbuf(fbb, object_requests[i].object_id),
        object_requests[i].type));
  }

  auto message =
      CreatePlasmaWaitRequest(fbb, fbb.CreateVector(object_request_specs),
                              num_ready_objects, timeout_ms);
  fbb.Finish(message);
  return write_message(sock, MessageType_PlasmaWaitRequest, fbb.GetSize(),
                       fbb.GetBufferPointer());
}

int plasma_read_WaitRequest_num_object_ids(uint8_t *data) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaWaitRequest>(data);
  return message->object_requests()->size();
}

void plasma_read_WaitRequest(uint8_t *data,
                             ObjectRequest object_requests[],
                             int num_object_ids,
                             int64_t *timeout_ms,
                             int *num_ready_objects) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaWaitRequest>(data);
  *num_ready_objects = message->num_ready_objects();
  *timeout_ms = message->timeout();

  CHECK(num_object_ids == message->object_requests()->size());
  for (int i = 0; i < num_object_ids; i++) {
    object_requests[i].object_id =
        from_flatbuf(message->object_requests()->Get(i)->object_id());
    object_requests[i].type = message->object_requests()->Get(i)->type();
    /* Initialize the object request status to nonexistent. */
    object_requests[i].status = ObjectStatus_Nonexistent;
  }
}

int plasma_send_WaitReply(int sock,
                          protocol_builder *B,
                          const std::unordered_map<ObjectID, ObjectRequest, decltype(&hashObjectID)> &object_requests,
//                          const std::unordered_set<ObjectRequest> &object_requests,
//                          ObjectRequest object_requests[],
                          int num_ready_objects) {
  flatbuffers::FlatBufferBuilder fbb(FLATBUFFER_BUILDER_DEFAULT_SIZE);

  std::vector<flatbuffers::Offset<ObjectReply>> object_replies;
  for (const auto &objreq_pair : object_requests) {
    const auto &object_request = objreq_pair.second;
//  for (int i = 0; i < num_ready_objects; i++) {
    object_replies.push_back(
//        CreateObjectReply(fbb, to_flatbuf(fbb, object_requests[i].object_id), object_requests[i].status));
    CreateObjectReply(fbb,
        to_flatbuf(fbb, object_request.object_id), object_request.status));

  }

  auto message = CreatePlasmaWaitReply(
      fbb, fbb.CreateVector(object_replies.data(), num_ready_objects),
      num_ready_objects);
  fbb.Finish(message);
  return write_message(sock, MessageType_PlasmaWaitReply, fbb.GetSize(),
                       fbb.GetBufferPointer());
}

void plasma_read_WaitReply(uint8_t *data,
                           ObjectRequest object_requests[],
                           int *num_ready_objects) {
  DCHECK(data);

  auto message = flatbuffers::GetRoot<PlasmaWaitReply>(data);
  *num_ready_objects = message->num_ready_objects();
  for (int i = 0; i < *num_ready_objects; i++) {
    object_requests[i].object_id =
        from_flatbuf(message->object_requests()->Get(i)->object_id());
    object_requests[i].status = message->object_requests()->Get(i)->status();
  }
}

/* Subscribe messages. */

int plasma_send_SubscribeRequest(int sock, protocol_builder *B) {
  flatbuffers::FlatBufferBuilder fbb(FLATBUFFER_BUILDER_DEFAULT_SIZE);
  auto message = CreatePlasmaSubscribeRequest(fbb);
  fbb.Finish(message);
  return write_message(sock, MessageType_PlasmaSubscribeRequest, fbb.GetSize(),
                       fbb.GetBufferPointer());
}

/* Data messages. */

int plasma_send_DataRequest(int sock,
                            protocol_builder *B,
                            ObjectID object_id,
                            const char *address,
                            int port) {
  flatbuffers::FlatBufferBuilder fbb(FLATBUFFER_BUILDER_DEFAULT_SIZE);
  auto addr = fbb.CreateString((char *) address, strlen(address));
  auto message =
      CreatePlasmaDataRequest(fbb, to_flatbuf(fbb, object_id), addr, port);
  fbb.Finish(message);
  return write_message(sock, MessageType_PlasmaDataRequest, fbb.GetSize(),
                       fbb.GetBufferPointer());
}

void plasma_read_DataRequest(uint8_t *data,
                             ObjectID *object_id,
                             char **address,
                             int *port) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaDataRequest>(data);
  DCHECK(message->object_id()->size() == sizeof(object_id->id));
  *object_id = from_flatbuf(message->object_id());
  *address = strdup(message->address()->c_str());
  *port = message->port();
}

int plasma_send_DataReply(int sock,
                          protocol_builder *B,
                          ObjectID object_id,
                          int64_t object_size,
                          int64_t metadata_size) {
  flatbuffers::FlatBufferBuilder fbb(FLATBUFFER_BUILDER_DEFAULT_SIZE);
  auto message = CreatePlasmaDataReply(fbb, to_flatbuf(fbb, object_id),
                                       object_size, metadata_size);
  fbb.Finish(message);
  return write_message(sock, MessageType_PlasmaDataReply, fbb.GetSize(),
                       fbb.GetBufferPointer());
}

void plasma_read_DataReply(uint8_t *data,
                           ObjectID *object_id,
                           int64_t *object_size,
                           int64_t *metadata_size) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaDataReply>(data);
  *object_id = from_flatbuf(message->object_id());
  *object_size = (int64_t) message->object_size();
  *metadata_size = (int64_t) message->metadata_size();
}
