#include <assert.h>

#include "plasma_protocol.h"
#include "io.h"

#include "format/plasma_builder.h"
#include "plasma.h"

protocol_builder *make_protocol_builder(void) {
  return NULL;
}

void free_protocol_builder(protocol_builder *builder) {
}
#if 0
#define FLATBUFFER_BUILDER_DEFAULT_SIZE 1024

/**
 * Writes an array of object IDs into a flatbuffer buffer and return
 * the resulting vector.
 *
 * @params B Pointer to the flatbuffer builder.
 * @param object_ids Array of object IDs to be written.
 * @param num_objects The number of elements in the array.
 * @return Reference to the flatbuffer string vector.
 */
flatbuffers_string_vec_ref_t object_ids_to_flatbuffer(flatcc_builder_t *B,
                                                      ObjectID object_ids[],
                                                      int64_t num_objects) {
  flatbuffers_string_vec_start(B);
  for (int i = 0; i < num_objects; i++) {
    flatbuffers_string_ref_t id = flatbuffers_string_create(
        B, (const char *) &object_ids[i].id[0], sizeof(object_ids[i].id));
    flatbuffers_string_vec_push(B, id);
  }
  return flatbuffers_string_vec_end(B);
}

/**
 * Reads an array of object IDs from a flatbuffer vector.
 *
 * @param object_id_vector Flatbuffer vector containing object IDs.
 * @param object_ids_ptr Pointer to array that will contain the object IDs. The
 *        array is allocated by this function and must be freed by the user.
 * @param num_objects Pointer to the number of objects, will be written by this
 *        method.
 * @return Void.
 */
void object_ids_from_flatbuffer(flatbuffers_string_vec_t object_id_vector,
                                ObjectID object_ids[],
                                int64_t num_objects) {
  CHECK(flatbuffers_string_vec_len(object_id_vector) == num_objects);
  for (int64_t i = 0; i < num_objects; ++i) {
    memcpy(&object_ids[i].id[0], flatbuffers_string_vec_at(object_id_vector, i),
           sizeof(object_ids[i].id));
  }
}

/**
 * Finalize the flatbuffers and write a message with the result to a
 * file descriptor.
 *
 * @param B Pointer to the flatbuffer builder.
 * @param fd File descriptor the message gets written to.
 * @param message_type Type of the message that is written.
 * @return Whether there was an error while writing. 0 corresponds to success
 *         and -1 corresponds to an error (errno will be set).
 */
int finalize_buffer_and_send(flatcc_builder_t *B, int fd, int message_type) {
  size_t size;
  void *buff = flatcc_builder_finalize_buffer(B, &size);
  int r = write_message(fd, message_type, size, buff);
  free(buff);
  if (!(message_type == MessageType_PlasmaCreateRequest || message_type == MessageType_PlasmaSealRequest)) {
    printf("Bad things happening\n");
    flatcc_builder_reset(B);
  }
  return r;
}

uint8_t *plasma_receive(int sock, int64_t message_type) {
  int64_t type;
  int64_t length;
  uint8_t *reply_data;
  read_message(sock, &type, &length, &reply_data);
  CHECKM(type == message_type, "type = %" PRId64 ", message_type = %" PRId64,
         type, message_type);
  return reply_data;
}

#if 0
int plasma_send_CreateRequest(int sock,
                              protocol_builder *B,
                              ObjectID object_id,
                              int64_t data_size,
                              int64_t metadata_size) {
  PlasmaCreateRequest_start_as_root(B);
  PlasmaCreateRequest_object_id_create(B, (const char *) &object_id.id[0],
                                       sizeof(object_id.id));
  PlasmaCreateRequest_data_size_add(B, data_size);
  PlasmaCreateRequest_metadata_size_add(B, metadata_size);
  PlasmaCreateRequest_end_as_root(B);
  return finalize_buffer_and_send(B, sock, MessageType_PlasmaCreateRequest);
}

void plasma_read_CreateRequest(uint8_t *data,
                               ObjectID *object_id,
                               int64_t *data_size,
                               int64_t *metadata_size) {
  DCHECK(data);
  PlasmaCreateRequest_table_t req = PlasmaCreateRequest_as_root(data);
  *data_size = PlasmaCreateRequest_data_size(req);
  *metadata_size = PlasmaCreateRequest_metadata_size(req);
  flatbuffers_string_t id = PlasmaCreateRequest_object_id(req);
  DCHECK(flatbuffers_string_len(id) == sizeof(object_id->id));
  memcpy(&object_id->id[0], id, sizeof(object_id->id));
}

int plasma_send_CreateReply(int sock,
                            protocol_builder *B,
                            ObjectID object_id,
                            PlasmaObject *object,
                            int error_code) {
  PlasmaCreateReply_start_as_root(B);
  PlasmaCreateReply_object_id_create(B, (const char *) &object_id.id[0],
                                     sizeof(object_id.id));
  PlasmaCreateReply_plasma_object_create(
      B, object->handle.store_fd, object->handle.mmap_size, object->data_offset,
      object->data_size, object->metadata_offset, object->metadata_size);
  PlasmaCreateReply_error_add(B, error_code);
  PlasmaCreateReply_end_as_root(B);
  return finalize_buffer_and_send(B, sock, MessageType_PlasmaCreateReply);
}

void plasma_read_CreateReply(uint8_t *data,
                             ObjectID *object_id,
                             PlasmaObject *object,
                             int *error_code) {
  DCHECK(data);
  PlasmaCreateReply_table_t rep = PlasmaCreateReply_as_root(data);
  flatbuffers_string_t id = PlasmaCreateReply_object_id(rep);
  CHECK(flatbuffers_string_len(id) == sizeof(object_id->id));
  memcpy(&object_id->id[0], id, sizeof(object_id->id));
  PlasmaObjectSpec_struct_t obj = PlasmaCreateReply_plasma_object(rep);
  object->handle.store_fd = PlasmaObjectSpec_segment_index(obj);
  object->handle.mmap_size = PlasmaObjectSpec_mmap_size(obj);
  object->data_offset = PlasmaObjectSpec_data_offset(obj);
  object->data_size = PlasmaObjectSpec_data_size(obj);
  object->metadata_offset = PlasmaObjectSpec_metadata_offset(obj);
  object->metadata_size = PlasmaObjectSpec_metadata_size(obj);
  *error_code = PlasmaCreateReply_error(rep);
}
#endif

#define DEFINE_SIMPLE_SEND_REQUEST(MESSAGE_NAME)                       \
  int plasma_send_##MESSAGE_NAME(int sock, protocol_builder *B,        \
                                 ObjectID object_id) {                 \
    Plasma##MESSAGE_NAME##_start_as_root(B);                           \
    Plasma##MESSAGE_NAME##_object_id_create(                           \
        B, (const char *) &object_id.id[0], sizeof(object_id.id));     \
    Plasma##MESSAGE_NAME##_end_as_root(B);                             \
    return finalize_buffer_and_send(B, sock,                           \
                                    MessageType_Plasma##MESSAGE_NAME); \
  }

#define DEFINE_SIMPLE_READ_REQUEST(MESSAGE_NAME)                               \
  void plasma_read_##MESSAGE_NAME(uint8_t *data, ObjectID *object_id) {        \
    DCHECK(data);                                                              \
    Plasma##MESSAGE_NAME##_table_t req = Plasma##MESSAGE_NAME##_as_root(data); \
    flatbuffers_string_t id = Plasma##MESSAGE_NAME##_object_id(req);           \
    CHECK(flatbuffers_string_len(id) == sizeof(object_id->id));                \
    memcpy(&object_id->id[0], id, sizeof(object_id->id));                      \
  }

#define DEFINE_SIMPLE_SEND_REPLY(MESSAGE_NAME)                         \
  int plasma_send_##MESSAGE_NAME(int sock, protocol_builder *B,        \
                                 ObjectID object_id, int error) {      \
    Plasma##MESSAGE_NAME##_start_as_root(B);                           \
    Plasma##MESSAGE_NAME##_object_id_create(                           \
        B, (const char *) &object_id.id[0], sizeof(object_id.id));     \
    Plasma##MESSAGE_NAME##_error_add(B, error);                        \
    Plasma##MESSAGE_NAME##_end_as_root(B);                             \
    return finalize_buffer_and_send(B, sock,                           \
                                    MessageType_Plasma##MESSAGE_NAME); \
  }

#define DEFINE_SIMPLE_READ_REPLY(MESSAGE_NAME)                                 \
  void plasma_read_##MESSAGE_NAME(uint8_t *data, ObjectID *object_id,          \
                                  int *error) {                                \
    DCHECK(data);                                                              \
    Plasma##MESSAGE_NAME##_table_t req = Plasma##MESSAGE_NAME##_as_root(data); \
    flatbuffers_string_t id = Plasma##MESSAGE_NAME##_object_id(req);           \
    CHECK(flatbuffers_string_len(id) == sizeof(object_id->id));                \
    memcpy(&object_id->id[0], id, sizeof(object_id->id));                      \
    *error = Plasma##MESSAGE_NAME##_error(req);                                \
  }

#if 0
int plasma_send_SealRequest(int sock,
                            protocol_builder *B,
                            ObjectID object_id,
                            unsigned char *digest) {
  PlasmaSealRequest_start_as_root(B);
  PlasmaSealRequest_object_id_create(B, (const char *) &object_id.id[0],
                                     sizeof(object_id.id));
  PlasmaSealRequest_digest_create(B, digest, DIGEST_SIZE);
  PlasmaSealRequest_end_as_root(B);
  return finalize_buffer_and_send(B, sock, MessageType_PlasmaSealRequest);
}

void plasma_read_SealRequest(uint8_t *data,
                             ObjectID *object_id,
                             unsigned char *digest) {
  DCHECK(data);
  PlasmaSealRequest_table_t req = PlasmaSealRequest_as_root(data);
  flatbuffers_string_t id = PlasmaSealRequest_object_id(req);
  CHECK(flatbuffers_string_len(id) == sizeof(object_id->id));
  memcpy(&object_id->id[0], id, sizeof(object_id->id));
  flatbuffers_uint8_vec_t d = PlasmaSealRequest_digest(req);
  CHECK(flatbuffers_uint8_vec_len(d) == DIGEST_SIZE);
  memcpy(digest, d, DIGEST_SIZE);
}

DEFINE_SIMPLE_SEND_REPLY(SealReply);
DEFINE_SIMPLE_READ_REPLY(SealReply);
#endif

// DEFINE_SIMPLE_SEND_REQUEST(ReleaseRequest);
// DEFINE_SIMPLE_READ_REQUEST(ReleaseRequest);
// DEFINE_SIMPLE_SEND_REPLY(ReleaseReply);
// DEFINE_SIMPLE_READ_REPLY(ReleaseReply);

// DEFINE_SIMPLE_SEND_REQUEST(DeleteRequest);
// DEFINE_SIMPLE_READ_REQUEST(DeleteRequest);
/*
DEFINE_SIMPLE_SEND_REPLY(DeleteReply);
DEFINE_SIMPLE_READ_REPLY(DeleteReply);
*/

/* Plasma status message. */

#if 0
int plasma_send_StatusRequest(int sock,
                              protocol_builder *B,
                              ObjectID object_ids[],
                              int64_t num_objects) {
  PlasmaStatusRequest_start_as_root(B);
  PlasmaStatusRequest_object_ids_add(
      B, object_ids_to_flatbuffer(B, object_ids, num_objects));
  PlasmaStatusRequest_end_as_root(B);
  return finalize_buffer_and_send(B, sock, MessageType_PlasmaStatusRequest);
}

int64_t plasma_read_StatusRequest_num_objects(uint8_t *data) {
  DCHECK(data);
  PlasmaStatusRequest_table_t req = PlasmaStatusRequest_as_root(data);
  return flatbuffers_string_vec_len(PlasmaStatusRequest_object_ids(req));
}

void plasma_read_StatusRequest(uint8_t *data,
                               ObjectID object_ids[],
                               int64_t num_objects) {
  DCHECK(data);
  PlasmaStatusRequest_table_t req = PlasmaStatusRequest_as_root(data);
  object_ids_from_flatbuffer(PlasmaStatusRequest_object_ids(req), object_ids,
                             num_objects);
}

int plasma_send_StatusReply(int sock,
                            protocol_builder *B,
                            ObjectID object_ids[],
                            int object_status[],
                            int64_t num_objects) {
  PlasmaStatusReply_start_as_root(B);
  PlasmaStatusReply_object_ids_add(
      B, object_ids_to_flatbuffer(B, object_ids, num_objects));
  flatbuffers_int32_vec_start(B);
  for (int64_t i = 0; i < num_objects; ++i) {
    flatbuffers_int32_vec_push(B, &object_status[i]);
  }
  PlasmaStatusReply_status_add(B, flatbuffers_int32_vec_end(B));
  PlasmaStatusReply_end_as_root(B);
  return finalize_buffer_and_send(B, sock, MessageType_PlasmaStatusReply);
}

int64_t plasma_read_StatusReply_num_objects(uint8_t *data) {
  DCHECK(data);
  PlasmaStatusReply_table_t req = PlasmaStatusReply_as_root(data);
  return flatbuffers_string_vec_len(PlasmaStatusReply_object_ids(req));
}

void plasma_read_StatusReply(uint8_t *data,
                             ObjectID object_ids[],
                             int object_status[],
                             int64_t num_objects) {
  DCHECK(data);
  PlasmaStatusReply_table_t rep = PlasmaStatusReply_as_root(data);
  object_ids_from_flatbuffer(PlasmaStatusReply_object_ids(rep), object_ids,
                             num_objects);
  for (int64_t i = 0; i < num_objects; ++i) {
    object_status[i] =
        flatbuffers_int32_vec_at(PlasmaStatusReply_status(rep), i);
  }
}

/* Plasma contains message. */

int plasma_send_ContainsRequest(int sock,
                                protocol_builder *B,
                                ObjectID object_id) {
  PlasmaContainsRequest_start_as_root(B);
  PlasmaContainsRequest_object_id_create(B, (const char *) &object_id.id[0],
                                         sizeof(object_id.id));
  PlasmaContainsRequest_end_as_root(B);
  return finalize_buffer_and_send(B, sock, MessageType_PlasmaContainsRequest);
}

void plasma_read_ContainsRequest(uint8_t *data, ObjectID *object_id) {
  DCHECK(data);
  PlasmaContainsRequest_table_t req = PlasmaContainsRequest_as_root(data);
  flatbuffers_string_t id = PlasmaContainsRequest_object_id(req);
  CHECK(flatbuffers_string_len(id) == sizeof(object_id->id));
  memcpy(&object_id->id[0], id, sizeof(object_id->id));
}

int plasma_send_ContainsReply(int sock,
                              protocol_builder *B,
                              ObjectID object_id,
                              int has_object) {
  PlasmaContainsReply_start_as_root(B);
  PlasmaContainsReply_object_id_create(B, (const char *) &object_id.id[0],
                                       sizeof(object_id.id));
  PlasmaContainsReply_has_object_add(B, has_object);
  PlasmaContainsReply_end_as_root(B);
  return finalize_buffer_and_send(B, sock, MessageType_PlasmaContainsReply);
}

void plasma_read_ContainsReply(uint8_t *data,
                               ObjectID *object_id,
                               int *has_object) {
  DCHECK(data);
  PlasmaContainsReply_table_t rep = PlasmaContainsReply_as_root(data);
  flatbuffers_string_t id = PlasmaContainsReply_object_id(rep);
  CHECK(flatbuffers_string_len(id) == sizeof(object_id->id));
  memcpy(&object_id->id[0], id, sizeof(object_id->id));
  *has_object = PlasmaContainsReply_has_object(rep);
}

/* Plasma connect message. */

int plasma_send_ConnectRequest(int sock, protocol_builder *B) {
  PlasmaConnectRequest_start_as_root(B);
  PlasmaConnectRequest_end_as_root(B);
  return finalize_buffer_and_send(B, sock, MessageType_PlasmaConnectRequest);
}

void plasma_read_ConnectRequest(uint8_t *data) {
  DCHECK(data);
  PlasmaConnectRequest_table_t req = PlasmaConnectRequest_as_root(data);
}

int plasma_send_ConnectReply(int sock,
                             protocol_builder *B,
                             int64_t memory_capacity) {
  PlasmaConnectReply_start_as_root(B);
  PlasmaConnectReply_memory_capacity_add(B, memory_capacity);
  PlasmaConnectReply_end_as_root(B);
  return finalize_buffer_and_send(B, sock, MessageType_PlasmaConnectReply);
}

void plasma_read_ConnectReply(uint8_t *data, int64_t *memory_capacity) {
  DCHECK(data);
  PlasmaConnectReply_table_t rep = PlasmaConnectReply_as_root(data);
  *memory_capacity = PlasmaConnectReply_memory_capacity(rep);
}

/* Plasma evict message. */

int plasma_send_EvictRequest(int sock, protocol_builder *B, int64_t num_bytes) {
  PlasmaEvictRequest_start_as_root(B);
  PlasmaEvictRequest_num_bytes_add(B, num_bytes);
  PlasmaEvictRequest_end_as_root(B);
  return finalize_buffer_and_send(B, sock, MessageType_PlasmaEvictRequest);
}

void plasma_read_EvictRequest(uint8_t *data, int64_t *num_bytes) {
  DCHECK(data);
  PlasmaEvictRequest_table_t req = PlasmaEvictRequest_as_root(data);
  *num_bytes = PlasmaEvictRequest_num_bytes(req);
}

int plasma_send_EvictReply(int sock, protocol_builder *B, int64_t num_bytes) {
  PlasmaEvictReply_start_as_root(B);
  PlasmaEvictReply_num_bytes_add(B, num_bytes);
  PlasmaEvictReply_end_as_root(B);
  return finalize_buffer_and_send(B, sock, MessageType_PlasmaEvictReply);
}

void plasma_read_EvictReply(uint8_t *data, int64_t *num_bytes) {
  DCHECK(data);
  PlasmaEvictReply_table_t req = PlasmaEvictReply_as_root(data);
  *num_bytes = PlasmaEvictReply_num_bytes(req);
}

/* Plasma Get message. */

int plasma_send_GetRequest(int sock,
                           protocol_builder *B,
                           ObjectID object_ids[],
                           int64_t num_objects,
                           int64_t timeout_ms) {
  PlasmaGetRequest_start_as_root(B);
  PlasmaGetRequest_object_ids_add(
      B, object_ids_to_flatbuffer(B, object_ids, num_objects));
  PlasmaGetRequest_timeout_ms_add(B, timeout_ms);
  PlasmaGetRequest_end_as_root(B);
  return finalize_buffer_and_send(B, sock, MessageType_PlasmaGetRequest);
}

int64_t plasma_read_GetRequest_num_objects(uint8_t *data) {
  DCHECK(data);
  PlasmaGetRequest_table_t req = PlasmaGetRequest_as_root(data);
  return flatbuffers_string_vec_len(PlasmaGetRequest_object_ids(req));
}

void plasma_read_GetRequest(uint8_t *data,
                            ObjectID object_ids[],
                            int64_t *timeout_ms,
                            int64_t num_objects) {
  DCHECK(data);
  PlasmaGetRequest_table_t req = PlasmaGetRequest_as_root(data);
  flatbuffers_string_vec_t object_id_vector = PlasmaGetRequest_object_ids(req);
  object_ids_from_flatbuffer(object_id_vector, object_ids, num_objects);
  *timeout_ms = PlasmaGetRequest_timeout_ms(req);
}

int plasma_send_GetReply(int sock,
                         protocol_builder *B,
                         ObjectID object_ids[],
                         PlasmaObject plasma_objects[],
                         int64_t num_objects) {
  PlasmaGetReply_start_as_root(B);

  flatbuffers_string_vec_ref_t ids =
      object_ids_to_flatbuffer(B, object_ids, num_objects);
  PlasmaGetReply_object_ids_add(B, ids);

  PlasmaObjectSpec_vec_start(B);
  for (int i = 0; i < num_objects; ++i) {
    PlasmaObject obj = plasma_objects[i];
    PlasmaObjectSpec_t plasma_obj;
    memset(&plasma_obj, 0, sizeof(PlasmaObjectSpec_t));
    plasma_obj.segment_index = obj.handle.store_fd;
    plasma_obj.mmap_size = obj.handle.mmap_size;
    plasma_obj.data_offset = obj.data_offset;
    plasma_obj.data_size = obj.data_size;
    plasma_obj.metadata_offset = obj.metadata_offset;
    plasma_obj.metadata_size = obj.metadata_size;
    PlasmaObjectSpec_vec_push(B, &plasma_obj);
  }
  PlasmaObjectSpec_vec_ref_t object_vec = PlasmaObjectSpec_vec_end(B);
  PlasmaGetReply_plasma_objects_add(B, object_vec);
  PlasmaGetReply_end_as_root(B);
  return finalize_buffer_and_send(B, sock, MessageType_PlasmaGetReply);
}

void plasma_read_GetReply(uint8_t *data,
                          ObjectID object_ids[],
                          PlasmaObject plasma_objects[],
                          int64_t num_objects) {
  CHECK(data);
  PlasmaGetReply_table_t req = PlasmaGetReply_as_root(data);
  flatbuffers_string_vec_t object_id_vector = PlasmaGetReply_object_ids(req);
  object_ids_from_flatbuffer(object_id_vector, object_ids, num_objects);

  memset(plasma_objects, 0, sizeof(PlasmaObject) * num_objects);
  PlasmaObjectSpec_vec_t plasma_objects_vector =
      PlasmaGetReply_plasma_objects(req);

  for (int i = 0; i < num_objects; ++i) {
    PlasmaObjectSpec_struct_t obj =
        PlasmaObjectSpec_vec_at(plasma_objects_vector, i);
    plasma_objects[i].handle.store_fd = PlasmaObjectSpec_segment_index(obj);
    plasma_objects[i].handle.mmap_size = PlasmaObjectSpec_mmap_size(obj);
    plasma_objects[i].data_offset = PlasmaObjectSpec_data_offset(obj);
    plasma_objects[i].data_size = PlasmaObjectSpec_data_size(obj);
    plasma_objects[i].metadata_offset = PlasmaObjectSpec_metadata_offset(obj);
    plasma_objects[i].metadata_size = PlasmaObjectSpec_metadata_size(obj);
  }
}

/* Plasma fetch messages. */

int plasma_send_FetchRequest(int sock,
                             protocol_builder *B,
                             ObjectID object_ids[],
                             int64_t num_objects) {
  PlasmaFetchRequest_start_as_root(B);
  PlasmaFetchRequest_object_ids_add(
      B, object_ids_to_flatbuffer(B, object_ids, num_objects));
  PlasmaFetchRequest_end_as_root(B);
  return finalize_buffer_and_send(B, sock, MessageType_PlasmaFetchRequest);
}

int64_t plasma_read_FetchRequest_num_objects(uint8_t *data) {
  DCHECK(data);
  PlasmaFetchRequest_table_t req = PlasmaFetchRequest_as_root(data);
  return ObjectRequest_vec_len(PlasmaFetchRequest_object_ids(req));
}

void plasma_read_FetchRequest(uint8_t *data,
                              ObjectID object_ids[],
                              int64_t num_objects) {
  DCHECK(data);
  PlasmaFetchRequest_table_t req = PlasmaFetchRequest_as_root(data);
  flatbuffers_string_vec_t object_id_vector =
      PlasmaFetchRequest_object_ids(req);
  object_ids_from_flatbuffer(object_id_vector, object_ids, num_objects);
}

#endif

/* Plasma wait messages. */

int plasma_send_WaitRequest(int sock,
                            protocol_builder *B,
                            ObjectRequest object_requests[],
                            int num_requests,
                            int num_ready_objects,
                            int64_t timeout_ms) {
  PlasmaWaitRequest_start_as_root(B);
  ObjectRequest_vec_start(B);
  for (int i = 0; i < num_requests; ++i) {
    flatbuffers_string_ref_t id = flatbuffers_string_create(
        B, (const char *) &object_requests[i].object_id.id[0],
        sizeof(object_requests[i].object_id.id));
    ObjectRequest_vec_push_create(B, id, (int32_t) object_requests[i].type);
  }
  ObjectRequest_vec_ref_t objreq_vec = ObjectRequest_vec_end(B);
  PlasmaWaitRequest_object_requests_add(B, objreq_vec);
  PlasmaWaitRequest_num_ready_objects_add(B, num_ready_objects);
  PlasmaWaitRequest_timeout_add(B, timeout_ms);
  PlasmaWaitRequest_end_as_root(B);
  return finalize_buffer_and_send(B, sock, MessageType_PlasmaWaitRequest);
}

int plasma_read_WaitRequest_num_object_ids(uint8_t *data) {
  DCHECK(data);
  PlasmaWaitRequest_table_t req = PlasmaWaitRequest_as_root(data);
  return ObjectRequest_vec_len(PlasmaWaitRequest_object_requests(req));
}

void plasma_read_WaitRequest(uint8_t *data,
                             ObjectRequest object_requests[],
                             int num_object_ids,
                             int64_t *timeout_ms,
                             int *num_ready_objects) {
  DCHECK(data);
  PlasmaWaitRequest_table_t req = PlasmaWaitRequest_as_root(data);
  ObjectRequest_vec_t objreq_vec = PlasmaWaitRequest_object_requests(req);
  CHECK(num_object_ids == ObjectRequest_vec_len(objreq_vec));
  for (int i = 0; i < num_object_ids; i++) {
    ObjectRequest_table_t objreq = ObjectRequest_vec_at(objreq_vec, i);
    memcpy(&object_requests[i].object_id.id[0], ObjectRequest_object_id(objreq),
           sizeof(object_requests[i].object_id.id));
    object_requests[i].type = ObjectRequest_type(objreq);
  }
  *timeout_ms = PlasmaWaitRequest_timeout(req);
  *num_ready_objects = PlasmaWaitRequest_num_ready_objects(req);
}

int plasma_send_WaitReply(int sock,
                          protocol_builder *B,
                          ObjectRequest object_requests[],
                          int num_ready_objects) {
  PlasmaWaitReply_start_as_root(B);
  ObjectReply_vec_start(B);
  for (int i = 0; i < num_ready_objects; ++i) {
    flatbuffers_string_ref_t id = flatbuffers_string_create(
        B, (const char *) &object_requests[i].object_id.id[0],
        sizeof(object_requests[i].object_id.id));
    ObjectReply_vec_push_create(B, id, (int32_t) object_requests[i].status);
  }
  ObjectReply_vec_ref_t objreq_vec = ObjectReply_vec_end(B);
  PlasmaWaitReply_object_requests_add(B, objreq_vec);
  PlasmaWaitReply_num_ready_objects_add(B, num_ready_objects);
  PlasmaWaitReply_end_as_root(B);
  return finalize_buffer_and_send(B, sock, MessageType_PlasmaWaitReply);
}

void plasma_read_WaitReply(uint8_t *data,
                           ObjectRequest object_requests[],
                           int *num_ready_objects) {
  DCHECK(data);
  PlasmaWaitReply_table_t req = PlasmaWaitReply_as_root(data);
  ObjectReply_vec_t objreq_vec = PlasmaWaitReply_object_requests(req);
  /* TODO (ion): This is risky, maybe num_ready_objects should contain length of
   * object_request object_requests? */
  *num_ready_objects = ObjectReply_vec_len(objreq_vec);
  for (int i = 0; i < *num_ready_objects; i++) {
    ObjectReply_table_t objreq = ObjectReply_vec_at(objreq_vec, i);
    memcpy(&object_requests[i].object_id.id[0], ObjectReply_object_id(objreq),
           sizeof(object_requests[i].object_id.id));
    object_requests[i].status = ObjectReply_status(objreq);
  }
}

int plasma_send_SubscribeRequest(int sock, protocol_builder *B) {
  PlasmaSubscribeRequest_start_as_root(B);
  PlasmaSubscribeRequest_end_as_root(B);
  return finalize_buffer_and_send(B, sock, MessageType_PlasmaSubscribeRequest);
}

int plasma_send_DataRequest(int sock,
                            protocol_builder *B,
                            ObjectID object_id,
                            const char *address,
                            int port) {
  PlasmaDataRequest_start_as_root(B);
  PlasmaDataRequest_object_id_create(B, (const char *) &object_id.id[0],
                                     sizeof(object_id.id));
  PlasmaDataRequest_address_create(B, address, strlen(address));
  PlasmaDataRequest_port_add(B, port);
  PlasmaDataRequest_end_as_root(B);
  return finalize_buffer_and_send(B, sock, MessageType_PlasmaDataRequest);
}

void plasma_read_DataRequest(uint8_t *data,
                             ObjectID *object_id,
                             char **address,
                             int *port) {
  DCHECK(data);
  PlasmaDataRequest_table_t req = PlasmaDataRequest_as_root(data);
  flatbuffers_string_t id = PlasmaDataRequest_object_id(req);
  DCHECK(flatbuffers_string_len(id) == sizeof(object_id->id));
  memcpy(&object_id->id[0], id, sizeof(object_id->id));
  *address = strdup(PlasmaDataRequest_address(req));
  *port = PlasmaDataRequest_port(req);
}

int plasma_send_DataReply(int sock,
                          protocol_builder *B,
                          ObjectID object_id,
                          int64_t object_size,
                          int64_t metadata_size) {
  PlasmaDataReply_start_as_root(B);
  PlasmaDataReply_object_id_create(B, (const char *) &object_id.id[0],
                                   sizeof(object_id.id));
  PlasmaDataReply_object_size_add(B, object_size);
  PlasmaDataReply_metadata_size_add(B, metadata_size);
  PlasmaDataRequest_end_as_root(B);
  return finalize_buffer_and_send(B, sock, MessageType_PlasmaDataReply);
}

void plasma_read_DataReply(uint8_t *data,
                           ObjectID *object_id,
                           int64_t *object_size,
                           int64_t *metadata_size) {
  DCHECK(data);
  PlasmaDataReply_table_t rep = PlasmaDataReply_as_root(data);
  flatbuffers_string_t id = PlasmaDataReply_object_id(rep);
  DCHECK(flatbuffers_string_len(id) == sizeof(object_id->id));
  memcpy(&object_id->id[0], id, sizeof(object_id->id));
  *object_size = PlasmaDataReply_object_size(rep);
  *metadata_size = PlasmaDataReply_metadata_size(rep);
}
#endif
