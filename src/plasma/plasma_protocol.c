#include <assert.h>

#include "plasma_protocol.h"
#include "io.h"

#include "format/plasma_builder.h"
#include "plasma.h"

#define FLATBUFFER_BUILDER_DEFAULT_SIZE 1024

/* An argument to a function that a return value gets written to. */
#define OUT

/**
 * Writes an array of object IDs into a flatbuffer buffer and return
 * the resulting vector.
 *
 * @params B Pointer to the flatbuffer builder.
 * @param object_ids Array of object IDs to be written.
 * @param num_objectsnum_objects number of element in the array.
 *
 * @return Reference to the flatbuffer string vector.
 */
flatbuffers_string_vec_ref_t object_ids_to_flatbuffer(flatcc_builder_t *B,
                                                      object_id object_ids[],
                                                      int64_t num_objects) {
  flatbuffers_string_vec_start(B);
  for (int i = 0; i < num_objects; i++) {
    flatbuffers_string_ref_t id = flatbuffers_string_create(B, (const char *) &object_ids[i].id[0], UNIQUE_ID_SIZE);
    flatbuffers_string_vec_push(B, id);
  }
  return flatbuffers_string_vec_end(B);
}

/**
 * Reads an array of object IDs from a flatbuffer vector.
 * 
 * @param object_id_vector Flatbuffer vector containing object IDs.
 * @param object_ids_ptr Pointer to array that will contain the object IDs. The
 *                       array is allocated by this function and must be freed
 *                       by the user.
 * @param num_objects Pointer to the number of objects, will be written by
 *                    this method.
 * @return Void.
 */
void object_ids_from_flatbuffer(flatbuffers_string_vec_t object_id_vector,
                                OUT object_id **object_ids_ptr,
                                OUT int64_t *num_objects) {
  *num_objects = flatbuffers_string_vec_len(object_id_vector);
  if (*num_objects == 0) {
    *object_ids_ptr = NULL;
    *num_objects = 0;
  }
  *object_ids_ptr = malloc((*num_objects) * sizeof(object_id));
  object_id *object_ids = *object_ids_ptr;
  for (int i = 0; i < *num_objects; i++) {
    memcpy(&object_ids[i].id[0], flatbuffers_string_vec_at(object_id_vector, i), UNIQUE_ID_SIZE);
  }
}

/**
 * Finalize the flatbuffers and write a message with the result to a
 * file descriptor.
 *
 * @param B Pointer to the flatbuffer builder.
 * @param fd File descriptor the message gets written to.
 * @param message_type Type of the message that is written.
 *
 * @return Whether there was an error while writing. 0 corresponds to
 *         success and -1 corresponds to an error (errno will be set).
 */
int finalize_buffer_and_send(flatcc_builder_t *B, int fd, int message_type) {
  size_t size;
  void *buff = flatcc_builder_finalize_buffer(B, &size);
  int r = write_message(fd, PLASMA_PROTOCOL_VERSION, message_type, size, buff);
  free(buff);
  flatcc_builder_clear(B);
  return r;
}

/**
 * Functions to send and read an itegere.
 */

int send_data_int(int sock, int message_type, int value) {
  flatcc_builder_t builder;
  flatcc_builder_init(&builder);
  DataInt_start_as_root(&builder);
  DataInt_value_add(&builder, value);
  DataInt_end_as_root(&builder);
  return finalize_buffer_and_send(&builder, sock, message_type);
}

void read_data_int(uint8_t *data, int *value) {
  CHECK(data);
  DataInt_table_t req = DataInt_as_root(data);
  *value = DataInt_value(req);
}

/**
 * Functions to send and read an object id.
 */

int send_object_id(int sock, int message_type, object_id object_id) {
  flatcc_builder_t builder;
  flatcc_builder_init(&builder);
  ObjectId_start_as_root(&builder);
  ObjectId_object_id_create(&builder, (const char *)&object_id.id[0], UNIQUE_ID_SIZE);
  ObjectId_end_as_root(&builder);
  return finalize_buffer_and_send(&builder, sock, message_type);
}

void read_object_id(uint8_t *data, object_id *object_id) {
  CHECK(data);
  ObjectId_table_t req = ObjectId_as_root(data);
  flatbuffers_string_t id = ObjectId_object_id(req);
  CHECK(flatbuffers_string_len(id) == UNIQUE_ID_SIZE);
  memcpy(&object_id->id[0], id, UNIQUE_ID_SIZE);
}

/**
 * Functions to send and read an object id and its associated information.
 */

int send_object_id_and_info(int sock, int message_type, object_id object_id, int object_info) {
  flatcc_builder_t builder;
  flatcc_builder_init(&builder);
  ObjectIdAndInfo_start_as_root(&builder);
  ObjectIdAndInfo_object_id_create(&builder, (const char *)&object_id.id[0], UNIQUE_ID_SIZE);
  ObjectIdAndInfo_info_add(&builder, object_info);
  ObjectIdAndInfo_end_as_root(&builder);
  return finalize_buffer_and_send(&builder, sock, message_type);
}

void read_object_id_and_info(uint8_t *data, object_id *object_id, int *object_info) {
  CHECK(data);
  ObjectIdAndInfo_table_t req = ObjectIdAndInfo_as_root(data);
  flatbuffers_string_t id = ObjectIdAndInfo_object_id(req);
  CHECK(flatbuffers_string_len(id) == UNIQUE_ID_SIZE);
  memcpy(&object_id->id[0], id, UNIQUE_ID_SIZE);
  *object_info = ObjectIdAndInfo_info(req);
}

/**
 * Functions to send and read an array of object ids.
 */

int send_object_ids(int sock,
                    int message_type,
                    object_id object_ids[],
                    int64_t num_objects) {
  flatcc_builder_t builder;
  flatcc_builder_init(&builder);
  ObjectIds_start_as_root(&builder);

  flatbuffers_string_vec_ref_t ids = object_ids_to_flatbuffer(&builder, object_ids, num_objects);
  ObjectIds_object_ids_add(&builder, ids);

  ObjectIds_end_as_root(&builder);
  return finalize_buffer_and_send(&builder, sock, message_type);
}

void read_object_ids(uint8_t *data,
                     object_id** object_ids_ptr,
                     int64_t *num_objects) {
  CHECK(data);
  ObjectIds_table_t req = ObjectIds_as_root(data);
  flatbuffers_string_vec_t object_id_vector = ObjectIds_object_ids(req);
  object_ids_from_flatbuffer(object_id_vector, object_ids_ptr, num_objects);
}

int send_object_ids_and_infos(int sock,
                              int message_type,
                              object_id object_ids[],
                              int32_t object_infos[],
                              int64_t num_objects) {
  flatcc_builder_t builder;
  flatcc_builder_init(&builder);
  ObjectIdsAndInfos_start_as_root(&builder);
  // Add object IDs.
  flatbuffers_string_vec_ref_t ids = object_ids_to_flatbuffer(&builder, object_ids, num_objects);
  ObjectIdsAndInfos_object_ids_add(&builder, ids);
  // Add info for each object.
  flatbuffers_int32_vec_ref_t object_info_vector = flatbuffers_int32_vec_create(&builder, object_infos, num_objects);
  ObjectIdsAndInfos_object_infos_add(&builder, object_info_vector);
  ObjectIdsAndInfos_end_as_root(&builder);
  return finalize_buffer_and_send(&builder, sock, message_type);
}

void read_object_ids_and_infos(uint8_t *data,
                               object_id** object_ids_ptr,
                               int32_t **object_infos_ptr,
                               int64_t *num_objects) {
  CHECK(data);
  ObjectIdsAndInfos_table_t req = ObjectIdsAndInfos_as_root(data);
  // Get object IDs and number of object IDs.
  flatbuffers_string_vec_t object_id_vector = ObjectIdsAndInfos_object_ids(req);
  object_ids_from_flatbuffer(object_id_vector, object_ids_ptr, num_objects);
  // Get info associated to the object IDs.
  flatbuffers_int32_vec_t object_info_vector = ObjectIdsAndInfos_object_infos(req);
  assert((*num_objects) == flatbuffers_int32_vec_len(object_info_vector));
  *object_infos_ptr = calloc(*num_objects, sizeof(int32_t));
  assert(object_infos_ptr);
  for (int i = 0; i < *num_objects; ++i) {
    (*object_infos_ptr)[i] = flatbuffers_int32_vec_at(object_info_vector, i);
  }
}

int send_object_requests(int sock, int message_type, object_request object_requests[], int num_requests) {
  flatcc_builder_t builder;
  flatcc_builder_init(&builder);
  ObjectRequests_start_as_root(&builder);

  ObjectRequest_vec_start(&builder);
  for (int i = 0; i < num_requests; ++i) {
    flatbuffers_string_ref_t id =
            flatbuffers_string_create(&builder, (const char *) &object_requests[i].object_id.id[0], UNIQUE_ID_SIZE);
    ObjectRequest_vec_push_create(&builder, id, (int32_t)object_requests[i].type, (int32_t)object_requests[i].status);
  }
  ObjectRequest_vec_ref_t objreq_vec = ObjectRequest_vec_end(&builder);
  ObjectRequests_object_requests_add(&builder, objreq_vec);
  ObjectRequests_end_as_root(&builder);
  return finalize_buffer_and_send(&builder, sock, message_type);
}

void read_object_requests(uint8_t *data,
                          object_request **object_requests_ptr,
                          int *num_requests) {
  CHECK(data);
  ObjectRequests_table_t req = ObjectRequests_as_root(data);
  ObjectRequest_vec_t objreq_vec = ObjectRequests_object_requests(req);
  // TODO (ion): This is risky, maybe num_ready_objects should contain length of object_request object_requests?
  *num_requests = ObjectRequest_vec_len(objreq_vec);
  *object_requests_ptr = calloc(*num_requests, sizeof(object_request));
  object_request *object_requests = *object_requests_ptr;
  for (int i = 0; i < *num_requests; i++) {
    ObjectRequest_table_t objreq = ObjectRequest_vec_at(objreq_vec, i);
    memcpy(&object_requests[i].object_id.id[0], ObjectRequest_id(objreq), UNIQUE_ID_SIZE);
    object_requests[i].type = ObjectRequest_type(objreq);
    object_requests[i].status = ObjectRequest_status(objreq);
  }
}

int plasma_send_create_request(int sock,
                               object_id object_id,
                               int64_t data_size,
                               int64_t metadata_size) {
  flatcc_builder_t builder;
  flatcc_builder_init(&builder);
  PlasmaCreateRequest_start_as_root(&builder);
  PlasmaCreateRequest_object_id_create(&builder, (const char *)&object_id.id[0], UNIQUE_ID_SIZE);
  PlasmaCreateRequest_data_size_add(&builder, data_size);
  PlasmaCreateRequest_metadata_size_add(&builder, metadata_size);
  PlasmaCreateRequest_end_as_root(&builder);
  return finalize_buffer_and_send(&builder, sock, MessageType_PlasmaCreateRequest);
}

void plasma_read_create_request(uint8_t *data,
                                object_id *object_id,
                                int64_t *data_size,
                                int64_t *metadata_size) {
  CHECK(data);
	PlasmaCreateRequest_table_t req = PlasmaCreateRequest_as_root(data);
  *data_size = PlasmaCreateRequest_data_size(req);
  *metadata_size = PlasmaCreateRequest_metadata_size(req);
  flatbuffers_string_t id = PlasmaCreateRequest_object_id(req);
  CHECK(flatbuffers_string_len(id) == UNIQUE_ID_SIZE);
  memcpy(&object_id->id[0], id, UNIQUE_ID_SIZE);
}

int plasma_send_create_reply(int sock,
                             object_id object_id,
                             plasma_object *object,
                             int error_code) {
  flatcc_builder_t builder;
  flatcc_builder_init(&builder);
  PlasmaCreateReply_start_as_root(&builder);
  PlasmaCreateReply_object_id_create(&builder, (const char *)&object_id.id[0], UNIQUE_ID_SIZE);
  PlasmaCreateReply_plasma_object_create(&builder,
                                         object->handle.store_fd,
                                         object->handle.mmap_size,
                                         object->data_offset,
                                         object->data_size,
                                         object->metadata_offset,
                                         object->metadata_size);
  PlasmaCreateReply_error_add(&builder, error_code);
  PlasmaCreateReply_end_as_root(&builder);
  return finalize_buffer_and_send(&builder, sock, MessageType_PlasmaCreateReply);
}


void plasma_read_create_reply(uint8_t *data,
                              object_id *object_id,
                              plasma_object *object,
                              int *error_code) {
  CHECK(data);
  PlasmaCreateReply_table_t rep = PlasmaCreateReply_as_root(data);
  flatbuffers_string_t id = PlasmaCreateReply_object_id(rep);
  CHECK(flatbuffers_string_len(id) == UNIQUE_ID_SIZE);
  memcpy(&object_id->id[0], id, UNIQUE_ID_SIZE);
  PlasmaObject_struct_t obj = PlasmaCreateReply_plasma_object(rep);
  object->handle.store_fd = PlasmaObject_segment_index(obj);
  object->handle.mmap_size = PlasmaObject_mmap_size(obj);
  object->data_offset = PlasmaObject_data_offset(obj);
  object->data_size = PlasmaObject_data_size(obj);
  object->metadata_offset = PlasmaObject_metadata_offset(obj);
  object->metadata_size = PlasmaObject_metadata_size(obj);
  *error_code = PlasmaCreateReply_error(rep);
}

int plasma_send_get_local_reply(int sock,
                                object_id object_ids[],
                                plasma_object plasma_objects[],
                                int64_t num_objects) {
  flatcc_builder_t builder;
  flatcc_builder_init(&builder);
  PlasmaGetLocalReply_start_as_root(&builder);

  flatbuffers_string_vec_ref_t ids = object_ids_to_flatbuffer(&builder, object_ids, num_objects);
  PlasmaGetLocalReply_object_ids_add(&builder, ids);

  PlasmaObject_vec_start(&builder);
  for (int i = 0; i < num_objects; ++i) {
    plasma_object obj = plasma_objects[i];
    PlasmaObject_t plasma_obj;
    memset(&plasma_obj, 0, sizeof(PlasmaObject_t));
    plasma_obj.segment_index = obj.handle.store_fd;
    plasma_obj.mmap_size = obj.handle.mmap_size;
    plasma_obj.data_offset = obj.data_offset;
    plasma_obj.data_size = obj.data_size;
    plasma_obj.metadata_offset = obj.metadata_offset;
    plasma_obj.metadata_size = obj.metadata_size;
    PlasmaObject_vec_push(&builder, &plasma_obj);
  }
  PlasmaObject_vec_ref_t object_vec = PlasmaObject_vec_end(&builder);
  PlasmaGetLocalReply_plasma_objects_add(&builder, object_vec);
  PlasmaGetLocalReply_end_as_root(&builder);
  return finalize_buffer_and_send(&builder, sock, MessageType_PlasmaGetLocalReply);
}

void plasma_read_get_local_reply(uint8_t *data,
                                 object_id** object_ids_ptr,
                                 plasma_object plasma_objects[],
                                 int64_t *num_objects) {
  CHECK(data);
  PlasmaGetLocalReply_table_t req = PlasmaGetLocalReply_as_root(data);
  flatbuffers_string_vec_t object_id_vector = PlasmaGetLocalReply_object_ids(req);
  object_ids_from_flatbuffer(object_id_vector, object_ids_ptr, num_objects);

  memset(plasma_objects, 0, sizeof(plasma_object) * (*num_objects));
  PlasmaObject_vec_t plasma_objects_vector = PlasmaGetLocalReply_plasma_objects(req);

  for (int i = 0; i < *num_objects; ++i) {
    PlasmaObject_struct_t obj = PlasmaObject_vec_at(plasma_objects_vector, i);
    plasma_objects[i].handle.store_fd = PlasmaObject_segment_index(obj);
    plasma_objects[i].handle.mmap_size = PlasmaObject_mmap_size(obj);
    plasma_objects[i].data_offset = PlasmaObject_data_offset(obj);
    plasma_objects[i].data_size = PlasmaObject_data_size(obj);
    plasma_objects[i].metadata_offset = PlasmaObject_metadata_offset(obj);
    plasma_objects[i].metadata_size = PlasmaObject_metadata_size(obj);
  }
}

/**
 *
 * Plasma wait messages.
 */

int plasma_send_wait_request(int sock,
                             object_request object_requests[],
                             int num_requests,
                             int num_ready_objects,
                             int64_t timeout_ms) {
  flatcc_builder_t builder;
  flatcc_builder_init(&builder);
  PlasmaWaitRequest_start_as_root(&builder);

  ObjectRequest_vec_start(&builder);
  for (int i = 0; i < num_requests; ++i) {
    flatbuffers_string_ref_t id =
      flatbuffers_string_create(&builder, (const char *) &object_requests[i].object_id.id[0], UNIQUE_ID_SIZE);
    ObjectRequest_vec_push_create(&builder, id, (int32_t)object_requests[i].type, (int32_t)object_requests[i].status);
  }
  ObjectRequest_vec_ref_t objreq_vec = ObjectRequest_vec_end(&builder);
  PlasmaWaitRequest_object_requests_add(&builder, objreq_vec);
  PlasmaWaitRequest_num_ready_objects_add(&builder, num_ready_objects);
  PlasmaWaitRequest_timeout_add(&builder, timeout_ms);
  PlasmaWaitRequest_end_as_root(&builder);
  return finalize_buffer_and_send(&builder, sock, MessageType_PlasmaWaitRequest);
}


void plasma_read_wait_request(uint8_t *data,
                              object_request object_requests[],
                              int *num_ready_objects) {
  CHECK(data);
  PlasmaWaitRequest_table_t req = PlasmaWaitRequest_as_root(data);
  ObjectRequest_vec_t objreq_vec = PlasmaWaitRequest_object_requests(req);
  // TODO (ion): This is risky, maybe num_ready_objects should contain length of object_request object_requests?
  *num_ready_objects = ObjectRequest_vec_len(objreq_vec);
  for (int i = 0; i < *num_ready_objects; i++) {
    ObjectRequest_table_t objreq = ObjectRequest_vec_at(objreq_vec, i);
    memcpy(&object_requests[i].object_id.id[0], ObjectRequest_id(objreq), UNIQUE_ID_SIZE);
    object_requests[i].type = ObjectRequest_type(objreq);
    object_requests[i].status = ObjectRequest_status(objreq);
  }
}





