#include "plasma_protocol.h"

#include "io.h"

#include "format/plasma_builder.h"

#define FLATBUFFER_BUILDER_DEFAULT_SIZE 1024

/* An argument to a function that a return value gets written to. */
#define OUT

flatbuffers_string_vec_ref_t add_object_ids_to_flatbuffer(flatcc_builder_t *B, object_id object_ids[], int64_t num_objects) {
  flatbuffers_string_vec_start(B);
  for (int i = 0; i < num_objects; i++) {
    flatbuffers_string_ref_t id = flatbuffers_string_create(B, (const char *) &object_ids[i].id[0], UNIQUE_ID_SIZE);
    flatbuffers_string_vec_push(B, id);
  }
  return flatbuffers_string_vec_end(B);
}

void read_object_ids_from_flatbuffer(flatbuffers_string_vec_t object_id_vector,
                                     OUT object_id object_ids[],
                                     OUT int64_t *num_objects) {
  
  *num_objects = flatbuffers_string_vec_len(object_id_vector);
  for (int i = 0; i < *num_objects; i++) {
    memcpy(&object_ids[i].id[0], flatbuffers_string_vec_at(object_id_vector, i), UNIQUE_ID_SIZE);
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
  size_t size;
  void *buff = flatcc_builder_finalize_buffer(&builder, &size);
  int r = write_message(sock, PLASMA_PROTOCOL_VERSION, MessageType_PlasmaCreateRequest, size, buff);
  free(buff);
  return r;
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
  size_t size;
  void *buff = flatcc_builder_finalize_buffer(&builder, &size);
  int r = write_message(sock, PLASMA_PROTOCOL_VERSION, MessageType_PlasmaCreateReply, size, buff);
  free(buff);
  return r;
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

int plasma_send_get_local_request(int sock,
                                  object_id object_ids[],
                                  int64_t num_objects) {
  flatcc_builder_t builder;
  flatcc_builder_init(&builder);
  PlasmaGetLocalRequest_start_as_root(&builder);

  flatbuffers_string_vec_ref_t ids = add_object_ids_to_flatbuffer(&builder, object_ids, num_objects);
  PlasmaGetLocalRequest_object_ids_add(&builder, ids);

  PlasmaGetLocalRequest_num_objects_add(&builder, num_objects);
  PlasmaGetLocalRequest_end_as_root(&builder);
  size_t size;
  void *buff = flatcc_builder_finalize_buffer(&builder, &size);
  int r = write_message(sock, PLASMA_PROTOCOL_VERSION, MessageType_PlasmaGetLocalRequest, size, buff);
  free(buff);
  return r;
}

void plasma_read_get_local_request(uint8_t *data,
                                   object_id object_ids[],
                                   int64_t *num_objects) {
  CHECK(data);
  PlasmaGetLocalRequest_table_t req = PlasmaGetLocalRequest_as_root(data);
  // *num_objects = PlasmaGetLocalRequest_num_objects(req);
  flatbuffers_string_vec_t object_id_vector = PlasmaGetLocalRequest_object_ids(req);
  // printf("len is %d\n", flatbuffers_string_vec_len(object_id_vec));
  // for (int i = 0; i < *num_objects; i++) {
  //   memcpy(&object_ids[i].id[0], flatbuffers_string_vec_at(object_id_vec, i), UNIQUE_ID_SIZE);
  // }
  read_object_ids_from_flatbuffer(object_id_vector, object_ids, num_objects);
  // printf("len is %d\n", PlasmaGetLocalRequest_vec_len(req));
  // *metadata_size = PlasmaCreateRequest_metadata_size(req);
  // flatbuffers_string_t id = PlasmaCreateRequest_object_id(req);
  // CHECK(flatbuffers_string_len(id) == UNIQUE_ID_SIZE);
  // memcpy(&object_id->id[0], id, UNIQUE_ID_SIZE);
}

int plasma_send_get_local_reply(int sock,
                                object_id object_ids[],
                                plasma_object plasma_objects[],
                                int64_t num_objects) {
  flatcc_builder_t builder;
  flatcc_builder_init(&builder);
  PlasmaGetLocalReply_start_as_root(&builder);

  flatbuffers_string_vec_ref_t ids = add_object_ids_to_flatbuffer(&builder, object_ids, num_objects);
  PlasmaGetLocalReply_object_ids_add(&builder, ids);

  PlasmaObject_vec_start(&builder);
  for (int i = 0; i < num_objects; ++i) {
    printf("iter %d\n", i);
    plasma_object obj = plasma_objects[i];
    PlasmaObject_t plasma_obj;
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
  PlasmaGetLocalRequest_end_as_root(&builder);
  size_t size;
  void *buff = flatcc_builder_finalize_buffer(&builder, &size);
  int r = write_message(sock, PLASMA_PROTOCOL_VERSION, MessageType_PlasmaGetLocalReply, size, buff);
  free(buff);
  return r;
}

void plasma_read_get_local_reply(uint8_t *data,
                                 object_id object_ids[],
                                 plasma_object plasma_objects[],
                                 int64_t *num_objects) {
  CHECK(data);
  PlasmaGetLocalReply_table_t req = PlasmaGetLocalReply_as_root(data);
  flatbuffers_string_vec_t object_id_vector = PlasmaGetLocalReply_object_ids(req);
  read_object_ids_from_flatbuffer(object_id_vector, object_ids, num_objects);
  
  memset(plasma_objects, 0, sizeof(plasma_object) * (*num_objects));
  PlasmaObject_vec_t plasma_objects_vector = PlasmaGetLocalReply_plasma_objects(req);

  for (int i = 0; i < *num_objects; ++i) {
    printf("loop %d\n", i);
    PlasmaObject_struct_t obj = PlasmaObject_vec_at(plasma_objects_vector, i);
    plasma_objects[i].handle.store_fd = PlasmaObject_segment_index(obj);
    plasma_objects[i].handle.mmap_size = PlasmaObject_mmap_size(obj);
    plasma_objects[i].data_offset = PlasmaObject_data_offset(obj);
    plasma_objects[i].data_size = PlasmaObject_data_size(obj);
    plasma_objects[i].metadata_offset = PlasmaObject_metadata_offset(obj);
    plasma_objects[i].metadata_size = PlasmaObject_metadata_size(obj);
  }
}
