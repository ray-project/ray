#include "plasma_protocol.h"

#include "io.h"

#include "format/plasma_builder.h"

#define FLATBUFFER_BUILDER_DEFAULT_SIZE 1024

int plasma_send_create_request(int sock,
                               object_id object_id,
                               int64_t data_size,
                               int64_t metadata_size) {
  flatcc_builder_t builder;
  flatcc_builder_init(&builder);
  PlasmaCreateRequest_start_as_root(&builder);
  PlasmaCreateRequest_object_id_create(&builder, &object_id.id[0], UNIQUE_ID_SIZE);
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
  PlasmaCreateReply_object_id_create(&builder, &object_id.id[0], UNIQUE_ID_SIZE);
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
