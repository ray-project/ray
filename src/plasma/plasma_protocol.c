#include "plasma_protocol.h"

#include "io.h"

#include "format/plasma_builder.h"

#define FLATBUFFER_BUILDER_DEFAULT_SIZE 1024

int plasma_send_create_request(int sock,
                               object_id object_id,
                               int64_t data_size,
                               int64_t metadata_size) {
/*
  flatbuffers::FlatBufferBuilder fbb(FLATBUFFER_BUILDER_DEFAULT_SIZE);
  auto id = fbb.CreateString((char*) &object_id.id[0], UNIQUE_ID_SIZE);
  auto message = CreatePlasmaCreateRequest(fbb, id, data_size, metadata_size);
  fbb.Finish(message);
  return write_message(sock, PLASMA_PROTOCOL_VERSION, MessageType_PlasmaCreateRequest, fbb.GetSize(), fbb.GetBufferPointer());
*/
}

void plasma_read_create_request(uint8_t *data,
                                object_id *object_id,
                                int64_t *data_size,
                                int64_t *metadata_size) {
  CHECK(data);
/*
  auto message = flatbuffers::GetRoot<PlasmaCreateRequest>(data);
  *data_size = message->data_size();
  *metadata_size = message->metadata_size();
  CHECK(message->object_id()->size() == UNIQUE_ID_SIZE);
  memcpy(&object_id->id[0], message->object_id()->data(), UNIQUE_ID_SIZE);
*/
}

int plasma_send_create_reply(int sock,
                             object_id object_id,
                             plasma_object *object,
                             int error_code) {
/*
  flatbuffers::FlatBufferBuilder fbb(FLATBUFFER_BUILDER_DEFAULT_SIZE);
  auto id = fbb.CreateString((char*) &object_id.id[0], UNIQUE_ID_SIZE);
  PlasmaObject plasma_object(object->handle.store_fd, object->handle.mmap_size, object->data_offset, object->data_size, object->metadata_offset, object->metadata_size);
  auto message = CreatePlasmaCreateReply(fbb, id, &plasma_object, (PlasmaError) error_code);
  fbb.Finish(message);
  return write_message(sock, PLASMA_PROTOCOL_VERSION, MessageType_PlasmaCreateReply, fbb.GetSize(), fbb.GetBufferPointer());
*/
}

void plasma_read_create_reply(uint8_t *data,
                              object_id *object_id,
                              plasma_object *object,
                              int *error_code) {
  CHECK(data);
/*
  auto message = flatbuffers::GetRoot<PlasmaCreateReply>(data);
  memcpy(&object_id->id[0], message->object_id()->data(), UNIQUE_ID_SIZE);
  object->handle.store_fd = message->plasma_object()->segment_index();
  object->handle.mmap_size = message->plasma_object()->mmap_size();
  object->data_offset = message->plasma_object()->data_offset();
  object->data_size = message->plasma_object()->data_size();
  object->metadata_offset = message->plasma_object()->metadata_offset();
  object->metadata_size = message->plasma_object()->metadata_size();
  *error_code = message->error();
*/
}
