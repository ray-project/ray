#include "common_protocol.h"

flatbuffers::Offset<flatbuffers::String> to_flatbuf(
    flatbuffers::FlatBufferBuilder &fbb,
    ObjectID object_id) {
  return fbb.CreateString((char *) &object_id.id[0], sizeof(object_id.id));
}

ObjectID from_flatbuf(const flatbuffers::String *string) {
  ObjectID object_id;
  CHECK(string->size() == sizeof(object_id.id));
  memcpy(&object_id.id[0], string->data(), sizeof(object_id.id));
  return object_id;
}

flatbuffers::Offset<
    flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>>
to_flatbuf(flatbuffers::FlatBufferBuilder &fbb,
           ObjectID object_ids[],
           int64_t num_objects) {
  std::vector<flatbuffers::Offset<flatbuffers::String>> results;
  for (size_t i = 0; i < num_objects; i++) {
    results.push_back(to_flatbuf(fbb, object_ids[i]));
  }
  return fbb.CreateVector(results);
}
