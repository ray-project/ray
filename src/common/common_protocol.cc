#include "common_protocol.h"

flatbuffers::Offset<flatbuffers::String> to_flatbuf(
    flatbuffers::FlatBufferBuilder &fbb,
    ObjectID object_id) {
  return fbb.CreateString((char *) &object_id.id[0], sizeof(object_id.id));
}

ObjectID from_flatbuf(const flatbuffers::String &string) {
  ObjectID object_id;
  CHECK(string.size() == sizeof(object_id.id));
  memcpy(&object_id.id[0], string.data(), sizeof(object_id.id));
  return object_id;
}

flatbuffers::Offset<
    flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>>
to_flatbuf(flatbuffers::FlatBufferBuilder &fbb,
           ObjectID object_ids[],
           int64_t num_objects) {
  std::vector<flatbuffers::Offset<flatbuffers::String>> results;
  for (int64_t i = 0; i < num_objects; i++) {
    results.push_back(to_flatbuf(fbb, object_ids[i]));
  }
  return fbb.CreateVector(results);
}

std::string string_from_flatbuf(const flatbuffers::String &string) {
  return std::string(string.data(), string.size());
}

const std::unordered_map<std::string, double> map_from_flatbuf(
    const flatbuffers::Vector<flatbuffers::Offset<ResourcePair>>
        &resource_vector) {
  std::unordered_map<std::string, double> required_resources;
  for (int64_t i = 0; i < resource_vector.size(); i++) {
    const ResourcePair *resource_pair = resource_vector.Get(i);
    required_resources[string_from_flatbuf(*resource_pair->key())] =
        resource_pair->value();
  }
  return required_resources;
}

flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<ResourcePair>>>
map_to_flatbuf(flatbuffers::FlatBufferBuilder &fbb,
               const std::unordered_map<std::string, double> &resource_map) {
  std::vector<flatbuffers::Offset<ResourcePair>> resource_vector;
  for (auto const &resource_pair : resource_map) {
    resource_vector.push_back(CreateResourcePair(
        fbb, fbb.CreateString(resource_pair.first), resource_pair.second));
  }
  return fbb.CreateVector(resource_vector);
}
