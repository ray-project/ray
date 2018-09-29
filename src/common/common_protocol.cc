#include "common_protocol.h"

flatbuffers::Offset<flatbuffers::String> to_flatbuf(
    flatbuffers::FlatBufferBuilder &fbb,
    ray::ObjectID object_id) {
  return fbb.CreateString(reinterpret_cast<const char *>(object_id.data()),
                          sizeof(ray::ObjectID));
}

ray::ObjectID from_flatbuf(const flatbuffers::String &string) {
  ray::ObjectID object_id;
  RAY_CHECK(string.size() == sizeof(ray::ObjectID));
  memcpy(object_id.mutable_data(), string.data(), sizeof(ray::ObjectID));
  return object_id;
}

const std::vector<ray::ObjectID> from_flatbuf(
    const flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>
        &vector) {
  std::vector<ray::ObjectID> object_ids;
  for (int64_t i = 0; i < vector.Length(); i++) {
    object_ids.push_back(from_flatbuf(*vector.Get(i)));
  }
  return object_ids;
}

flatbuffers::Offset<
    flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>>
to_flatbuf(flatbuffers::FlatBufferBuilder &fbb,
           ray::ObjectID object_ids[],
           int64_t num_objects) {
  std::vector<flatbuffers::Offset<flatbuffers::String>> results;
  for (int64_t i = 0; i < num_objects; i++) {
    results.push_back(to_flatbuf(fbb, object_ids[i]));
  }
  return fbb.CreateVector(results);
}

flatbuffers::Offset<
    flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>>
to_flatbuf(flatbuffers::FlatBufferBuilder &fbb,
           const std::vector<ray::ObjectID> &object_ids) {
  std::vector<flatbuffers::Offset<flatbuffers::String>> results;
  for (auto object_id : object_ids) {
    results.push_back(to_flatbuf(fbb, object_id));
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

std::vector<std::string> string_vec_from_flatbuf(
    const flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>
        &flatbuf_vec) {
  std::vector<std::string> string_vector;
  string_vector.reserve(flatbuf_vec.size());
  for (int64_t i = 0; i < flatbuf_vec.size(); i++) {
    const auto flatbuf_str = flatbuf_vec.Get(i);
    string_vector.push_back(string_from_flatbuf(*flatbuf_str));
  }
  return string_vector;
}

flatbuffers::Offset<
    flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>>
string_vec_to_flatbuf(flatbuffers::FlatBufferBuilder &fbb,
                      const std::vector<std::string> &string_vector) {
  std::vector<flatbuffers::Offset<flatbuffers::String>> flatbuf_str_vec;
  flatbuf_str_vec.reserve(flatbuf_str_vec.size());
  for (auto const &str : string_vector) {
    flatbuf_str_vec.push_back(fbb.CreateString(str));
  }
  return fbb.CreateVector(flatbuf_str_vec);
}