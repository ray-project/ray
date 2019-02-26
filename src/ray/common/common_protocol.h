#ifndef COMMON_PROTOCOL_H
#define COMMON_PROTOCOL_H

#include "ray/gcs/format/gcs_generated.h"

#include <unordered_map>

#include "ray/id.h"
#include "ray/util/logging.h"

/// Convert an object ID to a flatbuffer string.
///
/// @param fbb Reference to the flatbuffer builder.
/// @param object_id The object ID to be converted.
/// @return The flatbuffer string contining the object ID.
template <typename ID>
flatbuffers::Offset<flatbuffers::String> to_flatbuf(flatbuffers::FlatBufferBuilder &fbb,
                                                    ID object_id);

/// Convert a flatbuffer string to an object ID.
///
/// @param string The flatbuffer string.
/// @return The object ID.
template <typename ID>
ID from_flatbuf(const flatbuffers::String &string);

/// Convert a flatbuffer vector of strings to a vector of object IDs.
///
/// @param vector The flatbuffer vector.
/// @return The vector of object IDs.
template <typename ID>
const std::vector<ID> from_flatbuf(
    const flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>> &vector);

/// Convert a flatbuffer of string that concatenated
/// object IDs to a vector of object IDs.
///
/// @param vector The flatbuffer vector.
/// @return The vector of object IDs.
template <typename ID>
const std::vector<ID> object_ids_from_flatbuf(const flatbuffers::String &string);

/// Convert a vector of object IDs to a flatbuffer string.
/// The IDs are concatenated to a string with binary.
///
/// @param fbb Reference to the flatbuffer builder.
/// @param object_ids The vector of object IDs.
/// @return Flatbuffer string of concatenated IDs.
template <typename ID>
flatbuffers::Offset<flatbuffers::String> object_ids_to_flatbuf(
    flatbuffers::FlatBufferBuilder &fbb, const std::vector<ID> &object_ids);

/// Convert an array of object IDs to a flatbuffer vector of strings.
///
/// @param fbb Reference to the flatbuffer builder.
/// @param object_ids Array of object IDs.
/// @param num_objects Number of elements in the array.
/// @return Flatbuffer vector of strings.
template <typename ID>
flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>>
to_flatbuf(flatbuffers::FlatBufferBuilder &fbb, ID object_ids[], int64_t num_objects);

/// Convert a vector of object IDs to a flatbuffer vector of strings.
///
/// @param fbb Reference to the flatbuffer builder.
/// @param object_ids Vector of object IDs.
/// @return Flatbuffer vector of strings.
template <typename ID>
flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>>
to_flatbuf(flatbuffers::FlatBufferBuilder &fbb, const std::vector<ID> &object_ids);

/// Convert a flatbuffer string to a std::string.
///
/// @param fbb Reference to the flatbuffer builder.
/// @param string A flatbuffers string.
/// @return The std::string version of the flatbuffer string.
std::string string_from_flatbuf(const flatbuffers::String &string);

/// Convert a std::unordered_map to a flatbuffer vector of pairs.
///
/// @param fbb Reference to the flatbuffer builder.
/// @param resource_map A mapping from resource name to resource quantity.
/// @return A flatbuffer vector of ResourcePair objects.
flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<ResourcePair>>>
map_to_flatbuf(flatbuffers::FlatBufferBuilder &fbb,
               const std::unordered_map<std::string, double> &resource_map);

/// Convert a flatbuffer vector of ResourcePair objects to a std::unordered map
/// from resource name to resource quantity.
///
/// @param fbb Reference to the flatbuffer builder.
/// @param resource_vector The flatbuffer object.
/// @return A map from resource name to resource quantity.
const std::unordered_map<std::string, double> map_from_flatbuf(
    const flatbuffers::Vector<flatbuffers::Offset<ResourcePair>> &resource_vector);

std::vector<std::string> string_vec_from_flatbuf(
    const flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>> &flatbuf_vec);

flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>>
string_vec_to_flatbuf(flatbuffers::FlatBufferBuilder &fbb,
                      const std::vector<std::string> &string_vector);

template <typename ID>
flatbuffers::Offset<flatbuffers::String> to_flatbuf(flatbuffers::FlatBufferBuilder &fbb,
                                                    ID object_id) {
  return fbb.CreateString(reinterpret_cast<const char *>(object_id.data()), sizeof(ID));
}

template <typename ID>
ID from_flatbuf(const flatbuffers::String &string) {
  ID object_id;
  RAY_CHECK(string.size() == sizeof(ID));
  memcpy(object_id.mutable_data(), string.data(), sizeof(ID));
  return object_id;
}

template <typename ID>
const std::vector<ID> from_flatbuf(
    const flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>> &vector) {
  std::vector<ID> object_ids;
  for (int64_t i = 0; i < vector.Length(); i++) {
    object_ids.push_back(from_flatbuf<ID>(*vector.Get(i)));
  }
  return object_ids;
}

template <typename ID>
const std::vector<ID> object_ids_from_flatbuf(const flatbuffers::String &string) {
  const auto &object_ids = string_from_flatbuf(string);
  std::vector<ID> ret;
  RAY_CHECK(object_ids.size() % kUniqueIDSize == 0);
  auto count = object_ids.size() / kUniqueIDSize;

  for (size_t i = 0; i < count; ++i) {
    auto pos = static_cast<size_t>(kUniqueIDSize * i);
    const auto &id = object_ids.substr(pos, kUniqueIDSize);
    ret.push_back(ID::from_binary(id));
  }

  return ret;
}

template <typename ID>
flatbuffers::Offset<flatbuffers::String> object_ids_to_flatbuf(
    flatbuffers::FlatBufferBuilder &fbb, const std::vector<ID> &object_ids) {
  std::string result;
  for (const auto &id : object_ids) {
    result += id.binary();
  }

  return fbb.CreateString(result);
}

template <typename ID>
flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>>
to_flatbuf(flatbuffers::FlatBufferBuilder &fbb, ID object_ids[], int64_t num_objects) {
  std::vector<flatbuffers::Offset<flatbuffers::String>> results;
  for (int64_t i = 0; i < num_objects; i++) {
    results.push_back(to_flatbuf(fbb, object_ids[i]));
  }
  return fbb.CreateVector(results);
}

template <typename ID>
flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>>
to_flatbuf(flatbuffers::FlatBufferBuilder &fbb, const std::vector<ID> &object_ids) {
  std::vector<flatbuffers::Offset<flatbuffers::String>> results;
  for (auto object_id : object_ids) {
    results.push_back(to_flatbuf(fbb, object_id));
  }
  return fbb.CreateVector(results);
}

#endif
