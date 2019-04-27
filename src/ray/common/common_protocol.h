#ifndef COMMON_PROTOCOL_H
#define COMMON_PROTOCOL_H

#include "ray/gcs/format/gcs_generated.h"

#include <unordered_map>

#include "ray/id.h"
#include "ray/util/logging.h"

/// Convert an unique ID to a flatbuffer string.
///
/// @param fbb Reference to the flatbuffer builder.
/// @param id The ID to be converted.
/// @return The flatbuffer string containing the ID.
template <typename ID>
flatbuffers::Offset<flatbuffers::String> to_flatbuf(flatbuffers::FlatBufferBuilder &fbb,
                                                    ID id);

/// Convert a flatbuffer string to an unique ID.
///
/// @param string The flatbuffer string.
/// @return The ID.
template <typename ID>
ID from_flatbuf(const flatbuffers::String &string);

/// Convert a flatbuffer vector of strings to a vector of unique IDs.
///
/// @param vector The flatbuffer vector.
/// @return The vector of IDs.
template <typename ID>
const std::vector<ID> from_flatbuf(
    const flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>> &vector);

/// Convert a flatbuffer of string that concatenated
/// unique IDs to a vector of unique IDs.
///
/// @param vector The flatbuffer vector.
/// @return The vector of IDs.
template <typename ID>
const std::vector<ID> ids_from_flatbuf(const flatbuffers::String &string);

/// Convert a vector of unique IDs to a flatbuffer string.
/// The IDs are concatenated to a string with binary.
///
/// @param fbb Reference to the flatbuffer builder.
/// @param ids The vector of IDs.
/// @return Flatbuffer string of concatenated IDs.
template <typename ID>
flatbuffers::Offset<flatbuffers::String> ids_to_flatbuf(
    flatbuffers::FlatBufferBuilder &fbb, const std::vector<ID> &ids);

/// Convert an array of unique IDs to a flatbuffer vector of strings.
///
/// @param fbb Reference to the flatbuffer builder.
/// @param ids Array of unique IDs.
/// @param num_ids Number of elements in the array.
/// @return Flatbuffer vector of strings.
template <typename ID>
flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>>
to_flatbuf(flatbuffers::FlatBufferBuilder &fbb, ID ids[], int64_t num_ids);

/// Convert a vector of unique IDs to a flatbuffer vector of strings.
///
/// @param fbb Reference to the flatbuffer builder.
/// @param ids Vector of IDs.
/// @return Flatbuffer vector of strings.
template <typename ID>
flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>>
to_flatbuf(flatbuffers::FlatBufferBuilder &fbb, const std::vector<ID> &ids);

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
                                                    ID id) {
  return fbb.CreateString(reinterpret_cast<const char *>(id.data()), id.size());
}

template <typename ID>
ID from_flatbuf(const flatbuffers::String &string) {
  RAY_CHECK(string.size() == ID::size());
  return ID::from_binary(string.str());
}

template <typename ID>
const std::vector<ID> from_flatbuf(
    const flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>> &vector) {
  std::vector<ID> ids;
  for (int64_t i = 0; i < vector.Length(); i++) {
    ids.push_back(from_flatbuf<ID>(*vector.Get(i)));
  }
  return ids;
}

template <typename ID>
const std::vector<ID> ids_from_flatbuf(const flatbuffers::String &string) {
  const auto &ids = string_from_flatbuf(string);
  std::vector<ID> ret;
  size_t id_size = ID::size();
  RAY_CHECK(ids.size() % id_size == 0);
  auto count = ids.size() / id_size;

  for (size_t i = 0; i < count; ++i) {
    auto pos = static_cast<size_t>(id_size * i);
    const auto &id = ids.substr(pos, id_size);
    ret.push_back(ID::from_binary(id));
  }

  return ret;
}

template <typename ID>
flatbuffers::Offset<flatbuffers::String> ids_to_flatbuf(
    flatbuffers::FlatBufferBuilder &fbb, const std::vector<ID> &ids) {
  std::string result;
  for (const auto &id : ids) {
    result += id.binary();
  }

  return fbb.CreateString(result);
}

template <typename ID>
flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>>
to_flatbuf(flatbuffers::FlatBufferBuilder &fbb, ID ids[], int64_t num_ids) {
  std::vector<flatbuffers::Offset<flatbuffers::String>> results;
  for (int64_t i = 0; i < num_ids; i++) {
    results.push_back(to_flatbuf(fbb, ids[i]));
  }
  return fbb.CreateVector(results);
}

template <typename ID>
flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>>
to_flatbuf(flatbuffers::FlatBufferBuilder &fbb, const std::vector<ID> &ids) {
  std::vector<flatbuffers::Offset<flatbuffers::String>> results;
  for (auto id : ids) {
    results.push_back(to_flatbuf(fbb, id));
  }
  return fbb.CreateVector(results);
}

#endif
