#ifndef COMMON_PROTOCOL_H
#define COMMON_PROTOCOL_H

#include "format/common_generated.h"

#include <unordered_map>

#include "common.h"

#define DB_CLIENT_PREFIX "CL:"

/// Convert an object ID to a flatbuffer string.
///
/// @param fbb Reference to the flatbuffer builder.
/// @param object_id The object ID to be converted.
/// @return The flatbuffer string contining the object ID.
flatbuffers::Offset<flatbuffers::String> to_flatbuf(
    flatbuffers::FlatBufferBuilder &fbb,
    ray::ObjectID object_id);

/// Convert a flatbuffer string to an object ID.
///
/// @param string The flatbuffer string.
/// @return The object ID.
ray::ObjectID from_flatbuf(const flatbuffers::String &string);

/// Convert a flatbuffer vector of strings to a vector of object IDs.
///
/// @param vector The flatbuffer vector.
/// @return The vector of object IDs.
const std::vector<ray::ObjectID> from_flatbuf(
    const flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>
        &vector);

/// Convert an array of object IDs to a flatbuffer vector of strings.
///
/// @param fbb Reference to the flatbuffer builder.
/// @param object_ids Array of object IDs.
/// @param num_objects Number of elements in the array.
/// @return Flatbuffer vector of strings.
flatbuffers::Offset<
    flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>>
to_flatbuf(flatbuffers::FlatBufferBuilder &fbb,
           ray::ObjectID object_ids[],
           int64_t num_objects);

/// Convert a vector of object IDs to a flatbuffer vector of strings.
///
/// @param fbb Reference to the flatbuffer builder.
/// @param object_ids Vector of object IDs.
/// @return Flatbuffer vector of strings.
flatbuffers::Offset<
    flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>>
to_flatbuf(flatbuffers::FlatBufferBuilder &fbb,
           const std::vector<ray::ObjectID> &object_ids);

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
    const flatbuffers::Vector<flatbuffers::Offset<ResourcePair>>
        &resource_vector);

std::vector<std::string> string_vec_from_flatbuf(
    const flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>
        &flatbuf_vec);

flatbuffers::Offset<
    flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>>
string_vec_to_flatbuf(flatbuffers::FlatBufferBuilder &fbb,
                      const std::vector<std::string> &string_vector);
#endif
