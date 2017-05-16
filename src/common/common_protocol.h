#ifndef COMMON_PROTOCOL_H
#define COMMON_PROTOCOL_H

#include "format/common_generated.h"

#include "common.h"

#define DB_CLIENT_PREFIX "CL:"

/**
 * Convert an object ID to a flatbuffer string.
 *
 * @param fbb Reference to the flatbuffer builder.
 * @param object_id The object ID to be converted.
 * @return The flatbuffer string contining the object ID.
 */
flatbuffers::Offset<flatbuffers::String> to_flatbuf(
    flatbuffers::FlatBufferBuilder &fbb,
    ObjectID object_id);

/**
 * Convert a flatbuffer string to an object ID.
 *
 * @param string The flatbuffer string.
 * @return The object ID.
 */
ObjectID from_flatbuf(const flatbuffers::String *string);

/**
 * Convert an array of object IDs to a flatbuffer vector of strings.
 *
 * @param fbb Reference to the flatbuffer builder.
 * @param object_ids Array of object IDs.
 * @param num_objects Number of elements in the array.
 * @return Flatbuffer vector of strings.
 */
flatbuffers::Offset<
    flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>>
to_flatbuf(flatbuffers::FlatBufferBuilder &fbb,
           ObjectID object_ids[],
           int64_t num_objects);

#endif
