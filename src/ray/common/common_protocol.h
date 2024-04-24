// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <flatbuffers/flatbuffers.h>

#include <unordered_set>

#include "ray/common/id.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/common.pb.h"
#include "src/ray/protobuf/gcs.pb.h"

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

/// Convert an unordered_set of unique IDs to a flatbuffer vector of strings.
///
/// @param fbb Reference to the flatbuffer builder.
/// @param ids Unordered set of IDs.
/// @return Flatbuffer vector of strings.
template <typename ID>
flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>>
to_flatbuf(flatbuffers::FlatBufferBuilder &fbb, const std::unordered_set<ID> &ids);

/// Convert a flatbuffer string to a std::string.
///
/// @param fbb Reference to the flatbuffer builder.
/// @param string A flatbuffers string.
/// @return The std::string version of the flatbuffer string.
std::string string_from_flatbuf(const flatbuffers::String &string);

template <typename ID>
flatbuffers::Offset<flatbuffers::String> to_flatbuf(flatbuffers::FlatBufferBuilder &fbb,
                                                    ID id) {
  return fbb.CreateString(reinterpret_cast<const char *>(id.Data()), id.Size());
}

template <typename ID>
ID from_flatbuf(const flatbuffers::String &string) {
  return ID::FromBinary(string.str());
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

template <typename ID>
flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>>
to_flatbuf(flatbuffers::FlatBufferBuilder &fbb, const std::unordered_set<ID> &ids) {
  std::vector<flatbuffers::Offset<flatbuffers::String>> results;
  for (auto id : ids) {
    results.push_back(to_flatbuf(fbb, id));
  }
  return fbb.CreateVector(results);
}

static inline ray::rpc::ObjectReference ObjectIdToRef(
    const ray::ObjectID &object_id, const ray::rpc::Address owner_address) {
  ray::rpc::ObjectReference ref;
  ref.set_object_id(object_id.Binary());
  ref.mutable_owner_address()->CopyFrom(owner_address);
  return ref;
}

static inline ray::ObjectID ObjectRefToId(const ray::rpc::ObjectReference &object_ref) {
  return ray::ObjectID::FromBinary(object_ref.object_id());
}

static inline std::vector<ray::ObjectID> ObjectRefsToIds(
    const std::vector<ray::rpc::ObjectReference> &object_refs) {
  std::vector<ray::ObjectID> object_ids;
  for (const auto &ref : object_refs) {
    object_ids.push_back(ObjectRefToId(ref));
  }
  return object_ids;
}

static inline ray::rpc::ActorTableData::ActorState StringToActorState(
    const std::string &actor_state_name) {
  if (actor_state_name == "DEPENDENCIES_UNREADY") {
    return ray::rpc::ActorTableData::DEPENDENCIES_UNREADY;
  } else if (actor_state_name == "PENDING_CREATION") {
    return ray::rpc::ActorTableData::PENDING_CREATION;
  } else if (actor_state_name == "ALIVE") {
    return ray::rpc::ActorTableData::ALIVE;
  } else if (actor_state_name == "RESTARTING") {
    return ray::rpc::ActorTableData::RESTARTING;
  } else if (actor_state_name == "DEAD") {
    return ray::rpc::ActorTableData::DEAD;
  } else {
    RAY_CHECK(false) << "Invalid actor state name:" << actor_state_name;
    return {};
  }
}