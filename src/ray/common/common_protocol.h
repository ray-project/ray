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
#include "src/ray/protobuf/common.pb.h"

namespace ray {

using flatbuffers::FlatBufferBuilder;
using flatbuffers::Offset;
using flatbuffers::String;
using flatbuffers::uoffset_t;
using flatbuffers::Vector;

template <typename ID>
Offset<String> to_flatbuf(FlatBufferBuilder &fbb, const ID &id) {
  return fbb.CreateString(reinterpret_cast<const char *>(id.Data()), id.Size());
}

template <typename ID>
void flatbuf_push_string(FlatBufferBuilder &fbb, const ID &id) {
  fbb.PreAlign<uoffset_t>(id.Size() + 1);
  fbb.Pad(1);
  fbb.PushBytes(id.Data(), id.Size());
  fbb.PushElement<uoffset_t>(static_cast<uoffset_t>(id.Size()));
}

template <typename ID>
Offset<Vector<Offset<String>>> to_flatbuf(FlatBufferBuilder &fbb,
                                          ID ids[],
                                          int64_t num_ids) {
  fbb.StartVector<Offset<String>>(num_ids);
  for (int64_t i = 0; i < num_ids; i++) {
    flatbuf_push_string(fbb, ids[i]);
  }
  return fbb.EndVector(num_ids);
}

template <typename ID>
Offset<Vector<Offset<String>>> to_flatbuf(FlatBufferBuilder &fbb,
                                          const std::vector<ID> &ids) {
  fbb.StartVector<Offset<String>>(ids.size());
  for (const auto &id : ids) {
    flatbuf_push_string(fbb, id);
  }
  return fbb.EndVector(ids.size());
}

template <typename ID>
Offset<Vector<Offset<String>>> to_flatbuf(FlatBufferBuilder &fbb,
                                          const std::unordered_set<ID> &ids) {
  fbb.StartVector<Offset<String>>(ids.size());
  for (const auto &id : ids) {
    flatbuf_push_string(fbb, id);
  }
  return fbb.EndVector(ids.size());
}

inline ObjectID ObjectRefToId(const rpc::ObjectReference &object_ref) {
  return ObjectID::FromBinary(object_ref.object_id());
}

inline std::vector<ObjectID> ObjectRefsToIds(
    const std::vector<rpc::ObjectReference> &object_refs) {
  std::vector<ObjectID> object_ids;
  object_ids.reserve(object_refs.size());
  for (const auto &ref : object_refs) {
    object_ids.push_back(ObjectRefToId(ref));
  }
  return object_ids;
}

}  // namespace ray
