// Copyright 2025 The Ray Authors.
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

namespace ray {

namespace flatbuf {

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
Offset<Vector<Offset<String>>> to_flatbuf(FlatBufferBuilder &fbb,
                                          ID ids[],
                                          int64_t num_ids) {
  std::vector<flatbuffers::Offset<flatbuffers::String>> results;
  results.reserve(num_ids);
  for (int64_t i = 0; i < num_ids; i++) {
    results.push_back(to_flatbuf(fbb, ids[i]));
  }
  return fbb.CreateVector(results);
}

template <typename ID>
Offset<Vector<Offset<String>>> to_flatbuf(FlatBufferBuilder &fbb,
                                          const std::vector<ID> &ids) {
  std::vector<flatbuffers::Offset<flatbuffers::String>> results;
  results.reserve(ids.size());
  for (const auto &id : ids) {
    results.push_back(to_flatbuf(fbb, id));
  }
  return fbb.CreateVector(results);
}

template <typename ID>
Offset<Vector<Offset<String>>> to_flatbuf(FlatBufferBuilder &fbb,
                                          const std::unordered_set<ID> &ids) {
  std::vector<flatbuffers::Offset<flatbuffers::String>> results;
  results.reserve(ids.size());
  for (const auto &id : ids) {
    results.push_back(to_flatbuf(fbb, id));
  }
  return fbb.CreateVector(results);
}

}  // namespace flatbuf

}  // namespace ray
