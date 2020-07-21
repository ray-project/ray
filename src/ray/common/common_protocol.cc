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

#include "ray/common/common_protocol.h"

#include "ray/util/logging.h"

std::string string_from_flatbuf(const flatbuffers::String &string) {
  return std::string(string.data(), string.size());
}

std::vector<std::string> string_vec_from_flatbuf(
    const flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>> &flatbuf_vec) {
  std::vector<std::string> string_vector;
  string_vector.reserve(flatbuf_vec.size());
  for (int64_t i = 0; i < flatbuf_vec.size(); i++) {
    const auto flatbuf_str = flatbuf_vec.Get(i);
    string_vector.push_back(string_from_flatbuf(*flatbuf_str));
  }
  return string_vector;
}

flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>>
string_vec_to_flatbuf(flatbuffers::FlatBufferBuilder &fbb,
                      const std::vector<std::string> &string_vector) {
  std::vector<flatbuffers::Offset<flatbuffers::String>> flatbuf_str_vec;
  flatbuf_str_vec.reserve(flatbuf_str_vec.size());
  for (auto const &str : string_vector) {
    flatbuf_str_vec.push_back(fbb.CreateString(str));
  }
  return fbb.CreateVector(flatbuf_str_vec);
}
