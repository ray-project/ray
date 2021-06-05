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

#include "ray/object_manager/spilled_object.h"

#include <fstream>
#include <regex>

#include "ray/util/logging.h"

namespace ray {

SpilledObject::SpilledObject(const std::string &object_url, uint64_t chunk_size) {
  ParseObjectURL(object_url);
  ReadObjectHeader();
  chunk_size_ = chunk_size;
}

uint64_t SpilledObject::GetDataSize() const { return data_size_; }

uint64_t SpilledObject::GetMetadataSize() const { return metadata_size_; }

const rpc::Address &SpilledObject::GetOwnerAddress() const { return owner_address_; }

uint64_t SpilledObject::GetNumChunks() const {
  return (total_size_ + chunk_size_ - 1) / chunk_size_;
}

std::string SpilledObject::GetChunk(uint64_t chunk_index) const {
  auto start_offset = chunk_index * chunk_size_;
  auto length = std::min(chunk_size_, total_size_ - start_offset);
  if (start_offset + length <= data_size_) {
    // read from data section
    return ReadFromFile(data_offset_ + start_offset, length);
  }
  if (start_offset >= data_size_) {
    // read from metadata section
    return ReadFromFile(metadata_offset_ + (start_offset - data_size_), length);
  }

  std::string result;
  result.append(ReadFromFile(data_offset_ + start_offset, data_size_ - start_offset));
  result.append(ReadFromFile(metadata_offset_, length - (data_size_ - start_offset)));
  return result;
}

void SpilledObject::ParseObjectURL(const std::string &object_url) {
  static const std::regex object_url_pattern("(.*)\\?offset=(\\d+)&size=(\\d+)");
  std::smatch match_groups;
  RAY_DCHECK(std::regex_match(object_url, match_groups, object_url_pattern));
  RAY_DCHECK(match_groups.size() == 4);
  file_path_ = match_groups[1].str();
  object_offset_ = std::stoi(match_groups[2].str());
  total_size_ = std::stoi(match_groups[3].str());
}

void SpilledObject::ReadObjectHeader() {
  auto offset = object_offset_;
  auto address_size = ToUint64(ReadFromFile(offset, 8));
  offset += 8;
  metadata_size_ = ToUint64(ReadFromFile(offset, 8));
  offset += 8;
  data_size_ = ToUint64(ReadFromFile(offset, 8));
  offset += 8;
  owner_address_.ParseFromString(ReadFromFile(offset, address_size));
  metadata_offset_ = offset + address_size;
  data_offset_ = metadata_offset_ + metadata_size_;
}

uint64_t SpilledObject::ToUint64(const std::string &s) const {
  RAY_CHECK(s.size() == 8);
  uint64_t result = 0;
  for (size_t i = 0; i < s.size(); i++) {
    result = result << 8;
    result += static_cast<unsigned char>(s.at(s.size() - i - 1));
  }
  return result;
}

std::string SpilledObject::ReadFromFile(int64_t offset, int64_t length) const {
  std::ifstream is(file_path_, std::ios::binary);
  is.seekg(offset);
  std::string str(length, '\0');
  RAY_CHECK(is.read(&str[0], length));
  return str;
}

}  // namespace ray
