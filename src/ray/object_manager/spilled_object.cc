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
namespace {
const size_t UINT64_size = sizeof(uint64_t);
}

/* static */ absl::optional<SpilledObject> SpilledObject::CreateSpilledObject(
    const std::string &object_url, uint64_t chunk_size) {
  if (chunk_size == 0) {
    RAY_LOG(WARNING) << "chunk_size can't be 0.";
    return absl::optional<SpilledObject>();
  }

  std::string file_path;
  uint64_t object_offset = 0;
  uint64_t object_size = 0;

  if (!SpilledObject::ParseObjectURL(object_url, file_path, object_offset, object_size)) {
    RAY_LOG(WARNING) << "Failed to parse spilled object url: " << object_url;
    return absl::optional<SpilledObject>();
  }

  uint64_t data_offset = 0;
  uint64_t data_size = 0;
  uint64_t metadata_offset = 0;
  uint64_t metadata_size = 0;
  rpc::Address owner_address;

  std::ifstream is(file_path, std::ios::binary);
  if (!is ||
      !SpilledObject::ParseObjectHeader(is, object_offset, data_offset, data_size,
                                        metadata_offset, metadata_size, owner_address)) {
    RAY_LOG(WARNING) << "Failed to parse object header for spilled object " << object_url;
    return absl::optional<SpilledObject>();
  }

  return absl::optional<SpilledObject>(SpilledObject(
      std::move(file_path), object_size, data_offset, data_size, metadata_offset,
      metadata_size, std::move(owner_address), chunk_size));
}

uint64_t SpilledObject::GetDataSize() const { return data_size_; }

uint64_t SpilledObject::GetMetadataSize() const { return metadata_size_; }

const rpc::Address &SpilledObject::GetOwnerAddress() const { return owner_address_; }

uint64_t SpilledObject::GetNumChunks() const {
  // return ceil( (data_size + metadata_size) / chunk_size_)
  return (data_size_ + metadata_size_ + chunk_size_ - 1) / chunk_size_;
}

absl::optional<std::string> SpilledObject::GetChunk(uint64_t chunk_index) const {
  // The spilled file stores metadata before data. But the GetChunk needs to
  // return data before metadata. We achieve by first read from data section,
  // then read from metadata section.
  const auto cur_chunk_offset = chunk_index * chunk_size_;
  const auto cur_chunk_size =
      std::min(chunk_size_, data_size_ + metadata_size_ - cur_chunk_offset);

  std::string result(cur_chunk_size, '\0');
  size_t result_offset = 0;

  if (cur_chunk_offset < data_size_) {
    // read from data section.
    auto offset = cur_chunk_offset;
    auto size = std::min(data_size_ - cur_chunk_offset, cur_chunk_size);
    if (!ReadFromDataSection(offset, size, &result[result_offset])) {
      return absl::optional<std::string>();
    }
    result_offset = size;
  }

  if (cur_chunk_offset + cur_chunk_size > data_size_) {
    // read from metadata section.
    auto offset = std::max(cur_chunk_offset, data_size_) - data_size_;
    auto size = std::min(cur_chunk_offset + cur_chunk_size - data_size_, cur_chunk_size);
    if (!ReadFromMetadataSection(offset, size, &result[result_offset])) {
      return absl::optional<std::string>();
    }
  }
  return absl::optional<std::string>(std::move(result));
}

SpilledObject::SpilledObject(std::string file_path, uint64_t object_size,
                             uint64_t data_offset, uint64_t data_size,
                             uint64_t metadata_offset, uint64_t metadata_size,
                             rpc::Address owner_address, uint64_t chunk_size)
    : file_path_(std::move(file_path)),
      object_size_(object_size),
      data_offset_(data_offset),
      data_size_(data_size),
      metadata_offset_(metadata_offset),
      metadata_size_(metadata_size),
      owner_address_(std::move(owner_address)),
      chunk_size_(chunk_size) {}

/* static */ bool SpilledObject::ParseObjectURL(const std::string &object_url,
                                                std::string &file_path,
                                                uint64_t &object_offset,
                                                uint64_t &object_size) {
  static const std::regex object_url_pattern("^(.*)\\?offset=(\\d+)&size=(\\d+)$");
  std::smatch match_groups;
  if (!std::regex_match(object_url, match_groups, object_url_pattern) ||
      match_groups.size() != 4) {
    return false;
  }
  file_path = match_groups[1].str();
  try {
    object_offset = std::stoi(match_groups[2].str());
    object_size = std::stoi(match_groups[3].str());
  } catch (...) {
    return false;
  }
  return true;
}

/* static */
bool SpilledObject::ParseObjectHeader(std::istream &is, uint64_t object_offset,
                                      uint64_t &data_offset, uint64_t &data_size,
                                      uint64_t &metadata_offset, uint64_t &metadata_size,
                                      rpc::Address &owner_address) {
  if (!is.seekg(object_offset)) {
    return false;
  }

  uint64_t address_size = 0;
  if (!ReadUINT64(is, address_size) || !ReadUINT64(is, metadata_size) ||
      !ReadUINT64(is, data_size)) {
    return false;
  }

  std::string address_str(address_size, '\0');
  if (!is.read(&address_str[0], address_size) ||
      !owner_address.ParseFromString(address_str)) {
    return false;
  }

  metadata_offset = object_offset + UINT64_size * 3 + address_size;
  data_offset = metadata_offset + metadata_size;
  return true;
}

/* static */
bool SpilledObject::ReadUINT64(std::istream &is, uint64_t &output) {
  std::string buff(UINT64_size, '\0');
  if (!is.read(&buff[0], UINT64_size)) {
    return false;
  }
  output = SpilledObject::ToUINT64(buff);
  return true;
}

/* static */
uint64_t SpilledObject::ToUINT64(const std::string &s) {
  RAY_CHECK(s.size() == UINT64_size);
  uint64_t result = 0;
  for (size_t i = 0; i < s.size(); i++) {
    result = result << 8;
    result += static_cast<unsigned char>(s.at(s.size() - i - 1));
  }
  return result;
}

bool SpilledObject::ReadFromDataSection(uint64_t offset, uint64_t size,
                                        char *output) const {
  std::ifstream is(file_path_, std::ios::binary);
  return is.seekg(data_offset_ + offset) && is.read(output, size);
}

bool SpilledObject::ReadFromMetadataSection(uint64_t offset, uint64_t size,
                                            char *output) const {
  std::ifstream is(file_path_, std::ios::binary);
  return is.seekg(metadata_offset_ + offset) && is.read(output, size);
}
}  // namespace ray
