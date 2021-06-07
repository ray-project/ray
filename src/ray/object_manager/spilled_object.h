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

#include <string>

#include "absl/types/optional.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {

/// Represent an object which is spilled in the object_url.
/// This class is thread safe.
class SpilledObject {
 public:
  static absl::optional<SpilledObject> CreateSpilledObject(const std::string &object_url,
                                                           uint64_t chunk_size);

  uint64_t GetDataSize() const;
  uint64_t GetMetadataSize() const;
  const rpc::Address &GetOwnerAddress() const;

  uint64_t GetNumChunks() const;
  absl::optional<std::string> GetChunk(uint64_t chunk_index) const;

 private:
  SpilledObject(std::string file_path, uint64_t total_size, uint64_t data_offset,
                const uint64_t data_size, const uint64_t metadata_offset,
                const uint64_t metadata_size, const rpc::Address owner_address,
                const uint64_t chunk_size);

  static bool ParseObjectURL(const std::string &object_url, std::string &file_path,
                             uint64_t &object_offset, uint64_t total_size);

  static bool ParseObjectHeader(const std::string &file_path, uint64_t object_offset,
                                uint64_t &data_offset, uint64_t &data_size,
                                uint64_t &metadata_offset, uint64_t &metadata_size,
                                rpc::Address &owner_address);

  static uint64_t ToUINT64(const std::string &s);
  static bool ReadUINT64(std::ifstream &is, uint64_t &output);

  bool ReadFromDataSection(uint64_t offset, uint64_t size, char *output) const;
  bool ReadFromMetadataSection(uint64_t offset, uint64_t size, char *output) const;

 private:
  const std::string file_path_;
  const uint64_t total_size_;
  const uint64_t data_offset_;
  const uint64_t data_size_;
  const uint64_t metadata_offset_;
  const uint64_t metadata_size_;
  const rpc::Address owner_address_;
  const uint64_t chunk_size_;
};

}  // namespace ray
