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

#include "ray/object_manager/memory_object_reader.h"

#include <cstring>

namespace ray {

MemoryObjectReader::MemoryObjectReader(plasma::ObjectBuffer object_buffer,
                                       rpc::Address owner_address)
    : object_buffer_(std::move(object_buffer)),
      owner_address_(std::move(owner_address)) {}

uint64_t MemoryObjectReader::GetDataSize() const { return object_buffer_.data->Size(); }

uint64_t MemoryObjectReader::GetMetadataSize() const {
  return object_buffer_.metadata->Size();
}

const rpc::Address &MemoryObjectReader::GetOwnerAddress() const { return owner_address_; }

bool MemoryObjectReader::ReadFromDataSection(uint64_t offset,
                                             uint64_t size,
                                             char *output) const {
  if (offset + size > GetDataSize()) {
    return false;
  }
  std::memcpy(output, object_buffer_.data->Data() + offset, size);
  return true;
}

bool MemoryObjectReader::ReadFromMetadataSection(uint64_t offset,
                                                 uint64_t size,
                                                 char *output) const {
  if (offset + size > GetMetadataSize()) {
    return false;
  }
  std::memcpy(output, object_buffer_.metadata->Data() + offset, size);
  return true;
}

}  // namespace ray
