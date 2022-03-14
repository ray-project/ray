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

#include "ray/object_manager/object_reader.h"
#include "ray/object_manager/plasma/client.h"

namespace ray {

/// A wrapper over plasma object. Please reader ray/object_manager/object_reader.h
/// for interface guanrantees. This class is thread safe.
class MemoryObjectReader : public IObjectReader {
 public:
  MemoryObjectReader(plasma::ObjectBuffer object_buffer, rpc::Address owner_address);

  uint64_t GetDataSize() const override;

  uint64_t GetMetadataSize() const override;

  const rpc::Address &GetOwnerAddress() const override;

  bool ReadFromDataSection(uint64_t offset, uint64_t size, char *output) const override;
  bool ReadFromMetadataSection(uint64_t offset,
                               uint64_t size,
                               char *output) const override;

 private:
  const plasma::ObjectBuffer object_buffer_;
  const rpc::Address owner_address_;
};

}  // namespace ray
