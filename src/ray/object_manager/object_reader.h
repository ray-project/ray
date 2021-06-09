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

#include <memory>

#include "ray/common/status.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {

class ObjectReaderInterface {
 public:
  virtual ~ObjectReaderInterface() = default;
  virtual uint64_t GetDataSize() const = 0;
  virtual uint64_t GetMetadataSize() const = 0;
  virtual const rpc::Address &GetOwnerAddress() const = 0;
  virtual Status ReadFromDataSection(uint64_t offset, uint64_t size,
                                     char *output) const = 0;
  virtual Status ReadFromMetadataSecton(uint64_t offset, uint64_t size,
                                        char *output) const = 0;
};

class ChunkObjectReader {
 public:
  ChunkReader(std::shared_ptr<ObjectReaderInterface> object_reader, uint64_t chunk_size);
  uint64_t GetNumChunks() const;
  uint64_t GetChunkSize() const;
  uint64_t GetTotalSize() const;
  Status ReadChunk(uint64_t index, std::string &output) const;

 private:
  const std::shared_ptr<const ObjectReaderInterface> object_reader_;
  const uint64_t chunk_size_;
  const uint64_t data_size_;
  const uint64_t metadata_size_;
};

}  // namespace ray
