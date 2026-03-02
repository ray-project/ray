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
#include <optional>
#include <string>

#include "ray/common/buffer.h"
#include "ray/object_manager/object_reader.h"

namespace ray {

/// A zero-copy reference to a chunk of an object in memory.
struct ChunkRef {
  const uint8_t *data;
  size_t size;
  std::shared_ptr<Buffer> buffer_ref;
};

/// Read object in chunks.
class ChunkObjectReader {
 public:
  /// Create a ChunkObjectReader.
  ///
  /// \param object_url the underlying object to read from.
  /// \param chunk_size the size of chunk for read
  ChunkObjectReader(std::shared_ptr<IObjectReader> object, uint64_t chunk_size);

  uint64_t GetNumChunks() const;

  /// Return the value in a given chunk, identified by chunk_index.
  /// It migh return an empty optional if the file is deleted.
  ///
  /// \param chunk_index the index of chunk to return. index greater or
  ///                    equal to GetNumChunks() yields undefined behavior.
  std::optional<std::string> GetChunk(uint64_t chunk_index) const;

  /// Return a zero-copy reference to a chunk's data.
  /// Returns nullopt if the underlying reader doesn't support zero-copy
  /// (e.g. spilled objects read from disk).
  std::optional<ChunkRef> GetChunkRef(uint64_t chunk_index) const;

  const IObjectReader &GetObject() const { return *object_; }

 private:
  const std::shared_ptr<IObjectReader> object_;
  const uint64_t chunk_size_;
};

}  // namespace ray
