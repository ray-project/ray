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

#include "ray/object_manager/chunk_object_reader.h"

#include "ray/util/logging.h"

namespace ray {

ChunkObjectReader::ChunkObjectReader(std::shared_ptr<IObjectReader> object,
                                     uint64_t chunk_size)
    : object_(std::move(object)), chunk_size_(chunk_size) {
  RAY_CHECK(chunk_size_ > 0) << "chunk_size shouldn't be 0";
}

uint64_t ChunkObjectReader::GetNumChunks() const {
  return (object_->GetDataSize() + object_->GetMetadataSize() + chunk_size_ - 1) /
         chunk_size_;
}

absl::optional<std::string> ChunkObjectReader::GetChunk(uint64_t chunk_index) const {
  // The spilled file stores metadata before data. But the GetChunk needs to
  // return data before metadata. We achieve by first read from data section,
  // then read from metadata section.
  const auto cur_chunk_offset = chunk_index * chunk_size_;
  const auto cur_chunk_size =
      std::min(chunk_size_,
               object_->GetDataSize() + object_->GetMetadataSize() - cur_chunk_offset);

  std::string result(cur_chunk_size, '\0');
  size_t result_offset = 0;

  if (cur_chunk_offset < object_->GetDataSize()) {
    // read from data section.
    auto offset = cur_chunk_offset;
    auto size = std::min(object_->GetDataSize() - cur_chunk_offset, cur_chunk_size);
    if (!object_->ReadFromDataSection(offset, size, &result[result_offset])) {
      return absl::optional<std::string>();
    }
    result_offset = size;
  }

  if (cur_chunk_offset + cur_chunk_size > object_->GetDataSize()) {
    // read from metadata section.
    auto offset =
        std::max(cur_chunk_offset, object_->GetDataSize()) - object_->GetDataSize();
    auto size = std::min(cur_chunk_offset + cur_chunk_size - object_->GetDataSize(),
                         cur_chunk_size);
    if (!object_->ReadFromMetadataSection(offset, size, &result[result_offset])) {
      return absl::optional<std::string>();
    }
  }
  return absl::optional<std::string>(std::move(result));
}
};  // namespace ray
