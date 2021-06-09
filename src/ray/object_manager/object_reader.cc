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

#include "ray/rpc/object_manager/object_reader.h"

namespace ray {

ChunkReader::ChunkReader(std::shared_ptr<ObjectReaderInterface> object_reader,
                         uint64_t chunk_size)
    : object_reader_(std::move(object_reader)),
      chunk_size_(chunk_size),
      data_size_(object_reader_->GetDataSize()),
      metadata_size_(object_reader_->GetMetadataSize()) {
  RAY_CHECK(chunk_size > 0);
}

uint64_t ChunkReader::GetNumChunks() const {
  return (data_size_ + metadata_size_ + chunk_size_ - 1) / chunk_size_;
}

uint64_t GetChunkSize() const { return chunk_size_; }

Status ReadChunk(uint64_t chunk_index, std::string &output) const {
  // first read from data section, then read from metadata section.
  auto cur_chunk_offset = chunk_index * chunk_size_;
  auto cur_chunk_size =
      std::min(chunk_size_, data_size_ + metadata_size_ - cur_chunk_offset);

  // increase the output size by cur_chunk_size
  size_t output_offset = output.size();
  output.resize(output.size() + cur_chunk_size, '\0');

  if (cur_chunk_offset < data_size_) {
    // read from data section.
    auto offset = cur_chunk_offset;
    auto size = std::min(data_size_ - cur_chunk_offset, cur_chunk_size);
    RAY_CHECK_OK(
        object_reader->ReadFromDataSection(offset, size, &output[output_offset]));
    output_offset += size;
  }

  if (cur_chunk_offset + cur_chunk_size > data_size_) {
    // read from metadata section.
    auto offset = std::max(cur_chunk_offset, data_size_) - data_size_;
    auto size = std::min(cur_chunk_offset + cur_chunk_size - data_size_, cur_chunk_size);
    RAY_CHECK_OK(
        object_reader->ReadFromMetadataSecton(offset, size, &output[output_offset]));
  }
  return Status::OK();
}

}  // namespace ray
