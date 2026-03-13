// Copyright 2025 The Ray Authors.
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

// Util function on environment variables.

#pragma once

#include <zstd.h>

#include <string>

#include "ray/util/logging.h"

namespace ray {

/// Compress the input data using zstd compression
inline std::string CompressZstd(const std::string &data) {
  // Compress the data.
  const size_t compressed_size = ZSTD_compressBound(data.size());
  std::string compressed_data(compressed_size, '\0');
  const size_t actual_compressed_size =
      ZSTD_compress(compressed_data.data(), compressed_size, data.data(), data.size(), 1);
  compressed_data.resize(actual_compressed_size);
  return compressed_data;
}

/// Decompress the input data compressed by zstd compression
inline std::string DecompressZstd(const std::string &data) {
  // Get the decompressed size.
  const size_t decompressed_size = ZSTD_getFrameContentSize(data.data(), data.size());
  // Decompress the data.
  std::string decompressed_data(decompressed_size, '\0');
  const size_t actual_decompressed_size = ZSTD_decompress(
      decompressed_data.data(), decompressed_size, data.data(), data.size());
  if (actual_decompressed_size != decompressed_size) {
    RAY_LOG(ERROR) << "ZSTD_decompress failed";
    return "";
  }
  return decompressed_data;
}

}  // namespace ray
