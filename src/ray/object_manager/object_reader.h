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

#include "src/ray/protobuf/common.pb.h"

namespace ray {

/// Reader over an immutable Ray object.
class IObjectReader {
 public:
  virtual ~IObjectReader() = default;

  /// Return the size of data (exclusing metadata).
  virtual uint64_t GetDataSize() const = 0;

  /// Return the size of metadata.
  virtual uint64_t GetMetadataSize() const = 0;

  uint64_t GetObjectSize() const { return GetDataSize() + GetMetadataSize(); }

  virtual const rpc::Address &GetOwnerAddress() const = 0;

  /// Read from data sections into output.
  /// Return false if the object is corrupted or size/offset is invalid.
  ///
  /// \param offset offset to the data secton to copy from.
  /// \param size number of bytes to copy.
  /// \param output pointer to the memory location to copy to.
  /// \return bool.
  virtual bool ReadFromDataSection(uint64_t offset,
                                   uint64_t size,
                                   char *output) const = 0;
  /// Read from metadata sections into output.
  /// Return false if the object is corrupted or size/offset is invalid.
  ///
  /// \param offset offset to the metadata secton to copy from.
  /// \param size number of bytes to copy.
  /// \param output pointer to the memory location to copy to.
  /// \return bool.
  virtual bool ReadFromMetadataSection(uint64_t offset,
                                       uint64_t size,
                                       char *output) const = 0;
};
}  // namespace ray
