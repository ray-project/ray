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

#include <gtest/gtest_prod.h>

#include <string>

#include "absl/types/optional.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {

/// Represent a local object spilled in the object_url.
/// This class is thread safe.
class SpilledObject {
 public:
  /// Create a Spilled Object. Returns an empty optional if any error happens, such as
  /// malformed url; corrupted/deleted file; or 0 chunk_size.
  ///
  /// \param object_url the object url in the form of {path}?offset={offset}&size={size}
  /// \param chunk_size the size of chunk for read.
  static absl::optional<SpilledObject> CreateSpilledObject(const std::string &object_url,
                                                           uint64_t chunk_size);

  /// Return the size of data (exclusing metadata).
  uint64_t GetDataSize() const;

  /// Return the size of metadata.
  uint64_t GetMetadataSize() const;

  const rpc::Address &GetOwnerAddress() const;

  uint64_t GetNumChunks() const;

  /// Return the value in a given chunk, identified by chunk_index.
  /// It migh return an empty optional if the file is deleted.
  ///
  /// \param chunk_index the index of chunk to return. index greater or
  ///                    equal to GetNumChunks() yields undefined behavior.
  absl::optional<std::string> GetChunk(uint64_t chunk_index) const;

 private:
  SpilledObject(std::string file_path, uint64_t total_size, uint64_t data_offset,
                uint64_t data_size, uint64_t metadata_offset, uint64_t metadata_size,
                rpc::Address owner_address, uint64_t chunk_size);

  /// Parse the object url in the form of {path}?offset={offset}&size={size}.
  /// Return false if parsing failed.
  ///
  /// \param[in] object_url url to parse from.
  /// \param[out] file_path file stores the object.
  /// \param[out] object_offset offset of the object stored in the file..
  /// \param[out] total_size object size in the file.
  /// \return bool.
  static bool ParseObjectURL(const std::string &object_url, std::string &file_path,
                             uint64_t &object_offset, uint64_t &total_size);

  /// Read the istream, parse the object header according to the following format.
  /// Return false if the input stream is deleted or corrupted.
  ///     --- start of an object (at object_offset) ---
  ///      address_size        (8 bytes),
  ///      metadata_size       (8 bytes),
  ///      data_size           (8 bytes),
  ///      serialized_address  (address_size bytes),
  ///      metadata_payload    (metadata_size bytes),
  ///      data_payload        (data_size bytes)
  ///    --- start of another object ---
  ///      ...
  ///
  /// \param[in] is input stream to read from.
  /// \param[in] object_offset offset of the object stored in the file.
  /// \param[out] data_offset data payload offset in the file.
  /// \param[out] data_size size of the data payload.
  /// \param[out] metadata_offset metadata payload offset in the file.
  /// \param[out] metadata_size size of the metadata payload.
  /// \param[out] owner_address owner address.
  /// \return bool.
  static bool ParseObjectHeader(std::istream &is, uint64_t object_offset,
                                uint64_t &data_offset, uint64_t &data_size,
                                uint64_t &metadata_offset, uint64_t &metadata_size,
                                rpc::Address &owner_address);

  /// Read 8 bytes from inputstream and deserialize it as a little-endian
  /// uint64_t. Return false if reach end of stream early.
  static bool ReadUINT64(std::istream &is, uint64_t &output);

  /// Deserialize 8 bytes string as a little-endian uint64_t.
  static uint64_t ToUINT64(const std::string &s);

  /// Helper functions read from data/metadata sections into output.
  /// Return false if the file is corrupted.
  bool ReadFromDataSection(uint64_t offset, uint64_t size, char *output) const;
  bool ReadFromMetadataSection(uint64_t offset, uint64_t size, char *output) const;

 private:
  FRIEND_TEST(SpilledObjectTest, ParseObjectURL);
  FRIEND_TEST(SpilledObjectTest, ToUINT64);
  FRIEND_TEST(SpilledObjectTest, ReadUINT64);
  FRIEND_TEST(SpilledObjectTest, ParseObjectHeader);
  FRIEND_TEST(SpilledObjectTest, Getters);
  FRIEND_TEST(SpilledObjectTest, GetNumChunks);

  const std::string file_path_;
  const uint64_t object_size_;
  const uint64_t data_offset_;
  const uint64_t data_size_;
  const uint64_t metadata_offset_;
  const uint64_t metadata_size_;
  const rpc::Address owner_address_;
  const uint64_t chunk_size_;
};

}  // namespace ray
