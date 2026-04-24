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

#include <grpc/slice.h>
#include <grpcpp/support/byte_buffer.h>
#include <grpcpp/support/slice.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <utility>

#include "ray/common/buffer.h"
#include "ray/common/status.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/object_manager.pb.h"

namespace ray {

/// Wire format for raw Push RPCs:
///   [4 bytes: header_len (little-endian uint32)]
///   [header_len bytes: protobuf-serialized PushRequest]
///   [remaining bytes: raw chunk data]

/// Serialize a PushRequest header + raw data into a gRPC ByteBuffer.
/// Zero-copy variant: the data_slice wraps the provided buffer directly.
/// The buffer_ref shared_ptr is captured and released when gRPC finishes
/// sending, keeping the underlying memory (e.g. plasma) alive.
inline grpc::ByteBuffer SerializePushToByteBuffer(const rpc::PushRequest &header,
                                                  const uint8_t *data,
                                                  size_t data_len,
                                                  std::shared_ptr<Buffer> buffer_ref) {
  // Serialize header to string.
  std::string header_bytes;
  header.SerializeToString(&header_bytes);
  uint32_t header_len = static_cast<uint32_t>(header_bytes.size());

  // Build prefix: [header_len (4 bytes LE)][header_bytes].
  std::string prefix;
  prefix.resize(sizeof(header_len) + header_len);
  std::memcpy(prefix.data(), &header_len, sizeof(header_len));
  std::memcpy(prefix.data() + sizeof(header_len), header_bytes.data(), header_len);

  // Create a slice for the prefix (gRPC copies this small buffer).
  grpc::Slice prefix_slice(prefix.data(), prefix.size());

  // Create a zero-copy slice for the data, preventing the buffer from being
  // released until gRPC finishes sending.
  auto *ref_ptr = new std::shared_ptr<Buffer>(std::move(buffer_ref));
  grpc_slice raw_slice = grpc_slice_new_with_user_data(
      const_cast<uint8_t *>(data),
      data_len,
      [](void *p) { delete static_cast<std::shared_ptr<Buffer> *>(p); },
      ref_ptr);
  grpc::Slice data_slice(raw_slice, grpc::Slice::STEAL_REF);

  grpc::Slice slices[] = {prefix_slice, data_slice};
  return grpc::ByteBuffer(slices, 2);
}

/// Serialize a PushRequest header + owned string data into a gRPC ByteBuffer.
/// Used for the spilled-object path where data is read from disk into a string.
inline grpc::ByteBuffer SerializePushToByteBuffer(const rpc::PushRequest &header,
                                                  std::string owned_data) {
  // Serialize header to string.
  std::string header_bytes;
  header.SerializeToString(&header_bytes);
  uint32_t header_len = static_cast<uint32_t>(header_bytes.size());

  // Build prefix: [header_len (4 bytes LE)][header_bytes].
  std::string prefix;
  prefix.resize(sizeof(header_len) + header_len);
  std::memcpy(prefix.data(), &header_len, sizeof(header_len));
  std::memcpy(prefix.data() + sizeof(header_len), header_bytes.data(), header_len);

  grpc::Slice prefix_slice(prefix.data(), prefix.size());

  // Move owned_data to heap so the slice destructor can free it.
  auto *str_ptr = new std::string(std::move(owned_data));
  grpc_slice raw_slice = grpc_slice_new_with_user_data(
      const_cast<char *>(str_ptr->data()),
      str_ptr->size(),
      [](void *p) { delete static_cast<std::string *>(p); },
      str_ptr);
  grpc::Slice data_slice(raw_slice, grpc::Slice::STEAL_REF);

  grpc::Slice slices[] = {prefix_slice, data_slice};
  return grpc::ByteBuffer(slices, 2);
}

/// Result of deserializing a raw Push ByteBuffer.
struct DeserializedPush {
  rpc::PushRequest header;
  const uint8_t *data;
  size_t data_len;
  // Holds the backing memory. Caller must keep this alive while using data.
  grpc::Slice backing_slice;
};

/// Deserialize a raw Push ByteBuffer into header + data pointer.
/// Returns error status if the buffer is malformed.
inline Status DeserializePushFromByteBuffer(grpc::ByteBuffer *buffer,
                                            DeserializedPush *out) {
  // Dump to a single contiguous slice.
  grpc::Slice slice;
  auto status = buffer->DumpToSingleSlice(&slice);
  if (!status.ok()) {
    return Status::IOError("Failed to dump ByteBuffer to single slice");
  }

  const uint8_t *raw = slice.begin();
  size_t total_len = slice.size();

  // Parse header length.
  if (total_len < sizeof(uint32_t)) {
    return Status::IOError("Push ByteBuffer too small for header length");
  }
  uint32_t header_len;
  std::memcpy(&header_len, raw, sizeof(header_len));

  // Parse header.
  if (total_len < sizeof(uint32_t) + header_len) {
    return Status::IOError("Push ByteBuffer too small for header");
  }
  if (!out->header.ParseFromArray(raw + sizeof(uint32_t), header_len)) {
    return Status::IOError("Failed to parse PushRequest header");
  }

  // Remaining bytes are the raw chunk data.
  size_t data_offset = sizeof(uint32_t) + header_len;
  out->data = raw + data_offset;
  out->data_len = total_len - data_offset;
  out->backing_slice = std::move(slice);

  return Status::OK();
}

}  // namespace ray
