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

#include "ray/rpc/object_manager/plasma_object_reader.h"

#include <cstring>

#include "absl/strings/str_format.h"
#include "ray/util/logging.h"

namespace ray {

PlasmaObjectReader::PlasmaObjectReader(ObjectID object_id, rpc::Address owner_address,
                                       plasma::PlasmaClient &client)
    : object_id_(object_id),
      owner_address_(std::move(owner_address)),
      client_(client),
      object_buffer_() {
  RAY_CHECK_OK(client_.Get(&object_id_, /*num_objects*/ 1, /*timeout_ms*/ 0,
                           &object_buffer_, /*is_from_worker*/ false));
}

PlasmaObjectReader::~PlasmaObjectReader() { RAY_CHECK_OK(client_.Release(&object_id_)); }

uint64_t PlasmaObjectReader::GetDataSize() const override {
  return object_buffer_.data->Size();
}

uint64_t PlasmaObjectReader::GetMetadataSize() const override {
  return object_buffer_.metadata->Size();
}

const rpc::Address &PlasmaObjectReader::GetOwnerAddress() const override {
  return owner_address_;
}

Status PlasmaObjectReader::ReadFromDataSection(uint64_t offset, uint64_t size,
                                               char *output) const override {
  if (offset + size > GetDataSize()) {
    return Status::Invalid(absl::StrFormat(
        "Read from data section failed: offset(%d) + size(%d) > data size(%d)", offset,
        size, GetDataSize()));
  }
  std::memcpy(output, object_buffer_.data->Data() + offset, size);
  return Status::OK();
}

Status PlasmaObjectReader::ReadFromMetadataSecton(uint64_t offset, uint64_t size,
                                                  char *output) const override {
  if (offset + size > GetMetadataSize()) {
    return Status::Invalid(absl::StrFormat(
        "Read from metadata section failed: offset(%d) + size(%d) > metadata size(%d)",
        offset, size, GetMetadataSize()));
  }
  std::memcpy(output, object_buffer_.metadata->Data() + offset, size);
  return Status::OK();
}

}  // namespace ray
