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

#include "absl/time/clock.h"
#include "absl/types/optional.h"
#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {

/// Binary representation of a ray object, consisting of buffer pointers to data and
/// metadata. A ray object may have both data and metadata, or only one of them.
class RayObject {
 public:
  /// Create a ray object instance.
  ///
  /// Set `copy_data` to `false` is fine for most cases - for example when putting
  /// an object into store with a temporary RayObject, and we don't want to do an extra
  /// copy. But in some cases we do want to always hold a valid data - for example, memory
  /// store uses RayObject to represent objects, in this case we actually want the object
  /// data to remain valid after user puts it into store.
  ///
  /// \param[in] data Data of the ray object.
  /// \param[in] metadata Metadata of the ray object.
  /// \param[in] nested_rfs ObjectRefs that were serialized in data.
  /// \param[in] copy_data Whether this class should hold a copy of data.
  RayObject(const std::shared_ptr<Buffer> &data, const std::shared_ptr<Buffer> &metadata,
            const std::vector<rpc::ObjectReference> &nested_refs, bool copy_data = false)
      : data_(data),
        metadata_(metadata),
        nested_refs_(nested_refs),
        has_data_copy_(copy_data),
        creation_time_nanos_(absl::GetCurrentTimeNanos()) {
    if (has_data_copy_) {
      // If this object is required to hold a copy of the data,
      // make a copy if the passed in buffers don't already have a copy.
      if (data_ && !data_->OwnsData()) {
        data_ = std::make_shared<LocalMemoryBuffer>(data_->Data(), data_->Size(),
                                                    /*copy_data=*/true);
      }

      if (metadata_ && !metadata_->OwnsData()) {
        metadata_ = std::make_shared<LocalMemoryBuffer>(
            metadata_->Data(), metadata_->Size(), /*copy_data=*/true);
      }
    }

    RAY_CHECK(data_ || metadata_) << "Data and metadata cannot both be empty.";
  }

  RayObject(rpc::ErrorType error_type);

  RayObject(rpc::ErrorType error_type, const std::string &append_data);

  RayObject(rpc::ErrorType error_type, const uint8_t *append_data,
            size_t append_data_size);

  /// Return the data of the ray object.
  const std::shared_ptr<Buffer> &GetData() const { return data_; }

  /// Return the metadata of the ray object.
  const std::shared_ptr<Buffer> &GetMetadata() const { return metadata_; }

  /// Return the ObjectRefs that were serialized in data.
  const std::vector<rpc::ObjectReference> &GetNestedRefs() const { return nested_refs_; }

  uint64_t GetSize() const {
    uint64_t size = 0;
    size += (data_ != nullptr) ? data_->Size() : 0;
    size += (metadata_ != nullptr) ? metadata_->Size() : 0;
    return size;
  }

  /// Whether this object has data.
  bool HasData() const { return data_ != nullptr; }

  /// Whether this object has metadata.
  bool HasMetadata() const { return metadata_ != nullptr; }

  /// Whether the object represents an exception.
  bool IsException(rpc::ErrorType *error_type = nullptr) const;

  /// Whether the object has been promoted to plasma (i.e., since it was too
  /// large to return directly as part of a gRPC response).
  bool IsInPlasmaError() const;

  /// Mark this object as accessed before.
  void SetAccessed() { accessed_ = true; };

  /// Check if this object was accessed before.
  bool WasAccessed() const { return accessed_; }

  /// Return the absl time in nanoseconds when this object was created.
  int64_t CreationTimeNanos() const { return creation_time_nanos_; }

 private:
  std::shared_ptr<Buffer> data_;
  std::shared_ptr<Buffer> metadata_;
  const std::vector<rpc::ObjectReference> nested_refs_;
  /// Whether this class holds a data copy.
  bool has_data_copy_;
  /// Whether this object was accessed.
  bool accessed_ = false;
  /// The timestamp at which this object was created locally.
  int64_t creation_time_nanos_;
};

}  // namespace ray
