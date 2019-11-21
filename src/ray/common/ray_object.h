#ifndef RAY_COMMON_RAY_OBJECT_H
#define RAY_COMMON_RAY_OBJECT_H

#include "ray/common/buffer.h"
#include "ray/protobuf/gcs.pb.h"
#include "ray/util/logging.h"

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
  /// \param[in] copy_data Whether this class should hold a copy of data.
  RayObject(const std::shared_ptr<Buffer> &data, const std::shared_ptr<Buffer> &metadata,
            bool copy_data = false)
      : data_(data), metadata_(metadata), has_data_copy_(copy_data) {
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

  /// Return the data of the ray object.
  const std::shared_ptr<Buffer> &GetData() const { return data_; };

  /// Return the metadata of the ray object.
  const std::shared_ptr<Buffer> &GetMetadata() const { return metadata_; };

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
  bool IsException();

  /// Whether the object has been promoted to plasma (i.e., since it was too
  /// large to return directly as part of a gRPC response).
  bool IsInPlasmaError();

 private:
  std::shared_ptr<Buffer> data_;
  std::shared_ptr<Buffer> metadata_;
  /// Whether this class holds a data copy.
  bool has_data_copy_;
};

}  // namespace ray

#endif  // RAY_COMMON_RAY_OBJECT_H
