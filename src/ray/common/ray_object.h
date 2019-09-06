#ifndef RAY_COMMON_RAY_OBJECT_H
#define RAY_COMMON_RAY_OBJECT_H

#include "ray/common/buffer.h"
#include "ray/util/logging.h"

namespace ray {

/// Binary representation of a ray object. It houlds buffer pointers of data and metadata.
/// A ray object may have both data and metadata, or only one of them.
class RayObject {
 public:
  /// Create a ray object instance.
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

    RAY_CHECK((data_ && data_->Size()) || (metadata_ && metadata_->Size()))
        << "Data and metadata cannot both empty.";
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
  bool HasData() const { return data_ != nullptr && data_->Size() > 0; }

  /// Whether this object has metadata.
  bool HasMetadata() const { return metadata_ != nullptr && metadata_->Size() > 0; }

 private:
  // TODO (kfstorm): Currently both a null pointer and a pointer points to a buffer with
  // zero size means empty data/metadata. We'd better pick one and treat the other as
  // invalid.

  std::shared_ptr<Buffer> data_;
  std::shared_ptr<Buffer> metadata_;
  /// Whether this class holds a data copy.
  bool has_data_copy_;
};

}  // namespace ray

#endif  // RAY_COMMON_BUFFER_H