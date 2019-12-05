#include "ray/common/ray_object.h"

namespace ray {

std::shared_ptr<LocalMemoryBuffer> MakeErrorMetadataBuffer(rpc::ErrorType error_type) {
  std::string meta = std::to_string(static_cast<int>(error_type));
  auto metadata = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(meta.data()));
  auto meta_buffer =
      std::make_shared<LocalMemoryBuffer>(metadata, meta.size(), /*copy_data=*/true);
  return meta_buffer;
}

RayObject::RayObject(rpc::ErrorType error_type)
    : RayObject(nullptr, MakeErrorMetadataBuffer(error_type)) {}

bool RayObject::IsException(rpc::ErrorType *error_type) const {
  if (metadata_ == nullptr) {
    return false;
  }
  // TODO (kfstorm): metadata should be structured.
  const std::string metadata(reinterpret_cast<const char *>(metadata_->Data()),
                             metadata_->Size());
  const auto error_type_descriptor = ray::rpc::ErrorType_descriptor();
  for (int i = 0; i < error_type_descriptor->value_count(); i++) {
    const auto error_type_number = error_type_descriptor->value(i)->number();
    if (metadata == std::to_string(error_type_number)) {
      if (error_type) {
        *error_type = rpc::ErrorType(error_type_number);
      }
      return true;
    }
  }
  return false;
}

bool RayObject::IsInPlasmaError() const {
  if (metadata_ == nullptr) {
    return false;
  }
  const std::string metadata(reinterpret_cast<const char *>(metadata_->Data()),
                             metadata_->Size());
  return metadata == std::to_string(ray::rpc::ErrorType::OBJECT_IN_PLASMA);
}

}  // namespace ray
