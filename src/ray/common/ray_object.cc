#include "ray/common/ray_object.h"

namespace ray {

bool RayObject::IsException() {
  if (metadata_ == nullptr) {
    return false;
  }
  // TODO(ekl) properly detect the RAW metadata tag, it seems to get lost.
  // THe numbers below are all 1 in length so this doesn't impact correctness.
  if (metadata_->Size() == 3) {
    return false;
  }
  // TODO (kfstorm): metadata should be structured.
  const std::string metadata(reinterpret_cast<const char *>(metadata_->Data()),
                             metadata_->Size());
  const auto error_type_descriptor = ray::rpc::ErrorType_descriptor();
  for (int i = 0; i < error_type_descriptor->value_count(); i++) {
    const auto error_type_number = error_type_descriptor->value(i)->number();
    if (metadata == std::to_string(error_type_number)) {
      return true;
    }
  }
  return false;
}

}  // namespace ray
