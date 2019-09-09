#include "ray/core_worker/store_provider/store_provider.h"

namespace ray {

bool IsException(const std::shared_ptr<RayObject> &object) {
  // TODO (kfstorm): metadata should be structured.
  const std::string metadata(reinterpret_cast<const char *>(object->GetMetadata()->Data()),
                             object->GetMetadata()->Size());
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
