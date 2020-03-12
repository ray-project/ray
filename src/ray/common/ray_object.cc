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

#include "ray/common/ray_object.h"

#include "ray/util/test_util.h"

namespace ray {

std::shared_ptr<LocalMemoryBuffer> MakeErrorMetadataBuffer(rpc::ErrorType error_type) {
  std::string meta = std::to_string(static_cast<int>(error_type));
  auto metadata = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(meta.data()));
  auto meta_buffer =
      std::make_shared<LocalMemoryBuffer>(metadata, meta.size(), /*copy_data=*/true);
  return meta_buffer;
}

RayObject::RayObject(rpc::ErrorType error_type)
    : RayObject(nullptr, MakeErrorMetadataBuffer(error_type), {}) {}

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

std::shared_ptr<Buffer> GenerateRandomBuffer() {
  auto seed = std::chrono::high_resolution_clock::now().time_since_epoch().count();
  std::mt19937 gen(seed);
  std::uniform_int_distribution<> dis(1, 10);
  std::uniform_int_distribution<> value_dis(1, 255);

  std::vector<uint8_t> arg1(dis(gen), value_dis(gen));
  return std::make_shared<LocalMemoryBuffer>(arg1.data(), arg1.size(), true);
}

std::shared_ptr<RayObject> GenerateRandomObject(
    const std::vector<ObjectID> &inlined_ids) {
  return std::shared_ptr<RayObject>(
      new RayObject(GenerateRandomBuffer(), nullptr, inlined_ids));
}

}  // namespace ray
