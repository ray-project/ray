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
#include "io_ray_runtime_util_SystemConfig.h"

#include <jni.h>

#include "jni_utils.h"  // NOLINT(build/include_subdir)
#include "nlohmann/json.hpp"
#include "ray/util/logging.h"

using json = nlohmann::json;

JNIEXPORT jstring JNICALL Java_io_ray_runtime_util_SystemConfig_nativeGetSystemConfig(
    JNIEnv *env, jclass clz, jstring java_key) {
  RAY_CHECK(java_key != nullptr);
  const auto key = JavaStringToNativeString(env, java_key);

  /// -----------Include ray_config_def.h to set config items.-------------------
#define RAY_CONFIG(type, name, default_value)   \
  if (key == #name) {                           \
    json j = RayConfig::instance().name();      \
    return env->NewStringUTF(j.dump().c_str()); \
  }

#include "ray/common/ray_config_def.h"
/// ---------------------------------------------------------------------
#undef RAY_CONFIG
  RAY_LOG(FATAL) << "Unsupported system config: " << key;
  return nullptr;
}
