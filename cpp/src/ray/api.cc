// Copyright 2020-2021 The Ray Authors.
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

#include <ray/api.h>

#include "config_internal.h"
#include "runtime/abstract_ray_runtime.h"

namespace ray {
namespace api {

std::once_flag Ray::is_inited_;

void Ray::Init(RayConfig &config, int *argc, char ***argv) {
  ConfigInternal::Instance().Init(config, argc, argv);
  Init();
}

void Ray::Init(RayConfig &config) { Init(config, nullptr, nullptr); }

void Ray::Init() {
  std::call_once(is_inited_, [] {
    auto runtime = AbstractRayRuntime::DoInit();
    ray::internal::RayRuntimeHolder::Instance().Init(runtime);
  });
}

void Ray::Shutdown() { AbstractRayRuntime::DoShutdown(); }

}  // namespace api
}  // namespace ray