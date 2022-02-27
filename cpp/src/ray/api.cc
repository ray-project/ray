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

static bool is_init_ = false;

void Init(ray::RayConfig &config, int argc, char **argv) {
  if (!IsInitialized()) {
    internal::ConfigInternal::Instance().Init(config, argc, argv);
    auto runtime = internal::AbstractRayRuntime::DoInit();
    is_init_ = true;
  }
}

void Init(ray::RayConfig &config) { Init(config, 0, nullptr); }

void Init() {
  RayConfig config;
  Init(config, 0, nullptr);
}

bool IsInitialized() { return is_init_; }

void Shutdown() {
  // TODO(SongGuyang): Clean the ray runtime.
  internal::AbstractRayRuntime::DoShutdown();
  is_init_ = false;
}

}  // namespace ray
