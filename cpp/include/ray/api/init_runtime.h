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

#include <ray/api/ray_runtime.h>

namespace ray {
namespace api {

struct InitRuntime {
  static InitRuntime &Instance() {
    static InitRuntime instance;
    return instance;
  }

  void Init(std::shared_ptr<RayRuntime> runtime) { runtime_ = runtime; }

  std::shared_ptr<RayRuntime> &Runtime() { return runtime_; }

 private:
  InitRuntime() = default;
  InitRuntime(InitRuntime const &) = delete;
  InitRuntime(InitRuntime &&) = delete;
  InitRuntime &operator=(InitRuntime const &) = delete;
  InitRuntime &operator=(InitRuntime &&) = delete;

  std::shared_ptr<RayRuntime> runtime_;
};
}  // namespace api
}  // namespace ray