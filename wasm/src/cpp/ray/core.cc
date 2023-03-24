// Copyright 2020-2023 The Ray Authors.
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

#include "ray/core.h"

#include "ray/core_worker/core_worker.h"

#ifdef __cplusplus
extern "C" {
#endif

void CoreWorkerProcess_Initialize(CoreWorkerOptions *options) {
  ray::core::CoreWorkerOptions o;
  o.worker_type = static_cast<ray::core::WorkerType>(options->worker_type);
  // TODO:
  ray::core::CoreWorkerProcess::Initialize(o);
}

#ifdef __cplusplus
}
#endif
