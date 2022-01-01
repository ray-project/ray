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
#include "rust/ray-rs-sys/cpp/tasks.h"
#include "rust/ray-rs-sys/src/lib.rs.h"

int main(int argc, char **argv) {
  RAY_LOG(INFO) << "RUST default worker started.";
  ray::SetConfigToWorker();
  ray::load_code_paths_from_cmdline(argc, (int8_t**)argv);
  ray::RayConfig config;
  ray::Init(config, ray::internal::ExecuteTask, argc, argv);
  ::ray::core::CoreWorkerProcess::RunTaskExecutionLoop();
  return 0;
}
