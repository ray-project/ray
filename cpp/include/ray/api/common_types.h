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

namespace ray {
namespace api {

enum ErrorType : int {
  WORKER_DIED = 0,
  ACTOR_DIED = 1,
  OBJECT_UNRECONSTRUCTABLE = 2,
  TASK_EXECUTION_EXCEPTION = 3,
  OBJECT_IN_PLASMA = 4,
  TASK_CANCELLED = 5,
  ACTOR_CREATION_FAILED = 6,
};

enum WorkerType : int {
  WORKER = 0,
  DRIVER = 1,
  SPILL_WORKER = 2,
  RESTORE_WORKER = 3,
};

enum Language : int {
  PYTHON = 0,
  JAVA = 1,
  CPP = 2,
};

enum TaskType : int {
  NORMAL_TASK = 0,
  ACTOR_CREATION_TASK = 1,
  ACTOR_TASK = 2,
  DRIVER_TASK = 3,
};

}  // namespace api
}  // namespace ray