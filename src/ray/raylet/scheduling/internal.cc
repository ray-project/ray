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

#include "ray/raylet/scheduling/internal.h"

namespace ray {
namespace raylet {

namespace internal {

std::string UnscheduledWorkCauseToString(UnscheduledWorkCause cause) {
  switch (cause) {
  case UnscheduledWorkCause::WAITING_FOR_RESOURCE_ACQUISITION:
    return "Queued";
  case UnscheduledWorkCause::WAITING_FOR_AVAILABLE_PLASMA_MEMORY:
    return "Waiting for object store memory";
  case UnscheduledWorkCause::WAITING_FOR_RESOURCES_AVAILABLE:
    return "Waiting for resource available";
  case UnscheduledWorkCause::WORKER_NOT_FOUND_JOB_CONFIG_NOT_EXIST:
    return "Waiting for job config";
  case UnscheduledWorkCause::WORKER_NOT_FOUND_REGISTRATION_TIMEOUT:
    return "Worker registration failed";
  case UnscheduledWorkCause::WORKER_NOT_FOUND_RATE_LIMITED:
    return "Rate limited";
  case UnscheduledWorkCause::INFEASIBLE:
    return "Infeasible";
  default:
    RAY_LOG(FATAL) << "Unreachable";
  }
}

}  // namespace internal

}  // namespace raylet

}  // namespace ray