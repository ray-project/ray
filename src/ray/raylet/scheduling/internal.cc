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
    return "Task is queued. Task is waiting to be scheduled.";
  case UnscheduledWorkCause::WAITING_FOR_AVAILABLE_PLASMA_MEMORY:
    return "There's not enough memory to schedule this task. Task will be scheduled when "
           "the local object store memory is availabale.";
  case UnscheduledWorkCause::WAITING_FOR_RESOURCES_AVAILABLE:
    return "There's no available resources for this task across the cluster. Task will "
           "be scheduled once there are available resources from the cluster.";
  case UnscheduledWorkCause::WORKER_NOT_FOUND_JOB_CONFIG_NOT_EXIST:
    return "Failed to start a worker for the task because the corresponding job "
           "configuration wasn't found. It is usually due to a network related issue.";
  case UnscheduledWorkCause::WORKER_NOT_FOUND_REGISTRATION_TIMEOUT:
    return "Failed to start a worker for the task because the worker wasn't registered "
           "to raylet within the timeout. It is usually due to high pressure on GCS, "
           "slow network, or worker initialization failure.";
  case UnscheduledWorkCause::WORKER_NOT_FOUND_RATE_LIMITED:
    return "Failed to start a worker for the task because there are too many worker "
           "creation requests.";
  default:
    RAY_LOG(FATAL) << "Unreachable";
  }
}

}  // namespace internal

}  // namespace raylet

}  // namespace ray