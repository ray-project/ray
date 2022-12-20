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

#include "ray/core_worker/context.h"
#include "ray/core_worker/task_event_buffer.h"

namespace ray {
namespace core {

namespace worker {
class ProfileEvent {
 public:
  ProfileEvent(TaskEventBuffer &task_event_buffer,
               WorkerContext &worker_context,
               const std::string &node_ip_address,
               const std::string &event_type);

  // Set the end time for the event and add it to the profiler.
  ~ProfileEvent();

  // Set extra metadata for the event, which could change during the event.
  void SetExtraData(const std::string &extra_data);

 private:
  TaskEventBuffer &task_event_buffer_;

  rpc::TaskEvents rpc_profile_event_;
};

}  // namespace worker

}  // namespace core
}  // namespace ray
