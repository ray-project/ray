// Copyright 2025 The Ray Authors.
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

// src/ray/core_worker/lib/go/gcs_client_tasks.cc
// GCS Client CGO bridge - Task events operations
#include <cstdlib>
#include <string>
#include <vector>

#include "ray/core_worker/lib/go/gcs_client_bridge.h"
#include "ray/core_worker/lib/go/gcs_client_internal.h"
#include "ray/core_worker/lib/go/gcs_client_utils.h"
#include "ray/gcs_rpc_client/global_state_accessor.h"

extern "C" {

int ray_gcs_client_task_events_get_all(CGcsClient *client,
                                       char ***serialized_out,
                                       int **sizes_out,
                                       int *count_out,
                                       char **error_out) {
  if (!client || !client->global_state_accessor || !serialized_out || !sizes_out ||
      !count_out) {
    set_error(error_out, "Invalid arguments");
    return 0;
  }
  try {
    std::vector<std::string> task_events =
        client->global_state_accessor->GetAllTaskEvents();
    if (!allocate_serialized_array(task_events, serialized_out, sizes_out, count_out)) {
      set_error(error_out, "Failed to allocate memory for serialized array");
      return 0;
    }
    return 1;
  } catch (const std::exception &e) {
    set_error(error_out, e.what());
    return 0;
  }
}

}  // extern "C"
