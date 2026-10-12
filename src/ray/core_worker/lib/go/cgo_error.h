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

#ifndef SRC_RAY_CORE_WORKER_LIB_GO_CGO_ERROR_H_
#define SRC_RAY_CORE_WORKER_LIB_GO_CGO_ERROR_H_

#include <cstring>

// Sets *error to a strdup'd copy of msg when error is non-null. Shared by the
// cgo boundary translation units (metric_ops.cc, placement_group_ops.cc, ...)
// so a caller that passes NULL never gets a spurious dereference.
// inline keeps internal linkage per translation unit, so multiple extern "C"
// boundaries can use it without a link-time collision.
inline void SetBoundaryError(char **error, const char *msg) {
  if (error != nullptr) {
    *error = strdup(msg);
  }
}

#endif  // SRC_RAY_CORE_WORKER_LIB_GO_CGO_ERROR_H_
