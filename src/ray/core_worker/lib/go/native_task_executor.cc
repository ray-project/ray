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

// CGO wrapper implementation for TaskExecutor functions.
// This file contains all TaskExecutor-related CGO functions, separated from
// native_runtime.cc for better modularity and maintainability.
// Pattern: Similar to io_ray_runtime_task_NativeTaskExecutor.cc in Java runtime.

#include "ray/core_worker/lib/go/native_task_executor.h"

#include <atomic>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "ray/common/id.h"
#include "ray/common/ray_object.h"
#include "ray/common/task/task_common.h"
#include "ray/common/task/task_spec.h"
#include "ray/common/task/task_util.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/core_worker_process.h"
#include "ray/core_worker/lib/go/cgo_wrapper.h"
#include "ray/core_worker/lib/go/task_argument.h"
#include "ray/core_worker/lib/go/task_executor_ops.h"
#include "ray/util/logging.h"

// ============================================================================
// Global State
// ============================================================================

namespace {

// Static variable to store the C callback pointer
// The callback type is defined in native_task_executor.h
static std::atomic<GoTaskExecutorCallback> g_go_task_executor_callback_static{nullptr};

// ============================================================================
// Helper Functions
// ============================================================================
}  // anonymous namespace

// ============================================================================
// CGO Exports - TaskExecutor Functions
// ============================================================================

// RegisterGoTaskExecutorCallback registers the Go task executor with C++.
// This function is called from Go code during runtime initialization.
// It wraps CNativeTaskExecutor_RegisterCallback and passes the GoExecuteTask callback.
extern "C" void RegisterGoTaskExecutorCallback() {
  CNativeTaskExecutor_RegisterCallback(GoExecuteTask);
}

// Register Go task executor callback
// This function is called from Go to register the task executor callback
extern "C" void CNativeTaskExecutor_RegisterCallback(GoTaskExecutorCallback callback) {
  g_go_task_executor_callback_static.store(callback);
  RAY_LOG(INFO) << "Go task executor callback registered";
}
