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

#pragma once

// Python.h must come first: it defines both PY_VERSION_HEX and MS_WINDOWS, which
// the guard below tests. Including it later would silently compile the no-op
// fallback on every platform.
#include <Python.h>

#include <cstddef>

#include "ray/core_worker/task_execution/fiber.h"

#if PY_VERSION_HEX >= 0x030E0000 && !defined(MS_WINDOWS)
#include <dlfcn.h>
#ifndef RTLD_DEFAULT
#define RTLD_DEFAULT ((void *)0)
#endif
#endif

namespace ray {
namespace core {

/**
 * Points CPython's C-stack-overflow detection at the Boost fiber stack that the
 * calling thread is currently running on.
 *
 * CPython 3.14 replaced its recursion counter with a comparison of the live
 * stack pointer against the bounds recorded for the thread. Async-actor tasks
 * run on Boost fiber stacks that CPython knows nothing about, so those checks
 * treat the stack as already overflowed. `_Py_Dealloc` then defers every
 * GC-object deallocation to a list that Ray never drains, leaking the whole
 * object graph of each task.
 *
 * Call this at async-actor task entry and again after `YieldCurrentFiber`
 * returns: concurrent fibers share one thread state, so a resumed fiber may find
 * the bounds pointing at whichever fiber ran in the meantime.
 *
 * Requires the GIL. Never raises: a failure to install the bounds is reported
 * through the log, because the only consequence is that the leak returns.
 *
 * This is a no-op before CPython 3.14, on Windows, and on CPython 3.14.0/3.14.1,
 * where `PyUnstable_ThreadState_SetStackProtection` does not exist yet. The
 * symbol is resolved with `dlsym` rather than linked, so one build works across
 * all 3.14 patch releases.
 */
inline void ReanchorStackProtectionToCurrentFiberStack() {
#if PY_VERSION_HEX >= 0x030E0000 && !defined(MS_WINDOWS)
  using SetStackProtectionFn = int (*)(PyThreadState *, void *, size_t);
  static SetStackProtectionFn set_stack_protection =
      reinterpret_cast<SetStackProtectionFn>(
          dlsym(RTLD_DEFAULT, "PyUnstable_ThreadState_SetStackProtection"));
  if (set_stack_protection == nullptr) {
    return;
  }

  void *stack_start_addr = nullptr;
  size_t stack_size = 0;
  if (!FiberState::GetCurrentFiberStackBounds(&stack_start_addr, &stack_size)) {
    // Both call sites are meant to run on a tracked fiber stack, so reaching
    // here means the caller drifted off the fiber or onto an untracked one.
    // Leaving the bounds alone is safe, but the leak this guards against comes
    // back, so make that visible rather than silent.
    RAY_LOG_EVERY_N(WARNING, 1000)
        << "Async actor task is not running on a tracked fiber stack; leaving "
           "CPython stack protection unchanged. On Python 3.14+ this "
           "reintroduces a per-task memory leak in async actors.";
    return;
  }

  if (set_stack_protection(PyThreadState_Get(), stack_start_addr, stack_size) < 0) {
    // Only fails for sizes below _PyOS_MIN_STACK_SIZE, which Ray's fiber stacks
    // are comfortably above. Clear it regardless so no exception escapes.
    PyErr_Clear();
  }
#endif
}

}  // namespace core
}  // namespace ray
