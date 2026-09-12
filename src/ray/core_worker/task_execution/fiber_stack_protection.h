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
#include <optional>

#include "ray/core_worker/task_execution/fiber.h"

#if PY_VERSION_HEX >= 0x030E0000 && !defined(MS_WINDOWS)
#include <dlfcn.h>
#ifndef RTLD_DEFAULT
#define RTLD_DEFAULT ((void *)0)
#endif
#endif

namespace ray {
namespace core {

/** Signature of CPython's `PyUnstable_ThreadState_SetStackProtection`. */
using SetStackProtectionFn = int (*)(PyThreadState *, void *, size_t);

/** @brief Result of a single attempt to re-anchor stack protection. */
enum class ReanchorOutcome {
  /** The caller is not on a tracked fiber stack, so nothing was changed. */
  kNotOnFiberStack,
  /** CPython refused the bounds. The caller must clear the pending error. */
  kRejected,
  /** The fiber's bounds are now installed on the thread state. */
  kApplied,
};

/**
 * @brief Installs the current fiber's stack bounds through
 * @p set_stack_protection.
 *
 * Split out from the entry points below so it can be tested:
 * it touches no interpreter state beyond the arguments it is handed, so a test
 * can pass a stub and a dummy thread state and assert on the bounds that would
 * reach CPython. It is also compiled on every Python version, unlike the caller.
 *
 * @param set_stack_protection CPython entry point. Must not be nullptr;
 *   resolving the symbol is the caller's responsibility.
 * @param thread_state Thread state to update; passed through untouched.
 * @return Which branch was taken; see ReanchorOutcome.
 */
inline ReanchorOutcome ApplyCurrentFiberStackProtection(
    SetStackProtectionFn set_stack_protection, PyThreadState *thread_state) {
  void *stack_start_addr = nullptr;
  size_t stack_size = 0;
  if (!FiberState::GetCurrentFiberStackBounds(&stack_start_addr, &stack_size)) {
    // Deliberately silent: whether being off-fiber is a problem depends on the
    // caller. It is expected after a yield on the regular task-execution thread
    // and a bug at async-actor task entry, so the callers below decide.
    return ReanchorOutcome::kNotOnFiberStack;
  }

  if (set_stack_protection(thread_state, stack_start_addr, stack_size) < 0) {
    return ReanchorOutcome::kRejected;
  }
  return ReanchorOutcome::kApplied;
}

namespace internal {

/**
 * @brief Re-anchors this thread's Python stack protection to its active fiber.
 *
 * CPython 3.14 replaced its recursion counter with a comparison of the live
 * stack pointer against the bounds recorded for the thread. Async-actor tasks
 * run on Boost fiber stacks that CPython knows nothing about, so those checks
 * treat the stack as already overflowed. `_Py_Dealloc` then defers every
 * GC-object deallocation to a list that Ray never drains, leaking the whole
 * object graph of each task.
 *
 * Requires the GIL. Never raises; any error CPython reports is cleared here.
 *
 * @return The outcome, or an empty optional when re-anchoring does not apply at
 *   all: before CPython 3.14, on Windows, and on CPython 3.14.0/3.14.1, which
 *   lack `PyUnstable_ThreadState_SetStackProtection`. The symbol is resolved
 *   with `dlsym` rather than linked, so one build covers every 3.14 release.
 */
inline std::optional<ReanchorOutcome> TryReanchorStackProtection() {
#if PY_VERSION_HEX >= 0x030E0000 && !defined(MS_WINDOWS)
  static SetStackProtectionFn set_stack_protection =
      reinterpret_cast<SetStackProtectionFn>(
          dlsym(RTLD_DEFAULT, "PyUnstable_ThreadState_SetStackProtection"));
  if (set_stack_protection == nullptr) {
    // CPython 3.14.0 and 3.14.1 predate the API. Say so once: otherwise the
    // only symptom is unbounded memory growth in async actors, with nothing
    // in the logs to point at the cause.
    static bool warned_missing_api = false;
    if (!warned_missing_api) {
      warned_missing_api = true;
      RAY_LOG(WARNING) << "PyUnstable_ThreadState_SetStackProtection is not available in "
                       << "this interpreter (Python " << PY_VERSION
                       << "). Async actor tasks will leak memory on Python 3.14; upgrade "
                       << "to CPython 3.14.2 or later. See "
                       << "https://github.com/ray-project/ray/issues/63290";
    }
    return std::nullopt;
  }

  const ReanchorOutcome outcome =
      ApplyCurrentFiberStackProtection(set_stack_protection, PyThreadState_Get());
  if (outcome == ReanchorOutcome::kRejected) {
    // Only happens for sizes below _PyOS_MIN_STACK_SIZE, which Ray's fiber
    // stacks are comfortably above. Clear it regardless so nothing escapes.
    PyErr_Clear();
  }
  return outcome;
#else
  return std::nullopt;
#endif
}

}  // namespace internal

/**
 * @brief Re-anchors stack protection at async-actor task entry, where the
 * caller is required to be running on a fiber.
 *
 * Every actor task on an async actor is dispatched through FiberState, so
 * failing to find a fiber stack here means the bounds were left pointing
 * somewhere else and the per-task leak is back. That is worth reporting.
 *
 * Requires the GIL. Do not use for actor creation tasks, which run on the
 * regular task-execution thread; see ReanchorStackProtectionAfterFiberYield.
 */
inline void ReanchorStackProtectionForAsyncActorTask() {
  if (internal::TryReanchorStackProtection() == ReanchorOutcome::kNotOnFiberStack) {
    RAY_LOG_EVERY_N(WARNING, 1000)
        << "Async actor task is not running on a tracked fiber stack; leaving "
           "CPython stack protection unchanged. On Python 3.14+ this "
           "reintroduces a per-task memory leak in async actors.";
  }
}

/**
 * @brief Re-anchors stack protection after a fiber yield, restoring this
 * fiber's bounds before Python code resumes on it.
 *
 * Concurrent fibers share one thread state, so a resumed fiber may find the
 * bounds pointing at whichever fiber ran while it was parked.
 *
 * Being off-fiber is legitimate here and is a silent no-op: an async actor's
 * creation task yields on the regular task-execution thread before any
 * FiberState exists, and CPython's own bounds are already correct there.
 *
 * Requires the GIL.
 */
inline void ReanchorStackProtectionAfterFiberYield() {
  internal::TryReanchorStackProtection();
}

}  // namespace core
}  // namespace ray
