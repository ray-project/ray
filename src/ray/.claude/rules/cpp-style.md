---
paths:
  - "src/ray/**/*.h"
  - "src/ray/**/*.cc"
---
<!-- C++ core code style — derived from docs.ray.io/en/master/ray-contribute/getting-involved.html -->
<!-- Source of truth: Google C++ style guide + project-specific conventions -->
- Follow Google C++ style guide
- C++ standard: C++17
- Use absl::Mutex and absl::MutexLock, not std::mutex/std::lock_guard
- Use absl::StrCat, absl::StrJoin, absl::StrFormat for string operations, not std::string +/+=
- Formatting enforced via clang-format (run ./ci/lint/check-format.sh --fix)
- Debug processes via RAY_{PROCESS_NAME}_{DEBUGGER}=1 env vars (gdb requires tmux: RAY_RAYLET_GDB=1 RAY_RAYLET_TMUX=1)
- Set RAY_BACKEND_LOG_LEVEL=debug for verbose task execution and object transfer logs
- Set RAY_event_stats=1 to dump ASIO event handler stats to debug_state.txt
