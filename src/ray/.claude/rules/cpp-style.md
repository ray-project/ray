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
