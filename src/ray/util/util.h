#ifndef RAY_UTIL_UTIL_H
#define RAY_UTIL_UTIL_H

#include <chrono>

/// Return the number of milliseconds since the Unix epoch.
///
/// TODO(rkn): This function appears in multiple places. It should be
/// deduplicated.
///
/// \return The number of milliseconds since the Unix epoch.
int64_t current_time_ms() {
  std::chrono::milliseconds ms_since_epoch =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now().time_since_epoch());
  return ms_since_epoch.count();
}

#endif  // RAY_UTIL_UTIL_H
