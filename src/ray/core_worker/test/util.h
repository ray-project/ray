#ifndef RAY_CORE_WORKER_TEST_UTIL_H
#define RAY_CORE_WORKER_TEST_UTIL_H

#include <string>

namespace ray {

bool WaitForCondition(std::function<bool()> condition, int timeout_ms) {
  int wait_time = 0;
  while (true) {
    if (condition()) {
      return true;
    }

    // sleep 100ms.
    const int wait_interval_ms = 100;
    usleep(wait_interval_ms * 1000);
    wait_time += wait_interval_ms;
    if (wait_time > timeout_ms) {
      break;
    }
  }
  return false;
}

}  // namespace ray

#endif  // RAY_CORE_WORKER_TEST_UTIL_H
