#ifndef RAY_UTIL_TEST_UTIL_H
#define RAY_UTIL_TEST_UTIL_H

#include <string>

#include "ray/util/util.h"

namespace ray {

/// Wait until the condition is met, or timeout is reached.
///
/// \param[in] condition The condition to wait for.
/// \param[in] timeout_ms Timeout in milliseconds to wait for for.
/// \return Whether the condition is met.
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

// A helper function to return a random task id.
inline TaskID RandomTaskId() {
  std::string data(TaskID::Size(), 0);
  FillRandom(&data);
  return TaskID::FromBinary(data);
}

}  // namespace ray

#endif  // RAY_UTIL_TEST_UTIL_H
