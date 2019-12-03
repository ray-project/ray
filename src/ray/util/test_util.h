#ifndef RAY_UTIL_TEST_UTIL_H
#define RAY_UTIL_TEST_UTIL_H

#include <string>

#include "ray/common/buffer.h"
#include "ray/common/ray_object.h"
#include "ray/util/util.h"

namespace ray {

// Magic argument to signal to mock_worker we should check message order.
int64_t SHOULD_CHECK_MESSAGE_ORDER = 123450000;

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

    // sleep 10ms.
    const int wait_interval_ms = 10;
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

std::shared_ptr<Buffer> GenerateRandomBuffer() {
  auto seed = std::chrono::high_resolution_clock::now().time_since_epoch().count();
  std::mt19937 gen(seed);
  std::uniform_int_distribution<> dis(1, 10);
  std::uniform_int_distribution<> value_dis(1, 255);

  std::vector<uint8_t> arg1(dis(gen), value_dis(gen));
  return std::make_shared<LocalMemoryBuffer>(arg1.data(), arg1.size(), true);
}

std::shared_ptr<RayObject> GenerateRandomObject() {
  return std::shared_ptr<RayObject>(new RayObject(GenerateRandomBuffer(), nullptr));
}

}  // namespace ray

#endif  // RAY_UTIL_TEST_UTIL_H
