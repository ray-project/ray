
#include "ray/util/exponential_backoff.h"

#include <math.h>

#include "ray/util/logging.h"

namespace ray {

uint64_t ExponentialBackoff::GetBackoffMs(uint64_t attempt,
                                          uint64_t base_ms,
                                          uint64_t max_attempt,
                                          uint64_t max_backoff_ms) {
  if (attempt > max_attempt) {
    attempt = max_attempt;
    RAY_LOG_EVERY_MS(INFO, 60000) << "Backoff attempt exceeded max, attempt= " << attempt
                                  << " max attempt= " << max_backoff_ms;
  }
  uint64_t delay = static_cast<uint64_t>(pow(2, attempt));
  return std::min(base_ms * delay, max_backoff_ms);
};

}  // namespace ray