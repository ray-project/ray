#ifndef _STREAMING_QUEUE_UTILS_H_
#define _STREAMING_QUEUE_UTILS_H_
#include <chrono>
#include <future>
#include <thread>
#include "ray/util/util.h"

namespace ray {
namespace streaming {

/// Helper class encapulate std::future to help multithread async wait.
class PromiseWrapper {
 public:
  Status Wait();

  Status WaitFor(uint64_t timeout_ms);

  void Notify(Status status);

  Status GetResultStatus();

 private:
  std::promise<bool> promise_;
  Status status_;
};

}  // namespace streaming
}  // namespace ray
#endif
