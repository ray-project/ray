#pragma once

#include <chrono>
#include <future>
#include <thread>
#include "ray/util/util.h"

namespace ray {
namespace streaming {

/// Helper class encapulate std::future to help multithread async wait.
class PromiseWrapper {
 public:
  Status Wait() {
    std::future<bool> fut = promise_.get_future();
    fut.get();
    return status_;
  }

  Status WaitFor(uint64_t timeout_ms) {
    std::future<bool> fut = promise_.get_future();
    std::future_status status;
    do {
      status = fut.wait_for(std::chrono::milliseconds(timeout_ms));
      if (status == std::future_status::deferred) {
      } else if (status == std::future_status::timeout) {
        return Status::Invalid("timeout");
      } else if (status == std::future_status::ready) {
        return status_;
      }
    } while (status == std::future_status::deferred);

    return status_;
  }

  void Notify(Status status) {
    status_ = status;
    promise_.set_value(true);
  }

  Status GetResultStatus() { return status_; }

 private:
  std::promise<bool> promise_;
  Status status_;
};

}  // namespace streaming
}  // namespace ray
