#include "queue/utils.h"

namespace ray {
namespace streaming {

Status PromiseWrapper::WaitFor(uint64_t timeout_ms) {
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

}  // namespace streaming
}  // namespace ray
