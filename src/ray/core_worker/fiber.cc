#include "ray/core_worker/fiber.h"

namespace ray {

FiberState::FiberState(int max_concurrency) : rate_limiter_(max_concurrency) {
  fiber_runner_thread_ = std::thread([&]() {
    while (!channel_.is_closed()) {
      std::function<void()> func;
      auto op_status = channel_.pop(func);
      if (op_status == boost::fibers::channel_op_status::success) {
        boost::fibers::fiber(boost::fibers::launch::dispatch, func).detach();
      } else if (op_status == boost::fibers::channel_op_status::closed) {
        // The channel was closed. We will just exit the loop and finish
        // cleanup.
        break;
      } else {
        RAY_LOG(ERROR) << "Async actor fiber channel returned unexpected error code, "
                       << "shutting down the worker thread. Please submit a github issue "
                       << "at https://github.com/ray-project/ray";
        return;
      }
    }
    // The event here is used to make sure fiber_runner_thread_ never
    // terminates. Because fiber_shutdown_event_ is never notified,
    // fiber_runner_thread_ will immediately start working on any ready fibers.
    shutdown_worker_event_.Wait();
  });
}

}  // namespace ray
