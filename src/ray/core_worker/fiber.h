#ifndef RAY_CORE_WORKER_FIBER_H
#define RAY_CORE_WORKER_FIBER_H

#include <ray/util/logging.h>
#include <boost/fiber/all.hpp>
namespace ray {

/// Used by async actor mode. The fiber event will be used
/// from python to switch control among different coroutines.
/// Taken from boost::fiber examples
/// https://github.com/boostorg/fiber/blob/7be4f860e733a92d2fa80a848dd110df009a20e1/examples/wait_stuff.cpp#L115-L142
class FiberEvent {
 public:
  // Block the fiber until the event is notified.
  void Wait() {
    std::unique_lock<boost::fibers::mutex> lock(mutex_);
    cond_.wait(lock, [this]() { return ready_; });
  }

  // Notify the event and unblock all waiters.
  void Notify() {
    {
      std::unique_lock<boost::fibers::mutex> lock(mutex_);
      ready_ = true;
    }
    cond_.notify_one();
  }

 private:
  boost::fibers::condition_variable cond_;
  boost::fibers::mutex mutex_;
  bool ready_ = false;
};

/// Used by async actor mode. The FiberRateLimiter is a barrier that
/// allows at most num fibers running at once. It implements the
/// semaphore data structure.
class FiberRateLimiter {
 public:
  FiberRateLimiter(int num) : num_(num) {}

  // Enter the semaphore. Wait fo the value to be > 0 and decrement the value.
  void Acquire() {
    std::unique_lock<boost::fibers::mutex> lock(mutex_);
    cond_.wait(lock, [this]() { return num_ > 0; });
    num_ -= 1;
  }

  // Exit the semaphore. Increment the value and notify other waiter.
  void Release() {
    {
      std::unique_lock<boost::fibers::mutex> lock(mutex_);
      num_ += 1;
    }
    // TODO(simon): This not does guarantee to wake up the first queued fiber.
    // This could be a problem for certain workloads because there is no guarantee
    // on task ordering .
    cond_.notify_one();
  }

 private:
  boost::fibers::condition_variable cond_;
  boost::fibers::mutex mutex_;
  int num_ = 1;
};

using FiberChannel = boost::fibers::unbuffered_channel<std::function<void()>>;

class FiberState {
 public:
  FiberState(int max_concurrency) : rate_limiter_(max_concurrency) {
    fiber_runner_thread_ = std::thread([&]() {
      while (!channel_.is_closed()) {
        std::function<void()> func;
        auto op_status = channel_.pop(func);
        if (op_status == boost::fibers::channel_op_status::success) {
          boost::fibers::fiber(boost::fibers::launch::dispatch, func).detach();
        } else {  // Errored
          RAY_LOG(ERROR) << "Fiber worker popped errrored. Exiting";
          return;
        }
      }
      // The event here is used to make sure fiber_runner_thread_ never terminates.
      // Because fiber_shutdown_event_ is never notified, fiber_runner_thread_ will
      // immediately start working on any ready fibers.
      shutdown_worker_event_.Wait();
    });
  }

  void EnqueueFiber(std::function<void()> &&callback) {
    auto op_status = channel_.push([this, callback]() {
      rate_limiter_.Acquire();
      callback();
      rate_limiter_.Release();
    });
    RAY_CHECK(op_status == boost::fibers::channel_op_status::success);
  }

  ~FiberState() {
    channel_.close();
    shutdown_worker_event_.Notify();
    if (fiber_runner_thread_.joinable()) {
      fiber_runner_thread_.join();
    }
  }

 private:
  FiberChannel channel_;
  /// The fiber semaphore used to limit the number of concurrent fibers
  /// running at once.
  FiberRateLimiter rate_limiter_;
  /// The fiber event used to block fiber_runner_thread_ from shutdown.
  /// is_asyncio_ must be true.
  FiberEvent shutdown_worker_event_;
  /// The thread that runs all asyncio fibers. is_asyncio_ must be true.
  std::thread fiber_runner_thread_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_FIBER_H