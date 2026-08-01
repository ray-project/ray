// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <boost/fiber/all.hpp>
#include <chrono>
#include <memory>
#include <thread>
#include <utility>

#include "ray/util/logging.h"
#include "ray/util/macros.h"

namespace ray {
namespace core {

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
  explicit FiberRateLimiter(int num) : num_(num) {}

  // Enter the semaphore. Wait for the value to be > 0 and decrement the value.
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
    // NOTE(simon): This not does guarantee to wake up the first queued fiber.
    // This could be a problem for certain workloads because there is no guarantee
    // on task ordering.
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
  static bool NeedDefaultExecutor(int32_t max_concurrency_in_default_group,
                                  bool has_other_concurrency_groups) {
    RAY_UNUSED(max_concurrency_in_default_group);
    RAY_UNUSED(has_other_concurrency_groups);
    /// asyncio mode always need a default executor.
    return true;
  }

  explicit FiberState(
      int max_concurrency,
      // TODO(kevin85421): The language-specific callback function that
      // initializes threads. It's not currently used in the async mode.
      std::function<std::function<void()>()> initialize_thread_callback = nullptr)
      : allocator_(kStackSize), rate_limiter_(max_concurrency) {
    fiber_runner_thread_ = std::thread([this]() {
      while (!channel_.is_closed()) {
        std::function<void()> func;
        auto op_status = channel_.pop(func);
        if (op_status == boost::fibers::channel_op_status::success) {
          // Increment in-flight count before launching a fiber, this is called on
          // the main runner thread as this way we avoid a race where a fiber is submitted
          // but before it starts execution, num_in_flight_fibers_ value is checked and
          // observed to be 0 and thread shuts down.
          {
            std::unique_lock<boost::fibers::mutex> lock(mutex_);
            num_in_flight_fibers_ += 1;
          }
          boost::fibers::fiber(boost::fibers::launch::dispatch,
                               std::allocator_arg,
                               allocator_,
                               [this, func = std::move(func)]() {
                                 func();
                                 // Decrement the in-flight counter once the
                                 // fiber body has finished and notify the
                                 // graceful drain. `func` (the EnqueueFiber
                                 // wrapper) does not throw -- an uncaught
                                 // exception in a fiber would terminate the
                                 // process -- so this always runs.
                                 std::unique_lock<boost::fibers::mutex> lock(mutex_);
                                 num_in_flight_fibers_ -= 1;
                                 if (num_in_flight_fibers_ == 0) {
                                   // The runner thread's main fiber (in the
                                   // graceful drain below) is the only waiter.
                                   fibers_drained_event_.notify_one();
                                 }
                               })
              .detach();
        } else if (op_status == boost::fibers::channel_op_status::closed) {
          // The channel was closed. We will just exit the loop and finish
          // cleanup.
          break;
        } else {
          RAY_LOG(ERROR)
              << "Async actor fiber channel returned unexpected error code, "
              << "shutting down the worker thread. Please submit a github issue "
              << "at https://github.com/ray-project/ray";
          break;
        }
      }

      // Graceful drain: wait for all in-flight fibers to finish before stopping.
      // This keeps the fiber scheduler running so that in-flight coroutines can
      // complete on their (still-running) asyncio event loops, resume their
      // parked boost fibers, and store their task outputs -- so callers receive
      // results instead of ActorDiedError during graceful shutdown.
      //
      // The wait uses boost fiber primitives so it yields to the scheduler,
      // letting parked fibers be resumed as their coroutines complete. Like
      // BoundedExecutor::Join() for threaded actors, this waits indefinitely;
      // if an in-flight task never completes the worker hangs here until the
      // raylet force-kills it (matching threaded-actor behavior).
      {
        std::unique_lock<boost::fibers::mutex> lock(mutex_);
        if (num_in_flight_fibers_ > 0) {
          RAY_LOG(INFO) << "Async actor is draining " << num_in_flight_fibers_
                        << " in-flight task(s) before exiting. If this message is the "
                           "last one printed, the worker is probably hanging because an "
                           "in-flight async task never completes.";
        }
        fibers_drained_event_.wait(lock, [this]() { return num_in_flight_fibers_ == 0; });
      }

      // All fibers have completed, so no fiber can run after this point and it
      // is safe for this thread to finish (and be joined).
    });
  }

  ~FiberState() {
    // Make sure the runner thread is stopped and joined before members are
    // destroyed. No-op if Stop()/Join() were already called.
    Stop();
    Join();
  }

  void EnqueueFiber(std::function<void()> &&callback) {
    auto op_status = channel_.push([this, callback = std::move(callback)]() {
      rate_limiter_.Acquire();
      callback();
      rate_limiter_.Release();
    });
    RAY_CHECK(op_status == boost::fibers::channel_op_status::success);
  }

  void Stop() { channel_.close(); }

  void Join() {
    if (fiber_runner_thread_.joinable()) {
      fiber_runner_thread_.join();
    }
  }

 private:
  static constexpr size_t kStackSize = 1024 * 256;

  // The fiber stack allocator.
  boost::fibers::fixedsize_stack allocator_;
  /// The fiber channel used to send task between the submitter thread
  /// (main direct_actor_trasnport thread) and the fiber_runner_thread
  FiberChannel channel_;
  /// The fiber semaphore used to limit the number of concurrent fibers
  /// running at once.
  FiberRateLimiter rate_limiter_;
  /// Bookkeeping for tracking the number of in-flight fibers and waiting for
  /// them to finish during graceful shutdown. All accesses happen on
  /// `fiber_runner_thread_` (its main fiber and the task fibers it launches are
  /// cooperatively scheduled on that one kernel thread), so the mutex is not
  /// needed for mutual exclusion; it exists only because
  /// boost::fibers::condition_variable requires one to wait on.
  boost::fibers::mutex mutex_;
  boost::fibers::condition_variable fibers_drained_event_;
  int num_in_flight_fibers_ = 0;
  /// The thread that runs the fibers. It pops tasks off `channel_` and runs
  /// each as a fiber, then (after Stop() closes the channel) drains in-flight
  /// fibers before finishing. Join() joins it, so it never outlives this object.
  std::thread fiber_runner_thread_;
};

}  // namespace core
}  // namespace ray
