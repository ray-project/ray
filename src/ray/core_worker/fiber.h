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

#include "ray/common/task/task_spec.h"
#include "ray/util/logging.h"

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
  FiberRateLimiter(int32_t num) : num_(num) {}

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
  int32_t num_ = 1;
};

using FiberChannel = boost::fibers::unbuffered_channel<std::function<void()>>;

// FiberState uses 1 thread to run all asyncio tasks. The tasks are executed in a round
// robin manner, subject to a list of rate limiters for each concurrent group, each
// function in those concurrent groups, and a default rate limiter.
class FiberState {
 public:
  FiberState(const std::vector<ConcurrencyGroup> &concurrency_groups,
             const int32_t max_concurrency_for_default_concurrency_group)
      : default_rate_limiter_(max_concurrency_for_default_concurrency_group) {
    for (const auto &group : concurrency_groups) {
      auto rate_limiter = std::make_shared<FiberRateLimiter>(group.max_concurrency);
      concurrency_group_rate_limiters_[group.name] = rate_limiter;
      for (const auto &fd : group.function_descriptors) {
        function_rate_limiters_[fd->ToString()] = rate_limiter;
      }
    }

    fiber_runner_thread_ =
        std::thread(
            [&]() {
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
                  RAY_LOG(ERROR)
                      << "Async actor fiber channel returned unexpected error code, "
                      << "shutting down the worker thread. Please submit a github issue "
                      << "at https://github.com/ray-project/ray";
                  return;
                }
              }

              // Boost fiber thread cannot be terminated and joined
              // if there are still running detached fibers.
              // When we exit async actor, we stop running coroutines
              // which means the corresponding waiting boost fibers
              // will never be resumed by done callbacks of those coroutines.
              // As a result, those fibers will never exit and the fiber
              // runner thread cannot be joined.
              // The hack here is that we rely on the process exit to clean up
              // the fiber runner thread. What we guarantee here is that
              // no fibers can run after this point as we don't yield here.
              // This makes sure this thread won't accidentally
              // access being destructed core worker.
              fiber_stopped_event_.Notify();
              while (true) {
                std::this_thread::sleep_for(std::chrono::hours(1));
              }
            });
  }

  void EnqueueFiber(const std::string &concurrency_group_name,
                    const ray::FunctionDescriptor &fd,
                    std::function<void()> &&callback) {
    // Note we are doing dangerous trick of passing a reference of rate limiter to the
    // fiber function. This is safe because the *_rate_limiters_ member variables live as
    // long as the FiberState object, and is never modified.
    FiberRateLimiter &rate_limiter = GetRateLimiter(concurrency_group_name, fd);
    auto op_status = channel_.push([&rate_limiter, callback]() {
      rate_limiter.Acquire();
      callback();
      rate_limiter.Release();
    });
    RAY_CHECK(op_status == boost::fibers::channel_op_status::success);
  }

  void Stop() { channel_.close(); }

  void Join() {
    fiber_stopped_event_.Wait();
    fiber_runner_thread_.detach();
  }

 private:
  /// The fiber channel used to send task between the submitter thread
  /// (main direct_actor_trasnport thread) and the fiber_runner_thread_ (defined below)
  FiberChannel channel_;
  /// The fiber semaphore used to limit the number of concurrent fibers
  /// running at once for a concurrency group. Using shared_ptr because a same rate
  /// limiter can be shared between 1 concurrency group and multiple functions.
  absl::flat_hash_map<std::string, std::shared_ptr<FiberRateLimiter>>
      concurrency_group_rate_limiters_;
  /// The fiber semaphore used to limit the number of concurrent fibers
  /// running at once for a function.
  absl::flat_hash_map<std::string, std::shared_ptr<FiberRateLimiter>>
      function_rate_limiters_;
  FiberRateLimiter default_rate_limiter_;
  /// The fiber event used to notify that all worker fibers are stopped running.
  FiberEvent fiber_stopped_event_;
  /// The thread that runs all asyncio fibers. is_asyncio_ must be true.
  std::thread fiber_runner_thread_;

  FiberRateLimiter &GetRateLimiter(const std::string &concurrency_group_name,
                                   const ray::FunctionDescriptor &fd) {
    if (!concurrency_group_name.empty()) {
      auto it = concurrency_group_rate_limiters_.find(concurrency_group_name);
      /// TODO(qwang): Fail the user task.
      RAY_CHECK(it != concurrency_group_rate_limiters_.end())
          << "Failed to look up the executor of the given concurrency group "
          << concurrency_group_name << " . It might be that you didn't define "
          << "the concurrency group " << concurrency_group_name;
      return *it->second;
    }
    /// Code path of that this task wasn't specified in a concurrency group addtionally.
    /// Use the predefined concurrency group.
    auto it = function_rate_limiters_.find(fd->ToString());
    if (it != function_rate_limiters_.end()) {
      return *it->second;
    }
    return default_rate_limiter_;
  }
};

}  // namespace core
}  // namespace ray
