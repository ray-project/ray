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
#include <cstdint>
#include <map>
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

namespace internal {

/**
 * Records the stack allocations handed out to the Boost fibers of one
 * FiberState, so that code running on a fiber can recover the exact allocation
 * backing it.
 *
 * Boost.Fiber does not expose the `stack_context` of the running fiber, so the
 * bounds are captured when the stack allocator hands one out and recovered by
 * looking up an address known to lie on the current stack. The recorded ranges
 * are distinct allocations of the same size and therefore disjoint, so at most
 * one of them can contain any given address: a lookup either returns the
 * caller's own stack or reports that it found nothing.
 */
class FiberStackRegistry {
 public:
  /**
   * Records the allocation backing a fiber that is about to be created.
   *
   * \param stack_context Allocation returned by the underlying stack allocator.
   */
  void Register(const boost::context::stack_context &stack_context) {
    CheckOwnerThread();
    RAY_CHECK(stack_ranges_.emplace(StackBase(stack_context), stack_context.size).second)
        << "A fiber stack allocation was registered twice.";
  }

  /**
   * Forgets a recorded allocation. Callers must invoke this before releasing
   * the memory, otherwise the allocator could hand the same address out again
   * while the stale range is still recorded.
   *
   * \param stack_context Allocation that is about to be released.
   */
  void Unregister(const boost::context::stack_context &stack_context) {
    CheckOwnerThread();
    RAY_CHECK_EQ(stack_ranges_.erase(StackBase(stack_context)), 1U)
        << "A fiber stack allocation was released without being registered.";
  }

  /**
   * Looks up the recorded allocation that contains \p address.
   *
   * \param address Address to locate, normally that of a local variable in the
   *   calling frame.
   * \param[out] stack_start_addr Lowest address of the containing allocation.
   *   Left untouched when no allocation contains \p address.
   * \param[out] stack_size Size of the containing allocation in bytes. Left
   *   untouched when no allocation contains \p address.
   * \return True when a recorded allocation contains \p address.
   */
  bool FindContaining(uintptr_t address,
                      void **stack_start_addr,
                      size_t *stack_size) const {
    std::map<uintptr_t, size_t>::const_iterator it = stack_ranges_.upper_bound(address);
    if (it == stack_ranges_.begin()) {
      return false;
    }
    --it;
    const uintptr_t base = it->first;
    const size_t size = it->second;
    if (address >= base + size) {
      return false;
    }
    *stack_start_addr = reinterpret_cast<void *>(base);
    *stack_size = size;
    return true;
  }

 private:
  /**
   * Returns the lowest address of an allocation. Boost reports `sp` as the high
   * end because fiber stacks grow downwards.
   */
  static uintptr_t StackBase(const boost::context::stack_context &stack_context) {
    return reinterpret_cast<uintptr_t>(stack_context.sp) - stack_context.size;
  }

  /**
   * Fails if the registry is ever mutated from more than one thread.
   *
   * Registration, unregistration, and lookup all happen on a single FiberState
   * runner thread, so `stack_ranges_` needs no synchronization. That holds
   * because Boost.Fiber disposes a terminated detached fiber from the scheduler
   * that ran it, which is a behavior Boost does not document as a contract.
   * This check turns a future violation into a crash rather than a silent data
   * race on `stack_ranges_`.
   */
  void CheckOwnerThread() {
    if (owner_thread_ == std::thread::id{}) {
      owner_thread_ = std::this_thread::get_id();
      return;
    }
    RAY_CHECK(std::this_thread::get_id() == owner_thread_)
        << "FiberStackRegistry was accessed from a thread other than the fiber "
           "runner thread that first used it.";
  }

  /** Live allocations, keyed by their lowest address. */
  std::map<uintptr_t, size_t> stack_ranges_;
  /** Runner thread that owns this registry, captured on first use. */
  std::thread::id owner_thread_;
};

/**
 * Stack allocator that records each allocation in a FiberStackRegistry before
 * handing it to Boost.
 *
 * The lowercase `allocate` and `deallocate` names satisfy Boost's stack
 * allocator concept. Boost copies the allocator into every fiber context it
 * creates, so the registry is held through a `shared_ptr` rather than stored
 * inline: all copies must record into the same registry, and a copy may outlive
 * the FiberState that created it.
 */
class TrackingFiberStackAllocator {
 public:
  TrackingFiberStackAllocator(size_t size, std::shared_ptr<FiberStackRegistry> registry)
      : allocator_(size), registry_(std::move(registry)) {}

  /** Allocates a fiber stack and records its bounds. */
  boost::context::stack_context allocate() {
    boost::context::stack_context stack_context = allocator_.allocate();
    try {
      registry_->Register(stack_context);
    } catch (...) {
      allocator_.deallocate(stack_context);
      throw;
    }
    return stack_context;
  }

  /** Forgets the recorded bounds, then releases the stack. */
  void deallocate(boost::context::stack_context &stack_context) noexcept {
    registry_->Unregister(stack_context);
    allocator_.deallocate(stack_context);
  }

 private:
  boost::fibers::fixedsize_stack allocator_;
  std::shared_ptr<FiberStackRegistry> registry_;
};

/**
 * Registry belonging to the FiberState whose runner thread this is, or nullptr
 * on every other thread. Set once when a runner thread starts; the thread
 * always finishes inside FiberState::Join(), before the registry is destroyed.
 *
 * The `inline` definition is required for correctness, not brevity: it gives
 * every translation unit the same variable. A copy per translation unit (which
 * an anonymous namespace would produce) would leave the lookup compiled into
 * `_raylet` reading a different pointer than the one FiberState writes, so it
 * would always see nullptr.
 */
inline thread_local FiberStackRegistry *active_fiber_stack_registry = nullptr;

}  // namespace internal

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
      : stack_registry_(std::make_shared<internal::FiberStackRegistry>()),
        allocator_(kStackSize, stack_registry_),
        rate_limiter_(max_concurrency) {
    fiber_runner_thread_ = std::thread([this]() {
      /* Publish the registry so fibers running on this thread can find their
       * own stack bounds. Not restored on exit: the pointer lives in this
       * thread's storage and dies with the thread. */
      internal::active_fiber_stack_registry = stack_registry_.get();
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

  /**
   * Returns the allocation backing the Boost fiber running on this thread.
   *
   * The current stack is identified by the address of a local variable, so the
   * result does not depend on how deep the caller sits on the fiber stack. The
   * reported range is the whole allocation, which is a superset of the region
   * Boost leaves usable: Boost reserves the top of the allocation for its own
   * control record. The lowest address is exact, which is what matters for
   * stack-overflow detection, since fiber stacks grow downwards.
   *
   * \param[out] stack_start_addr Lowest address of the fiber stack. Left
   *   untouched when this thread is not running a tracked fiber.
   * \param[out] stack_size Size of the fiber stack in bytes. Left untouched
   *   when this thread is not running a tracked fiber.
   * \return True when called on a FiberState runner thread while it executes a
   *   fiber, false on any other stack.
   */
  static bool GetCurrentFiberStackBounds(void **stack_start_addr, size_t *stack_size) {
    char stack_anchor;
    internal::FiberStackRegistry *registry = internal::active_fiber_stack_registry;
    return registry != nullptr &&
           registry->FindContaining(
               reinterpret_cast<uintptr_t>(&stack_anchor), stack_start_addr, stack_size);
  }

 private:
  static constexpr size_t kStackSize = 1024 * 256;

  /** Shared with every allocator copy Boost makes; see the allocator's docs. */
  std::shared_ptr<internal::FiberStackRegistry> stack_registry_;
  /**
   * The fiber stack allocator. It records each allocation so that a running
   * fiber can recover its own stack bounds.
   */
  internal::TrackingFiberStackAllocator allocator_;
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
