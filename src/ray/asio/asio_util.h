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

#include <array>
#include <boost/asio.hpp>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "ray/asio/instrumented_io_context.h"
#include "ray/util/thread_utils.h"

template <typename Duration>
std::shared_ptr<boost::asio::deadline_timer> execute_after(
    instrumented_io_context &io_context,
    std::function<void()> fn,
    Duration delay_duration) {
  auto timer = std::make_shared<boost::asio::deadline_timer>(io_context);
  auto delay = boost::posix_time::microseconds(
      std::chrono::duration_cast<std::chrono::microseconds>(delay_duration).count());
  timer->expires_from_now(delay);

  timer->async_wait([timer, fn = std::move(fn)](const boost::system::error_code &error) {
    if (error != boost::asio::error::operation_aborted && fn) {
      fn();
    }
  });

  return timer;
}

/**
 * A class that manages an instrumented_io_context and a std::thread.
 * The constructor takes a thread name and starts the thread.
 * The destructor stops the io_service and joins the thread.
 */
class InstrumentedIOContextWithThread {
 public:
  /**
   * Constructor.
   * @param thread_name The name of the thread.
   * @param enable_lag_probe If true, enables the lag probe. It posts tasks to the
   * io_context so that a run() will never return.
   */
  explicit InstrumentedIOContextWithThread(const std::string &thread_name,
                                           bool enable_lag_probe = false)
      : io_service_(enable_lag_probe, /*running_on_single_thread=*/true, thread_name),
        work_(io_service_.get_executor()),
        thread_name_(thread_name) {
    io_thread_ = std::thread([this] {
      SetThreadName(this->thread_name_);
      io_service_.run();
    });
  }

  ~InstrumentedIOContextWithThread() { Stop(); }

  // Non-movable and non-copyable.
  InstrumentedIOContextWithThread(const InstrumentedIOContextWithThread &) = delete;
  InstrumentedIOContextWithThread &operator=(const InstrumentedIOContextWithThread &) =
      delete;
  InstrumentedIOContextWithThread(InstrumentedIOContextWithThread &&) = delete;
  InstrumentedIOContextWithThread &operator=(InstrumentedIOContextWithThread &&) = delete;

  instrumented_io_context &GetIoService() { return io_service_; }
  const std::string &GetName() const { return thread_name_; }

  // Idempotent. Once it's stopped you can't restart it.
  void Stop() {
    io_service_.stop();
    if (io_thread_.joinable()) {
      io_thread_.join();
    }
  }

 private:
  instrumented_io_context io_service_{/*enable_metrics=*/false,
                                      /*running_on_single_thread=*/true};
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type>
      work_;  // to keep io_service_ running
  std::thread io_thread_;
  std::string thread_name_;
};

/// `IOContextProvider` uses a specified `Policy` to determine whether a type `T`
/// requires a dedicated `io_context` or should use a shared default `io_context`.
/// It provides a method to retrieve the appropriate `io_context` for instances of
/// different classes.
///
/// @param Policy The policy class that defines which types require dedicated
/// `io_context` instances.
///
/// ## The Policy
/// SYNOPSIS:
/// ```
/// struct YourPolicy {
///     // Metadata for all dedicated IO contexts. We will create 1 thread + 1
///     // instrumented_io_context for each. Each element must expose at least a
///     // `name` (unique, non-empty std::string_view) and an `enable_lag_probe`
///     // (bool) member. Policies may add extra members for their own use.
///     constexpr static std::array<SomeMetadataStruct, N> kAllDedicatedIOContexts;
///
///     // For a given T, returns an index into kAllDedicatedIOContexts, or -1 for the
///     // default io context.
///     constexpr static int GetDedicatedIOContextIndex<T>();
/// }
/// ```
///
/// For an example, see `GcsServerIOContextPolicy`.
///
/// ## Notes
///
/// - `default_io_context` must outlive the `IOContextProvider` instance.
/// - Eagerly creates dedicated `io_context` instances in ctor.
/// - Thread safe.
/// - There is no way to remove a dedicated `io_context` once created until destruction.
template <typename Policy>
class IOContextProvider {
 public:
  explicit IOContextProvider(instrumented_io_context &default_io_context)
      : default_io_context_(default_io_context) {
    for (size_t i = 0; i < Policy::kAllDedicatedIOContexts.size(); i++) {
      const auto &metadata = Policy::kAllDedicatedIOContexts[i];
      dedicated_io_contexts_[i] = std::make_unique<InstrumentedIOContextWithThread>(
          std::string(metadata.name), metadata.enable_lag_probe);
    }
  }

  // Gets IOContext registered for type T. If the type is not registered in
  // Policy::kAllDedicatedIOContexts, it's a compile error.
  template <typename T>
  instrumented_io_context &GetIOContext() const {
    constexpr int index = Policy::template GetDedicatedIOContextIndex<T>();
    static_assert(
        index >= -1 && index < static_cast<int>(Policy::kAllDedicatedIOContexts.size()),
        "index out of bound, invalid GetDedicatedIOContextIndex implementation! Index "
        "can only be -1 or within range of kAllDedicatedIOContexts");

    if constexpr (index == -1) {
      return default_io_context_;
    } else {
      return dedicated_io_contexts_[index]->GetIoService();
    }
  }

  instrumented_io_context &GetDefaultIOContext() const { return default_io_context_; }
  // Used for inspections, e.g. print stats.
  const auto &GetAllDedicatedIOContexts() const { return dedicated_io_contexts_; }

  void StopAllDedicatedIOContexts() {
    for (auto &io_ctx : dedicated_io_contexts_) {
      io_ctx->Stop();
    }
  }

 private:
  static constexpr bool CheckNoEmptyNames() {
    for (const auto &metadata : Policy::kAllDedicatedIOContexts) {
      if (metadata.name.empty()) {
        return false;
      }
    }
    return true;
  }

  // Warning: O(n^2) complexity. Only used in a constexpr context.
  static constexpr bool NamesAreUnique() {
    const auto &contexts = Policy::kAllDedicatedIOContexts;
    for (size_t i = 0; i < contexts.size(); ++i) {
      for (size_t j = i + 1; j < contexts.size(); ++j) {
        if (contexts[i].name == contexts[j].name) {
          return false;
        }
      }
    }
    return true;
  }
  static_assert(CheckNoEmptyNames(),
                "kAllDedicatedIOContexts names must not contain empty strings.");
  static_assert(NamesAreUnique(),
                "kAllDedicatedIOContexts names must not contain duplicate elements.");

  // Using unique_ptr because the class has no default constructor, so it's not easy
  // to initialize objects directly in the array.
  std::array<std::unique_ptr<InstrumentedIOContextWithThread>,
             Policy::kAllDedicatedIOContexts.size()>
      dedicated_io_contexts_;
  instrumented_io_context &default_io_context_;
};
