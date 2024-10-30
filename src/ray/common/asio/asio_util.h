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

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/util/array.h"
#include "ray/util/util.h"

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
   */
  explicit InstrumentedIOContextWithThread(const std::string &thread_name)
      : io_service_(), work_(io_service_), thread_name_(thread_name) {
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
  instrumented_io_context io_service_;
  boost::asio::io_service::work work_;  // to keep io_service_ running
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
///     // List of all IO Context names. We will create 1 thread + 1
///     // instrumented_io_context for each. Must be unique and should not contain empty
///     // names.
///     constexpr static std::array<std::string_view, N> kAllDedicatedIOContextNames;
///
///     // For a given T, returns an index to kAllDedicatedIOContextNames, or -1 for the
///     // default io context.
///     constexpr static std::string_view GetDedicatedIOContextIndex<T>();
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
    for (size_t i = 0; i < Policy::kAllDedicatedIOContextNames.size(); i++) {
      const auto &name = Policy::kAllDedicatedIOContextNames[i];
      dedicated_io_contexts_[i] =
          std::make_unique<InstrumentedIOContextWithThread>(std::string(name));
    }
  }

  // Gets IOContext registered for type T. If the type is not registered in
  // Policy::kAllDedicatedIOContextNames, it's a compile error.
  template <typename T>
  instrumented_io_context &GetIOContext() const {
    constexpr int index = Policy::template GetDedicatedIOContextIndex<T>();
    static_assert(
        index >= -1 && index < Policy::kAllDedicatedIOContextNames.size(),
        "index out of bound, invalid GetDedicatedIOContextIndex implementation! Index "
        "can only be -1 or within range of kAllDedicatedIOContextNames");

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
  // Validating the Policy is valid.
  static constexpr bool CheckNoEmpty() {
    for (const auto &name : Policy::kAllDedicatedIOContextNames) {
      if (name.empty()) {
        return false;
      }
    }
    return true;
  }
  static_assert(CheckNoEmpty(),
                "kAllDedicatedIOContextNames must not contain empty strings.");
  static_assert(ray::ArrayIsUnique(Policy::kAllDedicatedIOContextNames),
                "kAllDedicatedIOContextNames must not contain duplicate elements.");

  // Using unique_ptr because the class has no default constructor, so it's not easy
  // to initialize objects directly in the array.
  std::array<std::unique_ptr<InstrumentedIOContextWithThread>,
             Policy::kAllDedicatedIOContextNames.size()>
      dedicated_io_contexts_;
  instrumented_io_context &default_io_context_;
};
