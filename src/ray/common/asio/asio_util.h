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

#include <boost/asio.hpp>
#include <chrono>
#include <thread>

#include "ray/common/asio/instrumented_io_context.h"
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
      : io_service_(), work_(io_service_) {
    io_thread_ = std::thread([this, thread_name] {
      SetThreadName(thread_name);
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
};
