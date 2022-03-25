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

inline std::shared_ptr<boost::asio::deadline_timer> execute_after_us(
    instrumented_io_context &io_context,
    std::function<void()> fn,
    int64_t delay_microseconds) {
  auto timer = std::make_shared<boost::asio::deadline_timer>(io_context);
  timer->expires_from_now(boost::posix_time::microseconds(delay_microseconds));
  timer->async_wait([timer, fn = std::move(fn)](const boost::system::error_code &error) {
    if (error != boost::asio::error::operation_aborted && fn) {
      fn();
    }
  });
  return timer;
}

inline std::shared_ptr<boost::asio::deadline_timer> execute_after(
    instrumented_io_context &io_context, std::function<void()> fn, int64_t milliseconds) {
  return execute_after_us(io_context, fn, milliseconds * 1000);
}
