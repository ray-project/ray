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
#include <boost/asio/coroutine.hpp>
#include <boost/asio/yield.hpp>
#include <chrono>

template <typename Duration>
std::shared_ptr<boost::asio::deadline_timer> execute_after(
    boost::asio::io_context &io_context,
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

template <typename Fn,
          typename Ex,
          typename Duration,
          typename CompletionToken = boost::asio::use_future_t<> >
auto async_retry_until(Ex &&e,
                       Fn &&fn,
                       std::optional<int64_t> retry_num,
                       Duration delay_duration,
                       CompletionToken &&token = CompletionToken()) {
  auto delay_timer = std::make_unique<boost::asio::deadline_timer>(e);
  auto delay = boost::posix_time::microseconds(
      std::chrono::duration_cast<std::chrono::microseconds>(delay_duration).count());
  return boost::asio::async_compose<CompletionToken, void(bool)>(
      [retry_num = retry_num,
       delay_timer = std::move(delay_timer),
       delay = delay,
       coro = boost::asio::coroutine(),
       fn = std::forward<Fn>(fn)](auto &self,
                                  const boost::system::error_code &error = {}) mutable {
        reenter(coro) {
          while (true) {
            if (fn()) {
              self.complete(true);
              return;
            } else {
              if (retry_num) {
                --*retry_num;
                if (*retry_num < 0) {
                  self.complete(false);
                  return;
                }
              }
              delay_timer->expires_from_now(delay);
              yield delay_timer->async_wait(std::move(self));
            }
          }
        }
      },
      token,
      e);
}
