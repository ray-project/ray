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

// This function is used to retry a function until it returns true or the
// retry_num is exhausted. The function will be called immediately and then
// after delay_duration for each retry.
//
// \param e The executor to run the retry loop on. It has to be an io_executor
// from boost::asio
// \param predicate The function to retry. It should accept no arguments and
// return bool.
// \param retry_num The number of times to retry. If it's nullopt, it'll retry
// forever until predicate returns true.
// \param duration The duration to wait between retries. It needs to be
// std::chrono::Duration.
// \param token The completion token to use. It can be either a callback or
// other completion tokens accepted by boost::asio.
//
// \return The completion token return type. For example, if the completion
// token is a callback, it'll return void. If the completion token is
// boost::asio::use_future_t it'll return future<bool>.
template <typename Fn,
          typename AsioIOExecutor,
          typename Duration,
          typename CompletionToken = boost::asio::use_future_t<> >
auto async_retry_until(AsioIOExecutor &&e,
                       Fn &&predicate,
                       std::optional<int64_t> retry_num,
                       Duration delay_duration,
                       CompletionToken &&token = CompletionToken()) {
  static_assert(std::is_assignable_v<std::function<bool()>, Fn>,
                "predicate must should accept no arguments and return bool");
  auto delay_timer = std::make_unique<boost::asio::deadline_timer>(e);
  auto delay = boost::posix_time::microseconds(
      std::chrono::duration_cast<std::chrono::microseconds>(delay_duration).count());
  return boost::asio::async_compose<CompletionToken, void(bool)>(
      [retry_num = retry_num,
       delay_timer = std::move(delay_timer),
       delay = delay,
       coro = boost::asio::coroutine(),
       predicate = std::forward<Fn>(predicate)](
          auto &self, const boost::system::error_code &error = {}) mutable {
        reenter(coro) {
          while (true) {
            if (error) {
              self.complete(false);
              return;
            }
            if (predicate()) {
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
