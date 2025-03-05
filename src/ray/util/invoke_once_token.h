// Copyright 2025 The Ray Authors.
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

#include <atomic>

#include "ray/util/logging.h"

namespace ray {

// A util class (a token) which guards again multiple invocations.
// It's thread-safe.
//
// Example usage:
// void SomeFunc() {
//   static InvokeOnceToken token;
//   token.CheckInvokeOnce();
// }
class InvokeOnceToken {
 public:
  void CheckInvokeOnce() {
    bool expected = false;
    RAY_CHECK(invoked_.compare_exchange_strong(expected, /*desired=*/true))
        << "Invoke once token has been visited before.";
  }

 private:
  std::atomic<bool> invoked_{false};
};

static_assert(std::is_trivially_destructible<InvokeOnceToken>::value,
              "InvokeOnceToken must be trivially destructible");

}  // namespace ray
