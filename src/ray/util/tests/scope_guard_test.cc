// Copyright 2024 The Ray Authors.
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

#include "src/ray/util/scope_guard.h"

#include <gtest/gtest.h>

#include <future>

namespace ray {

namespace {

TEST(ScopeGuardTest, BasicTest) {
  std::promise<void> promise{};
  {
    ScopeGuard wg{[&]() { promise.set_value(); }};
  }
  promise.get_future().get();
}

TEST(ScopeGuardTest, MacroTest) {
  std::promise<void> promise{};
  {
    SCOPE_EXIT { promise.set_value(); };
  }
  promise.get_future().get();
}

}  // namespace

}  // namespace ray
