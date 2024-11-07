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

// SCOPE_EXIT is used to execute a series of registered functions when it goes
// out of scope.
// For details, refer to Andrei Alexandrescu's CppCon 2015 talk "Declarative
// Control Flow"
//
// Examples:
//   void Function() {
//     FILE* fp = fopen("my_file.txt", "r");
//     SCOPE_EXIT { fclose(fp); };
//     // Do something.
//   }  // fp will be closed at exit.
//

#pragma once

#include <functional>
#include <utility>

#include "absl/base/attributes.h"
#include "src/ray/util/meta.h"

namespace ray {

class ABSL_MUST_USE_RESULT ScopeGuard {
 private:
  using Func = std::function<void(void)>;

 public:
  ScopeGuard() : func_([]() {}) {}
  explicit ScopeGuard(Func &&func) : func_(std::forward<Func>(func)) {}
  ~ScopeGuard() noexcept { func_(); }

  // Register a new function to be invoked at destruction.
  // Execution will be performed at the reversed order they're registered.
  ScopeGuard &operator+=(Func &&another_func) {
    Func cur_func = std::move(func_);
    func_ = [cur_func = std::move(cur_func), another_func = std::move(another_func)]() {
      // Executed in the reverse order functions are registered.
      another_func();
      cur_func();
    };
    return *this;
  }

 private:
  Func func_;
};

namespace internal {

using ScopeGuardFunc = std::function<void(void)>;

// Constructs a scope guard that calls 'fn' when it exits.
enum class ScopeGuardOnExit {};
inline auto operator+(ScopeGuardOnExit /*unused*/, ScopeGuardFunc fn) {
  return ScopeGuard{std::move(fn)};
}

}  // namespace internal

}  // namespace ray

#define SCOPE_EXIT                                 \
  auto RAY_UNIQUE_VARIABLE(SCOPE_EXIT_TEMP_EXIT) = \
      ray::internal::ScopeGuardOnExit{} + [&]()
