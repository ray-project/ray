// Copyright 2018 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// -----------------------------------------------------------------------------
// bad_variant_access.h
// -----------------------------------------------------------------------------
//
// This header file defines the `absl::bad_variant_access` type.

#ifndef ABSL_TYPES_BAD_VARIANT_ACCESS_H_
#define ABSL_TYPES_BAD_VARIANT_ACCESS_H_

#include <stdexcept>

#include "absl/base/config.h"

#ifdef ABSL_HAVE_STD_VARIANT

#include <variant>

namespace absl {
using std::bad_variant_access;
}  // namespace absl

#else  // ABSL_HAVE_STD_VARIANT

namespace absl {

// -----------------------------------------------------------------------------
// bad_variant_access
// -----------------------------------------------------------------------------
//
// An `absl::bad_variant_access` type is an exception type that is thrown in
// the following cases:
//
//   * Calling `absl::get(absl::variant) with an index or type that does not
//     match the currently selected alternative type
//   * Calling `absl::visit on an `absl::variant` that is in the
//     `variant::valueless_by_exception` state.
//
// Example:
//
//   absl::variant<int, std::string> v;
//   v = 1;
//   try {
//     absl::get<std::string>(v);
//   } catch(const absl::bad_variant_access& e) {
//     std::cout << "Bad variant access: " << e.what() << '\n';
//   }
class bad_variant_access : public std::exception {
 public:
  bad_variant_access() noexcept = default;
  ~bad_variant_access() override;
  const char* what() const noexcept override;
};

namespace variant_internal {

[[noreturn]] void ThrowBadVariantAccess();
[[noreturn]] void Rethrow();

}  // namespace variant_internal
}  // namespace absl

#endif  // ABSL_HAVE_STD_VARIANT

#endif  // ABSL_TYPES_BAD_VARIANT_ACCESS_H_
