//
// Copyright 2017 The Abseil Authors.
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
// File: casts.h
// -----------------------------------------------------------------------------
//
// This header file defines casting templates to fit use cases not covered by
// the standard casts provided in the C++ standard. As with all cast operations,
// use these with caution and only if alternatives do not exist.

#ifndef ABSL_BASE_CASTS_H_
#define ABSL_BASE_CASTS_H_

#include <cstring>
#include <memory>
#include <type_traits>

#include "absl/base/internal/identity.h"
#include "absl/base/macros.h"

namespace absl {

namespace internal_casts {

// NOTE: Not a fully compliant implementation of `std::is_trivially_copyable`.
// TODO(calabrese) Branch on implementations that directly provide
// `std::is_trivially_copyable`, create a more rigorous workaround, and publicly
// expose in meta/type_traits.
template <class T>
struct is_trivially_copyable
    : std::integral_constant<
          bool, std::is_destructible<T>::value&& __has_trivial_destructor(T) &&
                    __has_trivial_copy(T) && __has_trivial_assign(T)> {};

template <class Dest, class Source>
struct is_bitcastable
    : std::integral_constant<bool,
                             sizeof(Dest) == sizeof(Source) &&
                                 is_trivially_copyable<Source>::value &&
                                 is_trivially_copyable<Dest>::value &&
                                 std::is_default_constructible<Dest>::value> {};

}  // namespace internal_casts

// implicit_cast()
//
// Performs an implicit conversion between types following the language
// rules for implicit conversion; if an implicit conversion is otherwise
// allowed by the language in the given context, this function performs such an
// implicit conversion.
//
// Example:
//
//   // If the context allows implicit conversion:
//   From from;
//   To to = from;
//
//   // Such code can be replaced by:
//   implicit_cast<To>(from);
//
// An `implicit_cast()` may also be used to annotate numeric type conversions
// that, although safe, may produce compiler warnings (such as `long` to `int`).
// Additionally, an `implicit_cast()` is also useful within return statements to
// indicate a specific implicit conversion is being undertaken.
//
// Example:
//
//   return implicit_cast<double>(size_in_bytes) / capacity_;
//
// Annotating code with `implicit_cast()` allows you to explicitly select
// particular overloads and template instantiations, while providing a safer
// cast than `reinterpret_cast()` or `static_cast()`.
//
// Additionally, an `implicit_cast()` can be used to allow upcasting within a
// type hierarchy where incorrect use of `static_cast()` could accidentally
// allow downcasting.
//
// Finally, an `implicit_cast()` can be used to perform implicit conversions
// from unrelated types that otherwise couldn't be implicitly cast directly;
// C++ will normally only implicitly cast "one step" in such conversions.
//
// That is, if C is a type which can be implicitly converted to B, with B being
// a type that can be implicitly converted to A, an `implicit_cast()` can be
// used to convert C to B (which the compiler can then implicitly convert to A
// using language rules).
//
// Example:
//
//   // Assume an object C is convertible to B, which is implicitly convertible
//   // to A
//   A a = implicit_cast<B>(C);
//
// Such implicit cast chaining may be useful within template logic.
template <typename To>
constexpr To implicit_cast(typename absl::internal::identity_t<To> to) {
  return to;
}

// bit_cast()
//
// Performs a bitwise cast on a type without changing the underlying bit
// representation of that type's value. The two types must be of the same size
// and both types must be trivially copyable. As with most casts, use with
// caution. A `bit_cast()` might be needed when you need to temporarily treat a
// type as some other type, such as in the following cases:
//
//    * Serialization (casting temporarily to `char *` for those purposes is
//      always allowed by the C++ standard)
//    * Managing the individual bits of a type within mathematical operations
//      that are not normally accessible through that type
//    * Casting non-pointer types to pointer types (casting the other way is
//      allowed by `reinterpret_cast()` but round-trips cannot occur the other
//      way).
//
// Example:
//
//   float f = 3.14159265358979;
//   int i = bit_cast<int32_t>(f);
//   // i = 0x40490fdb
//
// Casting non-pointer types to pointer types and then dereferencing them
// traditionally produces undefined behavior.
//
// Example:
//
//   // WRONG
//   float f = 3.14159265358979;            // WRONG
//   int i = * reinterpret_cast<int*>(&f);  // WRONG
//
// The address-casting method produces undefined behavior according to the ISO
// C++ specification section [basic.lval]. Roughly, this section says: if an
// object in memory has one type, and a program accesses it with a different
// type, the result is undefined behavior for most values of "different type".
//
// Such casting results in type punning: holding an object in memory of one type
// and reading its bits back using a different type. A `bit_cast()` avoids this
// issue by implementing its casts using `memcpy()`, which avoids introducing
// this undefined behavior.
//
// NOTE: The requirements here are more strict than the bit_cast of standard
// proposal p0476 due to the need for workarounds and lack of intrinsics.
// Specifically, this implementation also requires `Dest` to be
// default-constructible.
template <
    typename Dest, typename Source,
    typename std::enable_if<internal_casts::is_bitcastable<Dest, Source>::value,
                            int>::type = 0>
inline Dest bit_cast(const Source& source) {
  Dest dest;
  memcpy(static_cast<void*>(std::addressof(dest)),
         static_cast<const void*>(std::addressof(source)), sizeof(dest));
  return dest;
}

// NOTE: This overload is only picked if the requirements of bit_cast are not
// met. It is therefore UB, but is provided temporarily as previous versions of
// this function template were unchecked. Do not use this in new code.
template <
    typename Dest, typename Source,
    typename std::enable_if<
        !internal_casts::is_bitcastable<Dest, Source>::value, int>::type = 0>
ABSL_DEPRECATED(
    "absl::bit_cast type requirements were violated. Update the types being "
    "used such that they are the same size and are both TriviallyCopyable.")
inline Dest bit_cast(const Source& source) {
  static_assert(sizeof(Dest) == sizeof(Source),
                "Source and destination types should have equal sizes.");

  Dest dest;
  memcpy(&dest, &source, sizeof(dest));
  return dest;
}

}  // namespace absl

#endif  // ABSL_BASE_CASTS_H_
