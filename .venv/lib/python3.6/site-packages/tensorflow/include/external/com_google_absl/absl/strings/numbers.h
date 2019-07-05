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
// File: numbers.h
// -----------------------------------------------------------------------------
//
// This package contains functions for converting strings to numbers. For
// converting numbers to strings, use `StrCat()` or `StrAppend()` in str_cat.h,
// which automatically detect and convert most number values appropriately.

#ifndef ABSL_STRINGS_NUMBERS_H_
#define ABSL_STRINGS_NUMBERS_H_

#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <limits>
#include <string>
#include <type_traits>

#include "absl/base/macros.h"
#include "absl/base/port.h"
#include "absl/numeric/int128.h"
#include "absl/strings/string_view.h"

namespace absl {

// SimpleAtoi()
//
// Converts the given string into an integer value, returning `true` if
// successful. The string must reflect a base-10 integer (optionally followed or
// preceded by ASCII whitespace) whose value falls within the range of the
// integer type.
template <typename int_type>
ABSL_MUST_USE_RESULT bool SimpleAtoi(absl::string_view s, int_type* out);

// SimpleAtof()
//
// Converts the given string (optionally followed or preceded by ASCII
// whitespace) into a float, which may be rounded on overflow or underflow.
// See http://en.cppreference.com/w/c/string/byte/strtof for details about the
// allowed formats for `str`.
ABSL_MUST_USE_RESULT bool SimpleAtof(absl::string_view str, float* value);

// SimpleAtod()
//
// Converts the given string (optionally followed or preceded by ASCII
// whitespace) into a double, which may be rounded on overflow or underflow.
// See http://en.cppreference.com/w/c/string/byte/strtof for details about the
// allowed formats for `str`.
ABSL_MUST_USE_RESULT bool SimpleAtod(absl::string_view str, double* value);

// SimpleAtob()
//
// Converts the given string into a boolean, returning `true` if successful.
// The following case-insensitive strings are interpreted as boolean `true`:
// "true", "t", "yes", "y", "1". The following case-insensitive strings
// are interpreted as boolean `false`: "false", "f", "no", "n", "0".
ABSL_MUST_USE_RESULT bool SimpleAtob(absl::string_view str, bool* value);

}  // namespace absl

// End of public API.  Implementation details follow.

namespace absl {
namespace numbers_internal {

// safe_strto?() functions for implementing SimpleAtoi()
bool safe_strto32_base(absl::string_view text, int32_t* value, int base);
bool safe_strto64_base(absl::string_view text, int64_t* value, int base);
bool safe_strtou32_base(absl::string_view text, uint32_t* value, int base);
bool safe_strtou64_base(absl::string_view text, uint64_t* value, int base);

static const int kFastToBufferSize = 32;
static const int kSixDigitsToBufferSize = 16;

// Helper function for fast formatting of floating-point values.
// The result is the same as printf's "%g", a.k.a. "%.6g"; that is, six
// significant digits are returned, trailing zeros are removed, and numbers
// outside the range 0.0001-999999 are output using scientific notation
// (1.23456e+06). This routine is heavily optimized.
// Required buffer size is `kSixDigitsToBufferSize`.
size_t SixDigitsToBuffer(double d, char* buffer);

// These functions are intended for speed. All functions take an output buffer
// as an argument and return a pointer to the last byte they wrote, which is the
// terminating '\0'. At most `kFastToBufferSize` bytes are written.
char* FastIntToBuffer(int32_t, char*);
char* FastIntToBuffer(uint32_t, char*);
char* FastIntToBuffer(int64_t, char*);
char* FastIntToBuffer(uint64_t, char*);

// For enums and integer types that are not an exact match for the types above,
// use templates to call the appropriate one of the four overloads above.
template <typename int_type>
char* FastIntToBuffer(int_type i, char* buffer) {
  static_assert(sizeof(i) <= 64 / 8,
                "FastIntToBuffer works only with 64-bit-or-less integers.");
  // TODO(jorg): This signed-ness check is used because it works correctly
  // with enums, and it also serves to check that int_type is not a pointer.
  // If one day something like std::is_signed<enum E> works, switch to it.
  if (static_cast<int_type>(1) - 2 < 0) {  // Signed
    if (sizeof(i) > 32 / 8) {           // 33-bit to 64-bit
      return FastIntToBuffer(static_cast<int64_t>(i), buffer);
    } else {  // 32-bit or less
      return FastIntToBuffer(static_cast<int32_t>(i), buffer);
    }
  } else {                     // Unsigned
    if (sizeof(i) > 32 / 8) {  // 33-bit to 64-bit
      return FastIntToBuffer(static_cast<uint64_t>(i), buffer);
    } else {  // 32-bit or less
      return FastIntToBuffer(static_cast<uint32_t>(i), buffer);
    }
  }
}

// Implementation of SimpleAtoi, generalized to support arbitrary base (used
// with base different from 10 elsewhere in Abseil implementation).
template <typename int_type>
ABSL_MUST_USE_RESULT bool safe_strtoi_base(absl::string_view s, int_type* out,
                                           int base) {
  static_assert(sizeof(*out) == 4 || sizeof(*out) == 8,
                "SimpleAtoi works only with 32-bit or 64-bit integers.");
  static_assert(!std::is_floating_point<int_type>::value,
                "Use SimpleAtof or SimpleAtod instead.");
  bool parsed;
  // TODO(jorg): This signed-ness check is used because it works correctly
  // with enums, and it also serves to check that int_type is not a pointer.
  // If one day something like std::is_signed<enum E> works, switch to it.
  if (static_cast<int_type>(1) - 2 < 0) {  // Signed
    if (sizeof(*out) == 64 / 8) {       // 64-bit
      int64_t val;
      parsed = numbers_internal::safe_strto64_base(s, &val, base);
      *out = static_cast<int_type>(val);
    } else {  // 32-bit
      int32_t val;
      parsed = numbers_internal::safe_strto32_base(s, &val, base);
      *out = static_cast<int_type>(val);
    }
  } else {                         // Unsigned
    if (sizeof(*out) == 64 / 8) {  // 64-bit
      uint64_t val;
      parsed = numbers_internal::safe_strtou64_base(s, &val, base);
      *out = static_cast<int_type>(val);
    } else {  // 32-bit
      uint32_t val;
      parsed = numbers_internal::safe_strtou32_base(s, &val, base);
      *out = static_cast<int_type>(val);
    }
  }
  return parsed;
}

}  // namespace numbers_internal

// SimpleAtoi()
//
// Converts a string to an integer, using `safe_strto?()` functions for actual
// parsing, returning `true` if successful. The `safe_strto?()` functions apply
// strict checking; the string must be a base-10 integer, optionally followed or
// preceded by ASCII whitespace, with a value in the range of the corresponding
// integer type.
template <typename int_type>
ABSL_MUST_USE_RESULT bool SimpleAtoi(absl::string_view s, int_type* out) {
  return numbers_internal::safe_strtoi_base(s, out, 10);
}

}  // namespace absl

#endif  // ABSL_STRINGS_NUMBERS_H_
