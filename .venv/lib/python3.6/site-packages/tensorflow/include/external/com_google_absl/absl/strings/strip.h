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
// File: strip.h
// -----------------------------------------------------------------------------
//
// This file contains various functions for stripping substrings from a string.
#ifndef ABSL_STRINGS_STRIP_H_
#define ABSL_STRINGS_STRIP_H_

#include <cstddef>
#include <string>

#include "absl/base/macros.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/string_view.h"

namespace absl {

// ConsumePrefix()
//
// Strips the `expected` prefix from the start of the given string, returning
// `true` if the strip operation succeeded or false otherwise.
//
// Example:
//
//   absl::string_view input("abc");
//   EXPECT_TRUE(absl::ConsumePrefix(&input, "a"));
//   EXPECT_EQ(input, "bc");
inline bool ConsumePrefix(absl::string_view* str, absl::string_view expected) {
  if (!absl::StartsWith(*str, expected)) return false;
  str->remove_prefix(expected.size());
  return true;
}
// ConsumeSuffix()
//
// Strips the `expected` suffix from the end of the given string, returning
// `true` if the strip operation succeeded or false otherwise.
//
// Example:
//
//   absl::string_view input("abcdef");
//   EXPECT_TRUE(absl::ConsumeSuffix(&input, "def"));
//   EXPECT_EQ(input, "abc");
inline bool ConsumeSuffix(absl::string_view* str, absl::string_view expected) {
  if (!absl::EndsWith(*str, expected)) return false;
  str->remove_suffix(expected.size());
  return true;
}

// StripPrefix()
//
// Returns a view into the input string 'str' with the given 'prefix' removed,
// but leaving the original string intact. If the prefix does not match at the
// start of the string, returns the original string instead.
ABSL_MUST_USE_RESULT inline absl::string_view StripPrefix(
    absl::string_view str, absl::string_view prefix) {
  if (absl::StartsWith(str, prefix)) str.remove_prefix(prefix.size());
  return str;
}

// StripSuffix()
//
// Returns a view into the input string 'str' with the given 'suffix' removed,
// but leaving the original string intact. If the suffix does not match at the
// end of the string, returns the original string instead.
ABSL_MUST_USE_RESULT inline absl::string_view StripSuffix(
    absl::string_view str, absl::string_view suffix) {
  if (absl::EndsWith(str, suffix)) str.remove_suffix(suffix.size());
  return str;
}

}  // namespace absl

#endif  // ABSL_STRINGS_STRIP_H_
