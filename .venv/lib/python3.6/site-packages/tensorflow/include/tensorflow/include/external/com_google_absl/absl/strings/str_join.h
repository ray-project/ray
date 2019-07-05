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
// File: str_join.h
// -----------------------------------------------------------------------------
//
// This header file contains functions for joining a range of elements and
// returning the result as a string. StrJoin operations are specified by passing
// a range, a separator string to use between the elements joined, and an
// optional Formatter responsible for converting each argument in the range to a
// string. If omitted, a default `AlphaNumFormatter()` is called on the elements
// to be joined, using the same formatting that `absl::StrCat()` uses. This
// package defines a number of default formatters, and you can define your own
// implementations.
//
// Ranges are specified by passing a container with `std::begin()` and
// `std::end()` iterators, container-specific `begin()` and `end()` iterators, a
// brace-initialized `std::initializer_list`, or a `std::tuple` of heterogeneous
// objects. The separator string is specified as an `absl::string_view`.
//
// Because the default formatter uses the `absl::AlphaNum` class,
// `absl::StrJoin()`, like `absl::StrCat()`, will work out-of-the-box on
// collections of strings, ints, floats, doubles, etc.
//
// Example:
//
//   std::vector<string> v = {"foo", "bar", "baz"};
//   string s = absl::StrJoin(v, "-");
//   EXPECT_EQ("foo-bar-baz", s);
//
// See comments on the `absl::StrJoin()` function for more examples.

#ifndef ABSL_STRINGS_STR_JOIN_H_
#define ABSL_STRINGS_STR_JOIN_H_

#include <cstdio>
#include <cstring>
#include <initializer_list>
#include <iterator>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/macros.h"
#include "absl/strings/internal/str_join_internal.h"
#include "absl/strings/string_view.h"

namespace absl {

// -----------------------------------------------------------------------------
// Concept: Formatter
// -----------------------------------------------------------------------------
//
// A Formatter is a function object that is responsible for formatting its
// argument as a string and appending it to a given output string. Formatters
// may be implemented as function objects, lambdas, or normal functions. You may
// provide your own Formatter to enable `absl::StrJoin()` to work with arbitrary
// types.
//
// The following is an example of a custom Formatter that simply uses
// `std::to_string()` to format an integer as a string.
//
//   struct MyFormatter {
//     void operator()(string* out, int i) const {
//       out->append(std::to_string(i));
//     }
//   };
//
// You would use the above formatter by passing an instance of it as the final
// argument to `absl::StrJoin()`:
//
//   std::vector<int> v = {1, 2, 3, 4};
//   string s = absl::StrJoin(v, "-", MyFormatter());
//   EXPECT_EQ("1-2-3-4", s);
//
// The following standard formatters are provided within this file:
//
// - `AlphaNumFormatter()` (the default)
// - `StreamFormatter()`
// - `PairFormatter()`
// - `DereferenceFormatter()`

// AlphaNumFormatter()
//
// Default formatter used if none is specified. Uses `absl::AlphaNum` to convert
// numeric arguments to strings.
inline strings_internal::AlphaNumFormatterImpl AlphaNumFormatter() {
  return strings_internal::AlphaNumFormatterImpl();
}

// StreamFormatter()
//
// Formats its argument using the << operator.
inline strings_internal::StreamFormatterImpl StreamFormatter() {
  return strings_internal::StreamFormatterImpl();
}

// Function Template: PairFormatter(Formatter, absl::string_view, Formatter)
//
// Formats a `std::pair` by putting a given separator between the pair's
// `.first` and `.second` members. This formatter allows you to specify
// custom Formatters for both the first and second member of each pair.
template <typename FirstFormatter, typename SecondFormatter>
inline strings_internal::PairFormatterImpl<FirstFormatter, SecondFormatter>
PairFormatter(FirstFormatter f1, absl::string_view sep, SecondFormatter f2) {
  return strings_internal::PairFormatterImpl<FirstFormatter, SecondFormatter>(
      std::move(f1), sep, std::move(f2));
}

// Function overload of PairFormatter() for using a default
// `AlphaNumFormatter()` for each Formatter in the pair.
inline strings_internal::PairFormatterImpl<
    strings_internal::AlphaNumFormatterImpl,
    strings_internal::AlphaNumFormatterImpl>
PairFormatter(absl::string_view sep) {
  return PairFormatter(AlphaNumFormatter(), sep, AlphaNumFormatter());
}

// Function Template: DereferenceFormatter(Formatter)
//
// Formats its argument by dereferencing it and then applying the given
// formatter. This formatter is useful for formatting a container of
// pointer-to-T. This pattern often shows up when joining repeated fields in
// protocol buffers.
template <typename Formatter>
strings_internal::DereferenceFormatterImpl<Formatter> DereferenceFormatter(
    Formatter&& f) {
  return strings_internal::DereferenceFormatterImpl<Formatter>(
      std::forward<Formatter>(f));
}

// Function overload of `DererefenceFormatter()` for using a default
// `AlphaNumFormatter()`.
inline strings_internal::DereferenceFormatterImpl<
    strings_internal::AlphaNumFormatterImpl>
DereferenceFormatter() {
  return strings_internal::DereferenceFormatterImpl<
      strings_internal::AlphaNumFormatterImpl>(AlphaNumFormatter());
}

// -----------------------------------------------------------------------------
// StrJoin()
// -----------------------------------------------------------------------------
//
// Joins a range of elements and returns the result as a string.
// `absl::StrJoin()` takes a range, a separator string to use between the
// elements joined, and an optional Formatter responsible for converting each
// argument in the range to a string.
//
// If omitted, the default `AlphaNumFormatter()` is called on the elements to be
// joined.
//
// Example 1:
//   // Joins a collection of strings. This pattern also works with a collection
//   // of `absl::string_view` or even `const char*`.
//   std::vector<string> v = {"foo", "bar", "baz"};
//   string s = absl::StrJoin(v, "-");
//   EXPECT_EQ("foo-bar-baz", s);
//
// Example 2:
//   // Joins the values in the given `std::initializer_list<>` specified using
//   // brace initialization. This pattern also works with an initializer_list
//   // of ints or `absl::string_view` -- any `AlphaNum`-compatible type.
//   string s = absl::StrJoin({"foo", "bar", "baz"}, "-");
//   EXPECT_EQ("foo-bar-baz", s);
//
// Example 3:
//   // Joins a collection of ints. This pattern also works with floats,
//   // doubles, int64s -- any `StrCat()`-compatible type.
//   std::vector<int> v = {1, 2, 3, -4};
//   string s = absl::StrJoin(v, "-");
//   EXPECT_EQ("1-2-3--4", s);
//
// Example 4:
//   // Joins a collection of pointer-to-int. By default, pointers are
//   // dereferenced and the pointee is formatted using the default format for
//   // that type; such dereferencing occurs for all levels of indirection, so
//   // this pattern works just as well for `std::vector<int**>` as for
//   // `std::vector<int*>`.
//   int x = 1, y = 2, z = 3;
//   std::vector<int*> v = {&x, &y, &z};
//   string s = absl::StrJoin(v, "-");
//   EXPECT_EQ("1-2-3", s);
//
// Example 5:
//   // Dereferencing of `std::unique_ptr<>` is also supported:
//   std::vector<std::unique_ptr<int>> v
//   v.emplace_back(new int(1));
//   v.emplace_back(new int(2));
//   v.emplace_back(new int(3));
//   string s = absl::StrJoin(v, "-");
//   EXPECT_EQ("1-2-3", s);
//
// Example 6:
//   // Joins a `std::map`, with each key-value pair separated by an equals
//   // sign. This pattern would also work with, say, a
//   // `std::vector<std::pair<>>`.
//   std::map<string, int> m = {
//       std::make_pair("a", 1),
//       std::make_pair("b", 2),
//       std::make_pair("c", 3)};
//   string s = absl::StrJoin(m, ",", absl::PairFormatter("="));
//   EXPECT_EQ("a=1,b=2,c=3", s);
//
// Example 7:
//   // These examples show how `absl::StrJoin()` handles a few common edge
//   // cases:
//   std::vector<string> v_empty;
//   EXPECT_EQ("", absl::StrJoin(v_empty, "-"));
//
//   std::vector<string> v_one_item = {"foo"};
//   EXPECT_EQ("foo", absl::StrJoin(v_one_item, "-"));
//
//   std::vector<string> v_empty_string = {""};
//   EXPECT_EQ("", absl::StrJoin(v_empty_string, "-"));
//
//   std::vector<string> v_one_item_empty_string = {"a", ""};
//   EXPECT_EQ("a-", absl::StrJoin(v_one_item_empty_string, "-"));
//
//   std::vector<string> v_two_empty_string = {"", ""};
//   EXPECT_EQ("-", absl::StrJoin(v_two_empty_string, "-"));
//
// Example 8:
//   // Joins a `std::tuple<T...>` of heterogeneous types, converting each to
//   // a string using the `absl::AlphaNum` class.
//   string s = absl::StrJoin(std::make_tuple(123, "abc", 0.456), "-");
//   EXPECT_EQ("123-abc-0.456", s);

template <typename Iterator, typename Formatter>
std::string StrJoin(Iterator start, Iterator end, absl::string_view sep,
               Formatter&& fmt) {
  return strings_internal::JoinAlgorithm(start, end, sep, fmt);
}

template <typename Range, typename Formatter>
std::string StrJoin(const Range& range, absl::string_view separator,
               Formatter&& fmt) {
  return strings_internal::JoinRange(range, separator, fmt);
}

template <typename T, typename Formatter>
std::string StrJoin(std::initializer_list<T> il, absl::string_view separator,
               Formatter&& fmt) {
  return strings_internal::JoinRange(il, separator, fmt);
}

template <typename... T, typename Formatter>
std::string StrJoin(const std::tuple<T...>& value, absl::string_view separator,
               Formatter&& fmt) {
  return strings_internal::JoinAlgorithm(value, separator, fmt);
}

template <typename Iterator>
std::string StrJoin(Iterator start, Iterator end, absl::string_view separator) {
  return strings_internal::JoinRange(start, end, separator);
}

template <typename Range>
std::string StrJoin(const Range& range, absl::string_view separator) {
  return strings_internal::JoinRange(range, separator);
}

template <typename T>
std::string StrJoin(std::initializer_list<T> il, absl::string_view separator) {
  return strings_internal::JoinRange(il, separator);
}

template <typename... T>
std::string StrJoin(const std::tuple<T...>& value, absl::string_view separator) {
  return strings_internal::JoinAlgorithm(value, separator, AlphaNumFormatter());
}

}  // namespace absl

#endif  // ABSL_STRINGS_STR_JOIN_H_
