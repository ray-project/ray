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

#include "ray/util/array.h"

#include <gtest/gtest.h>

// Tests for array.h
// All tests are compile time tests with static_assert, because they are intended to be
// used in compile time code.

namespace ray {

// Unique array tests
static constexpr bool array_unique_true = ArrayIsUnique(std::array<int, 3>{1, 2, 3});
static constexpr bool array_unique_false = ArrayIsUnique(std::array<int, 3>{1, 2, 2});

static_assert(array_unique_true, "Array should have unique elements");
static_assert(!array_unique_false, "Array should not have unique elements");

// String view unique tests
static constexpr bool array_unique_sv_true =
    ArrayIsUnique(std::array<std::string_view, 3>{"a", "b", "c"});
static constexpr bool array_unique_sv_false =
    ArrayIsUnique(std::array<std::string_view, 3>{"a", "b", "b"});

static_assert(array_unique_sv_true, "String view array should have unique elements");
static_assert(!array_unique_sv_false,
              "String view array should not have unique elements");

static constexpr bool index_of_int = IndexOf(std::array<int, 3>{1, 2, 3}, 2) == 1;
static_assert(index_of_int, "Index of int should be 1");

// can't test IndexOf not found because it's a compile error

}  // namespace ray
