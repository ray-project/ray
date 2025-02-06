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

#pragma once

#include <array>
#include <cstddef>

namespace ray {

// Warning: O(n^2) complexity. Only use in constexpr context.
// Returns true if there are no 2 elements in the array that are equal.
template <typename T, size_t N>
constexpr bool ArrayIsUnique(const std::array<T, N> &arr) {
  for (size_t i = 0; i < N; ++i) {
    for (size_t j = i + 1; j < N; ++j) {
      if (arr[i] == arr[j]) {
        return false;
      }
    }
  }
  return true;
}

// Returns the index of the first occurrence of value in the array. If value is not found,
// it's a compile error.
template <typename T, size_t N>
constexpr size_t IndexOf(const std::array<T, N> &arr, const T &value) {
  for (size_t i = 0; i < N; ++i) {
    if (arr[i] == value) {
      return i;
    }
  }
  // Throwing in constexpr context leads to a compile error.
  throw "Value not found in array";
}

}  // namespace ray
