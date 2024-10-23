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

#include <cstddef>
namespace ray {

template <typename T, size_t N>
constexpr size_t array_size(const T (&/*unused*/)[N]) {
  return N;
}

template <typename T, size_t N>
constexpr bool array_is_unique(const T (&arr)[N]) {
  for (size_t i = 0; i < N; ++i) {
    for (size_t j = i + 1; j < N; ++j) {
      if (arr[i] == arr[j]) {
        return false;
      }
    }
  }
  return true;
}

}  // namespace ray
