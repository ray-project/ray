// Copyright 2017 The Ray Authors.
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

#include <random>

#include "absl/time/clock.h"

// Randomly samples num_elements from the elements between first and last using reservoir
// sampling.
template <class Iterator, class T = typename std::iterator_traits<Iterator>::value_type>
void random_sample(Iterator begin,
                   Iterator end,
                   size_t num_elements,
                   std::vector<T> *out) {
  out->resize(0);
  if (num_elements == 0) {
    return;
  }

  std::default_random_engine gen(absl::GetCurrentTimeNanos());
  size_t current_index = 0;
  for (auto it = begin; it != end; it++) {
    if (current_index < num_elements) {
      out->push_back(*it);
    } else {
      size_t random_index = std::uniform_int_distribution<size_t>(0, current_index)(gen);
      if (random_index < num_elements) {
        out->at(random_index) = *it;
      }
    }
    current_index++;
  }
  return;
}
