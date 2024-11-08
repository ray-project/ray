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

#include <string>

#include "absl/strings/internal/resize_uninitialized.h"

namespace ray::utils::strings {

inline void STLStringResizeUninitialized(std::string *s, size_t new_size) {
  absl::strings_internal::STLStringResizeUninitialized(s, new_size);
}

// Create a string with the given [len] without initializing its memory.
inline std::string CreateStringWithSizeUninitialized(size_t len) {
  std::string s;
  STLStringResizeUninitialized(&s, len);
  return s;
}

}  // namespace ray::utils::strings
