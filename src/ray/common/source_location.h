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

#include <iostream>
#include <string_view>

namespace ray {

// A struct which represents source code location (aka. filename and line
// number), expected to use only via internal macros.
//
// TODO(hjiang): Remove default constructor after we upgrade to C++20, which has
// designated initializer. Ref:
// https://en.cppreference.com/w/cpp/language/aggregate_initialization
struct SourceLocation {
  SourceLocation() = default;
  SourceLocation(std::string_view fname, int line) : filename(fname), line_no(line) {}

  // Via `__FILE__` macros, memory ownership lies in data segment and never destructs.
  std::string_view filename;
  int line_no = 0;
};

// Whether given [loc] indicates a valid source code location.
bool IsValidSourceLoc(const SourceLocation &loc);

std::ostream &operator<<(std::ostream &os, const SourceLocation &loc);

}  // namespace ray

#define RAY_LOC() \
  ray::SourceLocation { __FILE__, __LINE__ }
