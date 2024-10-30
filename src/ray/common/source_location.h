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
//
// A struct which represents source code location (aka. filename and line
// number).

#pragma once

#include <iostream>

namespace ray {

struct SourceLocation {
  std::string_view filename;
  int line_no = 0;
};

// Whether given [loc] indicates a valid source code location.
bool IsValidSourceLoc(const SourceLocation &loc);

std::ostream &operator<<(std::ostream &os, const SourceLocation &loc);

}  // namespace ray

#define RAY_LOC() \
  ray::SourceLocation { .filename = __FILE__, .line_no = __LINE__, }
