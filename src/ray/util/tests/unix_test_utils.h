// Copyright 2025 The Ray Authors.
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

// This file contains a series of testing utils which are specific to linux platform.

#pragma once

#if !defined(__APPLE__) && !defined(__linux__)
#error "This header file can only be used in unix"
#endif

#include <string>

namespace ray {

// Read the whole content for the given [fname], and return as string.
// If any error happens, throw exception.
std::string CompleteReadFile(const std::string &fname);

}  // namespace ray
