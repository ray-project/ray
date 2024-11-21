/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace libkineto {

int32_t systemThreadId();
int32_t threadId();
bool setThreadName(const std::string& name);
std::string getThreadName();

int32_t processId();
std::string processName(int32_t pid);

// Return a list of pids and process names for the current process
// and its parents.
std::vector<std::pair<int32_t, std::string>> pidCommandPairsOfAncestors();

} // namespace libkineto
