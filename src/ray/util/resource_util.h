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

// Each memory "resource" counts as this many bytes of memory.
const uint64_t MEMORY_RESOURCE_UNIT_BYTES = 50 * 1024 * 1024ULL;

inline double FromMemoryUnitsToGiB(uint64_t memory_units) {
  return memory_units * MEMORY_RESOURCE_UNIT_BYTES / (1024.0 * 1024 * 1024);
}