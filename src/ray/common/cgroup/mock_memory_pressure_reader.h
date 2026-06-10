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
#pragma once

#include <cstdint>
#include <deque>
#include <string>
#include <tuple>
#include <utility>

#include "ray/common/cgroup/memory_pressure_reader.h"
#include "ray/common/status.h"

namespace ray {

// Injects a scripted sequence of (current, limit, status) reads for unit
// tests of MemoryPressureMonitor. Calls beyond the script replay the last
// scripted entry.
class MockMemoryPressureReader : public MemoryPressureReader {
 public:
  void Push(int64_t current, int64_t limit) {
    script_.emplace_back(std::make_tuple(current, limit, Status::OK()));
  }
  void PushError(const std::string &msg) {
    script_.emplace_back(std::make_tuple(int64_t{0}, int64_t{0}, Status::IOError(msg)));
  }

  Status Read(int64_t *current_bytes, int64_t *limit_bytes) override {
    ++calls_;
    if (script_.empty()) {
      return Status::IOError("mock: script exhausted");
    }
    auto entry = script_.front();
    if (script_.size() > 1) {
      script_.pop_front();
    }
    if (!std::get<2>(entry).ok()) {
      return std::get<2>(entry);
    }
    *current_bytes = std::get<0>(entry);
    *limit_bytes = std::get<1>(entry);
    return Status::OK();
  }

  int calls() const { return calls_; }

 private:
  std::deque<std::tuple<int64_t, int64_t, Status>> script_;
  int calls_ = 0;
};

}  // namespace ray
