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

#include <functional>
#include <memory>
#include <string>
#include <system_error>
#include <vector>

#include "ray/util/process_interface.h"

namespace ray {

class ProcessFD {
  pid_t pid_;
  intptr_t fd_;

 public:
  ~ProcessFD();
  ProcessFD();
  explicit ProcessFD(pid_t pid, intptr_t fd = -1);
  ProcessFD(ProcessFD &&other);
  ProcessFD &operator=(ProcessFD &&other);

  ProcessFD(const ProcessFD &other) = delete;
  ProcessFD &operator=(const ProcessFD &other) = delete;

  void CloseFD();
  intptr_t GetFD() const;
  pid_t GetId() const;

  // Fork + exec combo. Returns -1 for the PID on failure.
  static ProcessFD spawnvpe(const char *argv[],
                            std::error_code &ec,
                            bool decouple,
                            const ProcessEnvironment &env,
                            bool pipe_to_stdin,
                            std::function<void(const std::string &)> add_to_cgroup,
                            bool new_process_group);
};

}  // namespace ray
