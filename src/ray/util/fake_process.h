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

#include "ray/util/process_interface.h"

namespace ray {

/**
 * @class FakeProcess
 * @brief A fake process implementation providing barebone mocked functionality for
 * testing.
 */
class FakeProcess : public ProcessInterface {
 public:
  /**
   * @brief Creates a fake process with default settings.
   * @details By default: pid=-1, is_alive=true, exit_code=0, is_null=false
   */
  FakeProcess() : FakeProcess(-1) {}

  /**
   * @brief Creates a fake process with a specific PID.
   * @details The process is set to alive by default.
   *          The fake process state can be configured using setters.
   * @param pid The process ID to use.
   */
  explicit FakeProcess(pid_t pid)
      : pid_(pid), is_alive_(true), exit_code_(0), is_null_(false), killed_(false) {}

  pid_t GetId() const override { return is_null_ ? -1 : pid_; }

  bool IsNull() const override { return is_null_; }

  bool IsValid() const override { return !is_null_ && pid_ >= 0; }

  void Kill() override {
    killed_ = true;
    is_alive_ = false;
  }

  bool IsAlive() const override { return is_alive_; }

  int Wait() const override {
    if (is_null_) {
      return -1;
    }
    return exit_code_;
  }

  // Setters for mocking process with custom states.

  void SetPid(pid_t pid) { pid_ = pid; }

  void SetAlive(bool alive) { is_alive_ = alive; }

  void SetExitCode(int code) { exit_code_ = code; }

  void SetNull(bool is_null) { is_null_ = is_null; }

  bool WasKilled() const { return killed_; }

  void ResetKilled() { killed_ = false; }

 private:
  pid_t pid_;
  bool is_alive_;
  int exit_code_;
  bool is_null_;
  bool killed_;
};

}  // namespace ray
