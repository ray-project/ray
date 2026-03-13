// Copyright 2026 The Ray Authors.
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
#include <memory>
#include <string>
#include <thread>

#include "absl/strings/str_format.h"
#include "ray/common/memory_monitor_interface.h"
#include "ray/common/status_or.h"

namespace ray {

/**
 * @brief Memory Pressure Stall Information (PSI) configuration.
 *
 * Determined by the "stall_proportion" (percentage) of "stall_duration_s" (time)
 * processes spent waiting for memory resources.
 */
struct MemoryPsi {
  std::string mode;
  float stall_proportion;
  uint32_t stall_duration_s;

  /**
   * @return true if the mode is either some or full, false otherwise.
   */
  bool IsValidMode() const { return mode == "some" || mode == "full"; }

  /**
   * @brief Validates that the stall duration is one of the supported values.
   *
   * PSI interface only supports window durations that are multiples of 2 seconds
   * and a maximum of 10 seconds.
   *
   * @return true if the stall duration is valid, false otherwise.
   */
  bool IsValidStallDuration() const {
    return stall_duration_s % 2 == 0 && stall_duration_s > 0 && stall_duration_s <= 10;
  }

  /**
   * @return true if the stall proportion is between 0.0 and 1.0, false otherwise.
   */
  bool IsValidStallProportion() const {
    return stall_proportion > 0.0f && stall_proportion <= 1.0f;
  }

  /**
   * @return true if the MemoryPsi configuration is valid, false otherwise.
   */
  bool IsValid() const {
    return IsValidMode() && IsValidStallDuration() && IsValidStallProportion();
  }
};

inline std::string to_string(const MemoryPsi &psi) {
  return absl::StrFormat("MemoryPsi{mode=%s, stall proportion=%f, stall duration=%ds}",
                         psi.mode,
                         psi.stall_proportion,
                         psi.stall_duration_s);
}

/**
 * @brief Pressure based memory monitor that monitors
 * cgroup memory usage based on linux PSI (Pressure Stall Information) interface.
 *
 * The monitor will trigger the kill callback when the given pressure threshold is
 * exceeded.
 *
 * This class is thread safe.
 */
class PressureMemoryMonitor : public MemoryMonitorInterface {
 public:
  /**
   * @param pressure_threshold the PSI threshold to trigger the kill callback.
   * @param cgroup_path the path to the cgroup whose memory pressure will be monitored.
   * @param kill_workers_callback function to execute when memory pressure is detected.
   * @return The PressureMemoryMonitor instance, or an error status if initialization
   * fails.
   */
  static StatusOr<std::unique_ptr<PressureMemoryMonitor>> Create(
      MemoryPsi pressure_threshold,
      std::string cgroup_path,
      KillWorkersCallback kill_workers_callback);

  /**
   * @param cgroup_path the path to the cgroup whose memory pressure will be monitored.
   * @param memory_pressure_fd file descriptor for the memory.pressure file.
   * The pressure memory will own the given file descriptor and will be responsible
   * for closing it.
   * @param kill_workers_callback function to execute when memory pressure is detected.
   */
  PressureMemoryMonitor(const std::string &cgroup_path,
                        int memory_pressure_fd,
                        KillWorkersCallback kill_workers_callback);

  ~PressureMemoryMonitor() override;

  /**
   * @brief Enables the memory monitor to trigger the kill callback.
   */
  void Enable() override;

  /**
   * @brief Disables the memory monitor from triggering the kill callback.
   */
  void Disable() override;

  /**
   * @return True if the memory monitor is enabled, false otherwise.
   */
  bool IsEnabled() override;

 private:
  /**
   * @brief Monitoring loop that polls on the memory pressure file,
   * monitoring it for pressure exceeding trigger events.
   */
  void MonitoringThreadMain();

  /// The path to the cgroup that the pressure monitor for memory pressure
  std::string cgroup_path_;

  /// File descriptor for the memory.pressure file.
  int pressure_fd_;

  /// Callback function that executes when memory pressure is detected.
  KillWorkersCallback kill_workers_callback_;

  /// Flag to indicate that the worker killing event is in progress.
  std::atomic<bool> worker_killing_in_progress_;

  /// Eventfd used to wake the monitoring thread on shutdown.
  int shutdown_event_fd_;

  /// Thread for monitoring memory pressure.
  std::thread thread_;
};

}  // namespace ray
