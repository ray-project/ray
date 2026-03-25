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
#include <functional>
#include <memory>
#include <string>
#include <thread>

#include "ray/common/memory_monitor_interface.h"
#include "ray/common/status.h"

namespace ray {
/**
 * @brief Event based memory monitor that monitors
 * memory events when the memory.high is reached or exceeded
 * and trigger the kill workers callback.
 *
 * This class is thread safe.
 */
class EventMemoryMonitor : public MemoryMonitorInterface {
 public:
  /**
   * @param cgroup_path the path to the cgroup whose memory events will be monitored.
   * @param kill_workers_callback function to invoke when memory.high is exceeded.
   * @return The EventMemoryMonitor instance, or an error status if initialization
   *         fails.
   */
  static StatusSetOr<std::unique_ptr<EventMemoryMonitor>, StatusT::IOError> Create(
      std::string cgroup_path, KillWorkersCallback kill_workers_callback);

  ~EventMemoryMonitor() override;

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
   * @param cgroup_path the path to the cgroup whose memory events will be monitored.
   * @param memory_events_path the path to the memory.events file within the cgroup.
   * @param inotify_fd fd to listen to memory events on.
   * @param inotify_wd watch descriptor for the memory events file.
   * @param kill_workers_callback function to invoke when memory.high is exceeded.
   */
  EventMemoryMonitor(std::string cgroup_path,
                     std::string memory_events_path,
                     int inotify_fd,
                     int inotify_wd,
                     KillWorkersCallback kill_workers_callback);

  /**
   * @brief Monitoring loop that polls on the memory events file,
   * monitoring it for events where the memory usage exceeds memory.high.
   */
  void MonitoringThreadMain();

  /// The path to the cgroup whose memory events will be monitored.
  std::string cgroup_path_;

  /// Path to the memory.events file within the cgroup to monitor.
  std::string memory_events_path_;

  /// Previous count of memory.events 'high' events.
  int64_t prev_memory_events_high_;

  /// File descriptor for the memory events file to monitor.
  int inotify_fd_;

  /// Watch descriptor for the memory events file.
  int inotify_wd_;

  /// Callback function that executes when memory pressure is detected.
  KillWorkersCallback kill_workers_callback_;

  /// Flag to indicate that the worker killing event is in progress.
  std::atomic<bool> worker_killing_in_progress_;

  /// Eventfd used to wake the monitoring thread on shutdown.
  int shutdown_eventfd_;

  /// Thread for monitoring memory events.
  std::thread event_monitoring_thread_;
};

}  // namespace ray
