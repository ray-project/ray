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

#include "ray/common/pressure_memory_monitor.h"

#include <fcntl.h>
#include <sys/eventfd.h>
#include <sys/poll.h>
#include <unistd.h>

#include <filesystem>
#include <string>

#include "ray/common/memory_monitor_utils.h"
#include "ray/util/logging.h"
#include "ray/util/thread_utils.h"

namespace ray {

StatusOr<std::unique_ptr<PressureMemoryMonitor>> PressureMemoryMonitor::Create(
    MemoryPsi pressure_threshold,
    std::string cgroup_path,
    KillWorkersCallback kill_workers_callback) {
  if (!pressure_threshold.IsValid()) {
    return Status::InvalidArgument(
        absl::StrFormat("Failed to initialize PressureMemoryMonitor due to "
                        "invalid pressure threshold configuration: %s",
                        to_string(pressure_threshold)));
  }

  std::string memory_pressure_path =
      cgroup_path + std::filesystem::path::preferred_separator + "memory.pressure";
  int pressure_fd = open(memory_pressure_path.c_str(), O_RDWR | O_NONBLOCK);
  if (pressure_fd < 0) {
    return Status::IOError(absl::StrFormat(
        "Failed to initialize PressureMemoryMonitor due to "
        "failure to open memory.pressure file at path: %s, errno: %d, error: %s",
        memory_pressure_path,
        errno,
        strerror(errno)));
  }

  // PSI trigger string format: "<mode> <threshold_us> <window_us>"
  uint32_t window_us = pressure_threshold.stall_duration_s * 1000000;
  uint32_t threshold_us =
      static_cast<uint32_t>(pressure_threshold.stall_proportion * window_us);
  std::string trigger_str =
      absl::StrFormat("%s %u %u", pressure_threshold.mode, threshold_us, window_us);

  // Register PSI trigger with memory.pressure
  if (write(pressure_fd, trigger_str.c_str(), trigger_str.length()) !=
      static_cast<ssize_t>(trigger_str.length())) {
    std::string error_msg = absl::StrFormat(
        "Failed to initialize PressureMemoryMonitor due to "
        "failure to write PSI trigger to memory.pressure file at path: %s, "
        "trigger string: '%s', errno: %d, error: %s",
        memory_pressure_path,
        trigger_str,
        errno,
        strerror(errno));
    close(pressure_fd);
    return Status::IOError(error_msg);
  }

  std::unique_ptr<PressureMemoryMonitor> monitor =
      std::make_unique<PressureMemoryMonitor>(
          cgroup_path, pressure_fd, std::move(kill_workers_callback));

  RAY_LOG(INFO) << absl::StrFormat(
      "Pressure memory monitor successfully initialized with: "
      "memory pressure path: %s, and "
      "pressure threshold: %s",
      memory_pressure_path,
      to_string(pressure_threshold));

  return monitor;
}

PressureMemoryMonitor::PressureMemoryMonitor(const std::string &cgroup_path,
                                             int memory_pressure_fd,
                                             KillWorkersCallback kill_workers_callback)
    : cgroup_path_(cgroup_path),
      pressure_fd_(memory_pressure_fd),
      kill_workers_callback_(std::move(kill_workers_callback)),
      worker_killing_in_progress_(false),
      shutdown_event_fd_(eventfd(0, EFD_NONBLOCK)) {
  RAY_CHECK(shutdown_event_fd_ >= 0) << absl::StrFormat(
      "Failed to initialize PressureMemoryMonitor due to "
      "failure to create shutdown event fd, errno: %d, error: %s",
      errno,
      strerror(errno));
  thread_ = std::thread([this] {
    SetThreadName("PressureMemoryMonitor.MonitoringThread");
    MonitoringThreadMain();
  });
}

PressureMemoryMonitor::~PressureMemoryMonitor() {
  uint64_t val = 1;
  ssize_t rc = write(shutdown_event_fd_, &val, sizeof(val));
  RAY_CHECK(rc == sizeof(val)) << absl::StrFormat(
      "PressureMemoryMonitor could not be successfully cleaned up due to "
      "Failure to signal shutdown to monitoring thread via eventfd, errno: %d, error: %s",
      errno,
      strerror(errno));

  if (thread_.joinable()) {
    thread_.join();
  }

  close(shutdown_event_fd_);
  close(pressure_fd_);
}

void PressureMemoryMonitor::Enable() { worker_killing_in_progress_.store(false); }

void PressureMemoryMonitor::Disable() { worker_killing_in_progress_.store(true); }

bool PressureMemoryMonitor::IsEnabled() { return !worker_killing_in_progress_.load(); }

void PressureMemoryMonitor::MonitoringThreadMain() {
  struct pollfd fds[2];
  fds[0].fd = pressure_fd_;
  fds[0].events = POLLPRI;
  fds[1].fd = shutdown_event_fd_;
  fds[1].events = POLLIN;

  while (true) {
    int ret = poll(fds, 2, -1);

    if (ret < 0) {
      if (errno == EINTR) {
        continue;
      }
      RAY_LOG(ERROR) << absl::StrFormat(
          "Poll failed, errno: %d, error: %s. "
          "Pressure memory monitoring thread stopping.",
          errno,
          strerror(errno));
      break;
    }

    if (fds[1].revents & POLLIN) {
      break;
    }

    if (IsEnabled()) {
      if (fds[0].revents & POLLPRI) {
        Disable();
        kill_workers_callback_(
            MemoryMonitorUtils::TakeSystemMemorySnapshot(cgroup_path_));
      } else if (fds[0].revents & POLLERR) {
        RAY_LOG(ERROR) << "Got POLLERR while monitoring memory pressure. "
                       << "This likely indicates that the event source is gone, "
                       << "Pressure memory monitoring thread is stopping.";
        break;
      } else if (fds[0].revents) {
        RAY_LOG(ERROR) << absl::StrFormat(
            "Got unexpected event while monitoring memory pressure, event: 0x%x. "
            "Pressure memory monitoring thread is stopping.",
            fds[0].revents);
        break;
      }
    }
  }
}

}  // namespace ray
