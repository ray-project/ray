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

#include "ray/common/event_memory_monitor.h"

#include <sys/eventfd.h>
#include <sys/inotify.h>
#include <sys/poll.h>
#include <unistd.h>

#include <boost/algorithm/string.hpp>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "ray/common/memory_monitor_utils.h"
#include "ray/util/logging.h"
#include "ray/util/thread_utils.h"

namespace ray {

StatusSetOr<std::unique_ptr<EventMemoryMonitor>, StatusT::IOError>
EventMemoryMonitor::Create(std::string cgroup_path,
                           KillWorkersCallback kill_workers_callback) {
  std::string memory_events_path =
      cgroup_path + std::filesystem::path::preferred_separator + "memory.events";

  // Set up fd to listen to memory events
  int inotify_fd = inotify_init1(IN_NONBLOCK);
  if (inotify_fd < 0) {
    return StatusT::IOError(
        absl::StrFormat("Failed to initialize inotify fs to listen to memory events "
                        "due to errno: %d, error: %s",
                        errno,
                        strerror(errno)));
  }

  int inotify_wd = inotify_add_watch(inotify_fd, memory_events_path.c_str(), IN_MODIFY);
  if (inotify_wd < 0) {
    std::string error_msg = absl::StrFormat(
        "Failed to add inotify watch for memory.events at path: %s "
        "due to errno: %d, error: %s",
        memory_events_path,
        errno,
        strerror(errno));
    close(inotify_fd);
    return StatusT::IOError(error_msg);
  }

  std::unique_ptr<EventMemoryMonitor> monitor(
      new EventMemoryMonitor(std::move(cgroup_path),
                             memory_events_path,
                             inotify_fd,
                             inotify_wd,
                             std::move(kill_workers_callback)));

  RAY_LOG(INFO) << absl::StrFormat(
      "Event memory monitor successfully initialized with "
      "memory events path: %s",
      memory_events_path);

  return monitor;
}

EventMemoryMonitor::EventMemoryMonitor(std::string cgroup_path,
                                       std::string memory_events_path,
                                       int inotify_fd,
                                       int inotify_wd,
                                       KillWorkersCallback kill_workers_callback)
    : cgroup_path_(std::move(cgroup_path)),
      memory_events_path_(std::move(memory_events_path)),
      prev_memory_events_high_(0),
      inotify_fd_(inotify_fd),
      inotify_wd_(inotify_wd),
      kill_workers_callback_(std::move(kill_workers_callback)),
      worker_killing_in_progress_(false),
      shutdown_eventfd_(eventfd(0, EFD_NONBLOCK)) {
  RAY_CHECK(shutdown_eventfd_ >= 0) << absl::StrFormat(
      "Failed to initialize EventMemoryMonitor due to "
      "failure to create shutdown event fd, errno: %d, error: %s",
      errno,
      strerror(errno));
  event_monitoring_thread_ = std::thread([this] {
    SetThreadName("EventMemoryMonitor.MonitoringThread");
    MonitoringThreadMain();
  });
}

EventMemoryMonitor::~EventMemoryMonitor() {
  uint64_t val = 1;
  if (write(shutdown_eventfd_, &val, sizeof(val)) != sizeof(val)) {
    RAY_LOG(ERROR) << absl::StrFormat(
        "Failed to signal shutdown to event monitoring thread, errno: %d", errno);
  }

  if (event_monitoring_thread_.joinable()) {
    event_monitoring_thread_.join();
  }

  close(shutdown_eventfd_);
  inotify_rm_watch(inotify_fd_, inotify_wd_);
  close(inotify_fd_);
}

void EventMemoryMonitor::Enable() { worker_killing_in_progress_.store(false); }

void EventMemoryMonitor::Disable() { worker_killing_in_progress_.store(true); }

bool EventMemoryMonitor::IsEnabled() { return !worker_killing_in_progress_.load(); }

void EventMemoryMonitor::MonitoringThreadMain() {
  struct pollfd fds[2];
  fds[0].fd = inotify_fd_;
  fds[0].events = POLLIN;
  fds[1].fd = shutdown_eventfd_;
  fds[1].events = POLLIN;

  while (true) {
    int ret = poll(fds, 2, -1);

    if (ret < 0) {
      if (errno == EINTR) {
        continue;
      }
      RAY_LOG(ERROR) << absl::StrFormat(
          "Poll failed, errno: %d, error: %s. "
          "Event monitoring thread stoppping.",
          errno,
          strerror(errno));
      return;
    }

    if (fds[1].revents & POLLIN) {
      return;
    }

    if (fds[0].revents & POLLIN) {
      // Drain all pending inotify events to ensure poll will
      // only return on next new event.
      DrainResult drain_result = DrainInotifyBuffer(inotify_fd_);
      if (drain_result == DrainResult::kInterrupted) {
        // Re-enter poll loop if interrupt was signaled in case a terminate
        // signal was received.
        continue;
      }
      if (drain_result == DrainResult::kError) {
        RAY_LOG(ERROR) << absl::StrFormat(
            "Failed to drain inotify buffer while monitoring memory events. "
            "Event monitoring thread is stoppping, errno: %d, error: %s",
            errno,
            strerror(errno));
        return;
      }

      bool high_modified = false;
      std::ifstream mem_events_file(memory_events_path_);
      if (mem_events_file) {
        std::string line;
        while (std::getline(mem_events_file, line)) {
          std::vector<std::string> tokens;
          boost::split(tokens, line, boost::is_any_of(" "));

          // Expected memory.events format:
          // low 0
          // high 231226
          // max 1695
          // oom 26620
          // oom_kill 20
          if (line.find("high") != std::string::npos) {
            int64_t high_count = std::stoll(tokens[1]);
            if (high_count > prev_memory_events_high_) {
              prev_memory_events_high_ = high_count;
              high_modified = true;
            }
          }
        }
      } else {
        RAY_LOG(ERROR) << absl::StrFormat(
            "Failed read memory.events file: %s when checking "
            "for memory events. Event monitoring thread is stoppping.",
            memory_events_path_);
        return;
      }

      if (high_modified && IsEnabled()) {
        Disable();
        kill_workers_callback_(
            MemoryMonitorUtils::TakeSystemMemorySnapshot(cgroup_path_));
      }
    } else {
      RAY_LOG(ERROR) << absl::StrFormat(
          "Got unexpected revent while monitoring memory events, event: 0x%x. "
          "Event monitoring thread is stoppping.",
          fds[0].revents);
      return;
    }
  }
}

EventMemoryMonitor::DrainResult EventMemoryMonitor::DrainInotifyBuffer(int inotify_fd) {
  char inotify_buffer[256];
  while (true) {
    int read_bytes = read(inotify_fd, inotify_buffer, sizeof(inotify_buffer));
    if (read_bytes > 0) {
      continue;
    }
    if (read_bytes == 0 || errno == EAGAIN) {
      return DrainResult::kDrained;
    }
    if (errno == EINTR) {
      return DrainResult::kInterrupted;
    }
    return DrainResult::kError;
  }
}

}  // namespace ray
