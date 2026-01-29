// Copyright 2022 The Ray Authors.
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

#include "ray/common/threshold_memory_monitor.h"

#include "absl/strings/str_format.h"
#include "ray/util/logging.h"
#include "ray/util/thread_utils.h"

namespace ray {

ThresholdMemoryMonitor::ThresholdMemoryMonitor(KillWorkersCallback kill_workers_callback,
                                               float usage_threshold,
                                               int64_t min_memory_free_bytes,
                                               uint64_t monitor_interval_ms)
    : MemoryMonitor(std::move(kill_workers_callback)),
      io_service_(/*enable_metrics=*/false,
                  /*running_on_single_thread=*/true,
                  "MemoryMonitor.IOContext"),
      work_guard_(boost::asio::make_work_guard(io_service_.get_executor())),
      thread_([this] {
        SetThreadName("MemoryMonitor.IOContextThread");
        io_service_.run();
      }),
      runner_(PeriodicalRunner::Create(io_service_)) {
  if (monitor_interval_ms > 0) {
    auto [_, total_memory_bytes] = MemoryMonitor::GetMemoryBytes();
    computed_threshold_bytes_ = MemoryMonitor::GetMemoryThreshold(
        total_memory_bytes, usage_threshold, min_memory_free_bytes);
    computed_threshold_fraction_ = static_cast<float>(computed_threshold_bytes_) /
                                   static_cast<float>(total_memory_bytes);
    RAY_LOG(INFO) << "MemoryMonitor initialized with usage threshold at "
                  << computed_threshold_bytes_ << " bytes ("
                  << absl::StrFormat("%.2f", computed_threshold_fraction_)
                  << " system memory), total system memory bytes: " << total_memory_bytes;
    runner_->RunFnPeriodically(
        [this] {
          auto [used_mem_bytes, total_mem_bytes] = MemoryMonitor::GetMemoryBytes();
          MemorySnapshot system_memory;
          system_memory.used_bytes = used_mem_bytes;
          system_memory.total_bytes = total_mem_bytes;

          bool is_usage_above_threshold = MemoryMonitor::IsUsageAboveThreshold(
              system_memory, computed_threshold_bytes_);

          if (is_usage_above_threshold) {
            system_memory.process_used_bytes = MemoryMonitor::GetProcessMemoryUsage();
            kill_workers_callback_(std::move(system_memory));
          }
        },
        monitor_interval_ms,
        "MemoryMonitor.CheckIsMemoryUsageAboveThreshold");
  } else {
    RAY_LOG(INFO) << "MemoryMonitor disabled. Specify "
                  << "`memory_monitor_refresh_ms` > 0 to enable the monitor.";
  }
}

ThresholdMemoryMonitor::~ThresholdMemoryMonitor() {
  runner_.reset();
  io_service_.stop();
  if (thread_.joinable()) {
    thread_.join();
  }
}

}  // namespace ray
