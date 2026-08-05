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

// TEMPORARY INSTRUMENTATION -- DO NOT MERGE.
//
// Measures where time goes on the task-event record path so the remaining flags-on
// throughput gap can be attributed instead of guessed at. Written to stderr rather than
// RAY_LOG because release-test jobs upload no worker logs, while worker stderr is
// forwarded to the driver and therefore lands in the Buildkite job log.

#pragma once

#include <atomic>
#include <cstdint>
#include <iostream>
#include <string>

#include "absl/time/clock.h"

namespace ray {
namespace observability {
namespace instr {

inline int64_t NowNanos() { return absl::GetCurrentTimeNanos(); }

// Record-path counters, all monotonic.
inline std::atomic<int64_t> n_records{0};     // gated calls to the record helper
inline std::atomic<int64_t> ns_record{0};     // whole record-helper body
inline std::atomic<int64_t> n_events{0};      // events handed to the recorder
inline std::atomic<int64_t> ns_construct{0};  // event wrapper construction only
inline std::atomic<int64_t> ns_add{0};        // AddEvent body, lock included
inline std::atomic<int64_t> ns_lockwait{0};   // mutex acquisition inside AddEvent
inline std::atomic<int64_t> n_dropped{0};  // events dropped (ring full / sticky / caps)
// Export-path counters.
inline std::atomic<int64_t> n_exports{0};
inline std::atomic<int64_t> ns_export_locked{0};  // extraction while holding mutex_
inline std::atomic<int64_t> ns_export_total{0};   // extraction + group/serialize + send

inline double Avg(const std::atomic<int64_t> &total, const std::atomic<int64_t> &count) {
  const int64_t c = count.load();
  return c == 0 ? 0.0 : static_cast<double>(total.load()) / static_cast<double>(c);
}

// Emit one summary line. Called every `every_n` exports so the output shows steady state
// without flooding the driver.
inline void MaybeReport(int64_t every_n) {
  const int64_t exports = n_exports.load();
  if (every_n <= 0 || exports == 0 || exports % every_n != 0) {
    return;
  }
  std::cerr << "[TASKEVENT-INSTR]"
            << " records=" << n_records.load()
            << " record_avg_ns=" << Avg(ns_record, n_records)
            << " events=" << n_events.load()
            << " construct_avg_ns=" << Avg(ns_construct, n_events)
            << " add_avg_ns=" << Avg(ns_add, n_events)
            << " lockwait_avg_ns=" << Avg(ns_lockwait, n_events)
            << " dropped=" << n_dropped.load() << " drop_frac="
            << (n_events.load() == 0 ? 0.0
                                     : static_cast<double>(n_dropped.load()) /
                                           static_cast<double>(n_events.load()))
            << " exports=" << exports
            << " export_total_avg_us=" << Avg(ns_export_total, n_exports) / 1000.0
            << " export_locked_avg_us=" << Avg(ns_export_locked, n_exports) / 1000.0
            << std::endl;
}

}  // namespace instr
}  // namespace observability
}  // namespace ray
