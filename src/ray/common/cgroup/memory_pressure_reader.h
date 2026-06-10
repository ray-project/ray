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
#include <string>

#include "ray/common/status.h"

namespace ray {

// Reads the pod-level cgroup memory usage/limit pair. The raylet and all worker
// processes share the same pod-scoped cgroup in Kubernetes, so a single read
// reflects the aggregate footprint of the pod.
//
// Used by the MemoryPressureMonitor to feed IPPR decisions. The
// abstraction lets unit tests inject a fake sequence of (current, limit)
// values without touching the real sysfs.
class MemoryPressureReader {
 public:
  virtual ~MemoryPressureReader() = default;
  // Reads the latest usage snapshot. On success, writes current/limit bytes
  // to the given pointers and returns Status::OK. On any read failure (file
  // missing, permission denied, malformed content) returns Status::IOError
  // without touching the pointers.
  virtual Status Read(int64_t *current_bytes, int64_t *limit_bytes) = 0;
};

// Reads memory usage from the kernel sysfs cgroup files. On construction,
// probes `/sys/fs/cgroup/cgroup.controllers`: if present, uses cgroup v2
// files (`memory.current`, `memory.max`); otherwise falls back to cgroup v1
// (`memory.usage_in_bytes`, `memory.limit_in_bytes`).
class FileMemoryPressureReader : public MemoryPressureReader {
 public:
  // Production constructor — probes the real sysfs root.
  FileMemoryPressureReader();
  // Test constructor — lets the caller point at a fake sysfs tree.
  // `cgroup_root` is the directory containing `cgroup.controllers` (v2
  // probe file) plus the memory files. If `force_v2` is true, skips the
  // probe and treats the tree as v2; if false, treats as v1.
  FileMemoryPressureReader(std::string cgroup_root, bool force_v2);

  // Single-argument constructor: points at a fake sysfs tree but detects v2/v1 on its
  // own, replicating the detection semantics of the no-argument production constructor
  // (cgroup.controllers present -> v2, otherwise v1). Integration tests inject via the
  // RAY_IPPR_TEST_CGROUP_ROOT environment variable, letting a real raylet read a fake
  // cgroup and produce a stable high ratio without consuming host memory. Unlike the
  // two-argument constructor: force_v2 is no longer hardcoded by the caller, but
  // determined automatically based on whether cgroup_root/cgroup.controllers exists.
  explicit FileMemoryPressureReader(std::string cgroup_root);

  Status Read(int64_t *current_bytes, int64_t *limit_bytes) override;

  bool is_cgroup_v2() const { return is_v2_; }

 private:
  // Reads a single integer (or "max") from a file. Returns IOError on any
  // problem; on the v2 sentinel `max`, writes INT64_MAX.
  Status ReadInt64File(const std::string &path, int64_t *out);

  std::string current_path_;
  std::string limit_path_;
  bool is_v2_;
};

}  // namespace ray
