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
#include "ray/common/cgroup/memory_pressure_reader.h"

#include <cerrno>
#include <climits>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <system_error>

namespace ray {

namespace {

constexpr char kDefaultCgroupRoot[] = "/sys/fs/cgroup";
constexpr char kCgroupV2ProbeFile[] = "cgroup.controllers";
constexpr char kCgroupV2CurrentFile[] = "memory.current";
constexpr char kCgroupV2LimitFile[] = "memory.max";
constexpr char kCgroupV1CurrentFile[] = "memory.usage_in_bytes";
constexpr char kCgroupV1LimitFile[] = "memory.limit_in_bytes";

bool ExistsNoThrow(const std::string &path) {
  std::error_code ec;
  return std::filesystem::exists(path, ec);
}

}  // namespace

FileMemoryPressureReader::FileMemoryPressureReader()
    : FileMemoryPressureReader(
          kDefaultCgroupRoot,
          ExistsNoThrow(std::string(kDefaultCgroupRoot) + "/" + kCgroupV2ProbeFile)) {}

FileMemoryPressureReader::FileMemoryPressureReader(std::string cgroup_root)
    : FileMemoryPressureReader(
          cgroup_root, ExistsNoThrow(cgroup_root + "/" + kCgroupV2ProbeFile)) {}

FileMemoryPressureReader::FileMemoryPressureReader(std::string cgroup_root,
                                                   bool force_v2)
    : is_v2_(force_v2) {
  if (is_v2_) {
    current_path_ = cgroup_root + "/" + kCgroupV2CurrentFile;
    limit_path_ = cgroup_root + "/" + kCgroupV2LimitFile;
  } else {
    current_path_ = cgroup_root + "/" + kCgroupV1CurrentFile;
    limit_path_ = cgroup_root + "/" + kCgroupV1LimitFile;
  }
}

Status FileMemoryPressureReader::Read(int64_t *current_bytes, int64_t *limit_bytes) {
  int64_t current = 0;
  int64_t limit = 0;
  if (Status s = ReadInt64File(current_path_, &current); !s.ok()) {
    return s;
  }
  if (Status s = ReadInt64File(limit_path_, &limit); !s.ok()) {
    return s;
  }
  *current_bytes = current;
  *limit_bytes = limit;
  return Status::OK();
}

Status FileMemoryPressureReader::ReadInt64File(const std::string &path, int64_t *out) {
  std::ifstream in(path);
  if (!in.is_open()) {
    return Status::IOError("failed to open " + path + ": " + std::strerror(errno));
  }
  std::string line;
  if (!std::getline(in, line)) {
    return Status::IOError("empty content at " + path);
  }
  // Trim trailing whitespace / newline.
  while (!line.empty() && (line.back() == '\n' || line.back() == '\r' ||
                            line.back() == ' ' || line.back() == '\t')) {
    line.pop_back();
  }
  if (line == "max") {
    // v2 sentinel: uncapped → surface as INT64_MAX so ratio math still works.
    *out = INT64_MAX;
    return Status::OK();
  }
  try {
    size_t consumed = 0;
    long long v = std::stoll(line, &consumed);
    if (consumed == 0) {
      return Status::IOError("no digits at " + path + ": '" + line + "'");
    }
    if (v < 0) {
      return Status::IOError("negative value at " + path + ": " + line);
    }
    *out = static_cast<int64_t>(v);
    return Status::OK();
  } catch (const std::exception &e) {
    return Status::IOError("parse failure at " + path + ": " + e.what());
  }
}

}  // namespace ray
