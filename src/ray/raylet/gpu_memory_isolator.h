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

#include <sys/types.h>

#include <boost/asio/thread_pool.hpp>
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "ray/asio/instrumented_io_context.h"
#include "ray/common/cgroup2/cgroup_manager_interface.h"
#include "ray/common/id.h"
#include "ray/common/status.h"

namespace ray {
namespace raylet {

using CommandRunner = std::function<Status(const std::vector<std::string> &args)>;

/// Runs the command and returns OK only if it exits with status 0.
Status RunCommand(const std::vector<std::string> &args);

/// Caps the GPU memory of a worker with NVIDIA MPS memory partitioning: the worker gets
/// its own cgroup, and a privileged command sets the cgroup's limit on its GPU.
class GpuMemoryIsolator {
 public:
  GpuMemoryIsolator(instrumented_io_context &io_service,
                    CgroupManagerInterface &cgroup_manager,
                    std::vector<std::string> limit_command,
                    std::vector<std::string> gpu_ids,
                    CommandRunner run_command = RunCommand);

  ~GpuMemoryIsolator();

  /// Calls back on the io_service once the limit is set or has failed.
  void Isolate(const WorkerID &worker_id,
               const LeaseID &lease_id,
               pid_t pid,
               size_t gpu_index,
               int64_t gpu_memory_bytes,
               std::function<void(Status)> callback);

  void Release(const WorkerID &worker_id);

 private:
  struct Isolation {
    std::string name;
    std::string cgroup;
    std::string device;
  };

  std::vector<std::string> LimitArgs(const Isolation &isolation,
                                     const std::string &soft_limit,
                                     const std::string &hard_limit) const;

  void RunAsync(std::vector<std::string> args, std::function<void(Status)> callback);

  instrumented_io_context &io_service_;
  CgroupManagerInterface &cgroup_manager_;
  std::vector<std::string> limit_command_;
  std::vector<std::string> gpu_ids_;
  CommandRunner run_command_;
  absl::flat_hash_map<WorkerID, Isolation> isolations_;
  boost::asio::thread_pool command_pool_{1};
};

}  // namespace raylet
}  // namespace ray
