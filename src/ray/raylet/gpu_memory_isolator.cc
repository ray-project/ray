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

#include "ray/raylet/gpu_memory_isolator.h"

#ifndef _WIN32
#include <unistd.h>
#endif

#include <boost/asio/post.hpp>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "ray/util/logging.h"
#include "ray/util/process.h"

namespace ray {
namespace raylet {

Status RunCommand(const std::vector<std::string> &args) {
#ifdef _WIN32
  return Status::NotImplemented("Running commands is not supported on Windows.");
#else
  std::string output_path =
      (std::filesystem::temp_directory_path() / "ray_command_XXXXXX").string();
  int fd = mkstemp(output_path.data());
  if (fd == -1) {
    return Status::IOError(
        absl::StrFormat("Failed to create %s: %s", output_path, strerror(errno)));
  }
  close(fd);

  // The raylet ignores SIGCHLD, so waitpid can't report the exit code. The shell child
  // has SIGCHLD restored by Process, so it waits for the command and records the code.
  std::vector<std::string> shell_args = {
      "/bin/sh", "-c", "\"$@\" >\"$0\" 2>&1; echo $? >>\"$0\"", output_path};
  shell_args.insert(shell_args.end(), args.begin(), args.end());
  std::vector<const char *> argv;
  for (const std::string &arg : shell_args) {
    argv.push_back(arg.c_str());
  }
  argv.push_back(nullptr);

  std::error_code ec;
  Process process(argv.data(), ec, /*decouple=*/true);
  if (!ec) {
    process.Wait();
  }

  std::vector<std::string> lines;
  std::ifstream output(output_path);
  for (std::string line; std::getline(output, line);) {
    lines.push_back(std::move(line));
  }
  std::error_code remove_ec;
  std::filesystem::remove(output_path, remove_ec);

  std::string command = absl::StrJoin(args, " ");
  if (ec) {
    return Status::IOError(
        absl::StrFormat("Failed to start `%s`: %s", command, ec.message()));
  }
  if (lines.empty()) {
    return Status::IOError(absl::StrFormat("`%s` did not report an exit code.", command));
  }
  std::string exit_code = std::move(lines.back());
  lines.pop_back();
  if (exit_code != "0") {
    return Status::IOError(absl::StrFormat(
        "`%s` exited with code %s: %s", command, exit_code, absl::StrJoin(lines, "\n")));
  }
  return Status::OK();
#endif
}

GpuMemoryIsolator::GpuMemoryIsolator(instrumented_io_context &io_service,
                                     CgroupManagerInterface &cgroup_manager,
                                     std::vector<std::string> limit_command,
                                     std::vector<std::string> gpu_ids,
                                     CommandRunner run_command)
    : io_service_(io_service),
      cgroup_manager_(cgroup_manager),
      limit_command_(std::move(limit_command)),
      gpu_ids_(std::move(gpu_ids)),
      run_command_(std::move(run_command)) {}

GpuMemoryIsolator::~GpuMemoryIsolator() { command_pool_.join(); }

void GpuMemoryIsolator::Isolate(const WorkerID &worker_id,
                                const LeaseID &lease_id,
                                pid_t pid,
                                size_t gpu_index,
                                int64_t gpu_memory_bytes,
                                std::function<void(Status)> callback) {
  Isolation isolation{
      lease_id.Hex(),
      "",
      gpu_index < gpu_ids_.size() ? gpu_ids_[gpu_index] : std::to_string(gpu_index)};
  StatusOr<std::string> cgroup =
      cgroup_manager_.AddProcessToGpuWorkerCgroup(isolation.name, std::to_string(pid));
  if (!cgroup.ok()) {
    Status deleted = cgroup_manager_.DeleteGpuWorkerCgroup(isolation.name);
    if (!deleted.ok() && !deleted.IsNotFound()) {
      RAY_LOG(WARNING) << "Failed to delete cgroup " << isolation.name << ": " << deleted;
    }
    io_service_.post([callback = std::move(callback),
                      status = cgroup.status()]() { callback(status); },
                     "GpuMemoryIsolator.Isolate");
    return;
  }
  isolation.cgroup = std::move(*cgroup);

  constexpr int64_t kMiB = 1024 * 1024;
  std::string limit_mib = std::to_string((gpu_memory_bytes + kMiB - 1) / kMiB);
  std::vector<std::string> args = LimitArgs(isolation, limit_mib, limit_mib);
  isolations_.insert_or_assign(worker_id, std::move(isolation));
  RunAsync(std::move(args), std::move(callback));
}

void GpuMemoryIsolator::Release(const WorkerID &worker_id) {
  auto it = isolations_.find(worker_id);
  if (it == isolations_.end()) {
    return;
  }
  Isolation isolation = std::move(it->second);
  isolations_.erase(it);
  // Zeroing the soft limit hands back the reservation the driver added to the ancestors.
  std::vector<std::string> args = LimitArgs(isolation, "0", "max");
  RunAsync(std::move(args), [this, isolation = std::move(isolation)](Status status) {
    if (!status.ok()) {
      RAY_LOG(WARNING) << "Failed to reset the GPU memory limit of cgroup "
                       << isolation.cgroup << ": " << status;
    }
    status = cgroup_manager_.DeleteGpuWorkerCgroup(isolation.name);
    if (!status.ok()) {
      RAY_LOG(WARNING) << "Failed to delete cgroup " << isolation.cgroup << ": "
                       << status;
    }
  });
}

std::vector<std::string> GpuMemoryIsolator::LimitArgs(
    const Isolation &isolation,
    const std::string &soft_limit,
    const std::string &hard_limit) const {
  std::vector<std::string> args = limit_command_;
  args.insert(args.end(),
              {"memory-limits",
               "--set",
               "--namespace",
               isolation.cgroup,
               "--soft-limit",
               soft_limit,
               "--hard-limit",
               hard_limit,
               "-i",
               isolation.device});
  return args;
}

void GpuMemoryIsolator::RunAsync(std::vector<std::string> args,
                                 std::function<void(Status)> callback) {
  boost::asio::post(
      command_pool_,
      [this, args = std::move(args), callback = std::move(callback)]() mutable {
        Status status = run_command_(args);
        io_service_.post([callback = std::move(callback), status]() { callback(status); },
                         "GpuMemoryIsolator.RunCommand");
      });
}

}  // namespace raylet
}  // namespace ray
