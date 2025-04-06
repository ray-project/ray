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

// This benchmark measures the latency for (1) create an application cgroup; (2) add a
// process into the cgroup; (3) apply resource constraint; (4) remove application cgroup.
// The operations are used to mimic per-task resource constraint.
//
// Benchmark pre-requisite:
// 1. Benchmark should be executed under entity with sufficient permission to write
// cgroupv2.
// 2. cgroupv2 is properly mounted at `/sys/fs/cgroup` in read-write mode, with cpu and
// memory enabled for subtree controller.

#include <benchmark/benchmark.h>
#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <thread>

#include "ray/common/cgroup/cgroup_utils.h"
#include "ray/common/cgroup/constants.h"
#include "ray/util/logging.h"

void AppCgroupSetupAndDeletion(benchmark::State &state) {
  for (auto _ : state) {
    // Benchmark step-1: create cgroup.
    RAY_CHECK_EQ(mkdir("/sys/fs/cgroup/ray_cgroup_benchmark", ray::kReadWritePerm), 0)
        << "Failed to create cgroup because " << strerror(errno);

    state.PauseTiming();
    pid_t pid = fork();
    RAY_CHECK_NE(pid, -1) << "Failed to create subprocess because " << strerror(errno);

    // Child process.
    if (pid == 0) {
      // Spawn a process running long enough, so it could be added into cgroup.
      // It won't affect runtime, because it will be killed later.
      std::this_thread::sleep_for(std::chrono::seconds(3600));
      // Exit without flushing the buffer.
      std::_Exit(0);
    }

    state.ResumeTiming();

    // Parent process.
    // Benchmark step-2: place process into cgroup.
    {
      std::ofstream out_file("/sys/fs/cgroup/ray_cgroup_benchmark/cgroup.procs",
                             std::ios::app | std::ios::out);
      out_file << pid;
      out_file.flush();
      RAY_CHECK(out_file.good()) << "Failed to write pid into cgroup.";
    }

    // Benchmark step-3: apply memory and cpu constraint to cgroup.
    {
      std::ofstream out_file("/sys/fs/cgroup/ray_cgroup_benchmark/memory.max");
      out_file << "1024";  // 1KiB
      out_file.flush();
      RAY_CHECK(out_file.good()) << "Failed to apply max memory consumption to cgroup.";
    }

    {
      std::ofstream out_file("/sys/fs/cgroup/ray_cgroup_benchmark/cpu.max");
      out_file << "10000";
      out_file.flush();
      RAY_CHECK(out_file.good()) << "Failed to apply cpu allocation to cgroup.";
    }

    state.PauseTiming();
    RAY_CHECK_OK(ray::KillAllProcAndWait("/sys/fs/cgroup/ray_cgroup_benchmark"));
    state.ResumeTiming();

    // Benchmark step-4: delete cgroup.
    // Precondition: there're no processes inside.
    RAY_CHECK_EQ(rmdir("/sys/fs/cgroup/ray_cgroup_benchmark"), 0)
        << "Failed to delete cgroup because " << strerror(errno);
  }
}
// Register the function as a benchmark
BENCHMARK(AppCgroupSetupAndDeletion)->Iterations(2000)->Unit(benchmark::kMicrosecond);

int main(int argc, char **argv) {
  ::benchmark::Initialize(&argc, argv);
  ::benchmark::RunSpecifiedBenchmarks();
  return 0;
}
