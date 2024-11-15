#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

#include "absl/strings/str_format.h"

const int TIMES = 100000;

class PhysicalModeExecutionContext {
 public:
  std::string memory_cgroup_uuid = "";
  long long high_memory = 0;
  long long max_memory = 0;
  long long low_memory = 0;
  long long min_memory = 0;
};

bool setup_cgroup_for_memory(PhysicalModeExecutionContext &physical_exec_ctx) {
  if (physical_exec_ctx.memory_cgroup_uuid.empty()) {
    return true;
  }

  std::string cgroup_path = "/sys/fs/cgroup/" + physical_exec_ctx.memory_cgroup_uuid;

  try {
    std::filesystem::create_directories(cgroup_path);
  } catch (const std::exception &e) {
    std::cerr << "Error creating cgroup directory: " << e.what() << std::endl;
    return false;
  }

  pid_t pid = getpid();

  std::ofstream cgroup_procs(cgroup_path + "/cgroup.procs");
  if (cgroup_procs.is_open()) {
    cgroup_procs << pid;
  } else {
    std::cerr << "Failed to open cgroup.procs" << std::endl;
    return false;
  }

  // if (physical_exec_ctx.low_memory > 0) {
  //     std::ofstream low_memory(cgroup_path + "/memory.low");
  //     if (low_memory.is_open()) {
  //         low_memory << physical_exec_ctx.low_memory;
  //     }
  // }

  // if (physical_exec_ctx.min_memory > 0) {
  //     std::ofstream min_memory(cgroup_path + "/memory.min");
  //     if (min_memory.is_open()) {
  //         min_memory << physical_exec_ctx.min_memory;
  //     }
  // }

  // if (physical_exec_ctx.high_memory > 0) {
  //     std::ofstream high_memory(cgroup_path + "/memory.high");
  //     if (high_memory.is_open()) {
  //         high_memory << physical_exec_ctx.high_memory;
  //     }
  // }

  if (physical_exec_ctx.max_memory > 0) {
    std::ofstream max_memory(cgroup_path + "/memory.max");
    if (max_memory.is_open()) {
      max_memory << physical_exec_ctx.max_memory;
    }
  }

  return true;
}

void test_runtime() {
  auto current_timestamp = std::chrono::system_clock::now();
  auto current_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                          current_timestamp.time_since_epoch())
                          .count();
  std::cout << "Start timestamp: " << current_time << std::endl;

  for (int i = 0; i < TIMES; ++i) {
    std::string unique_id = absl::StrFormat("uuid-hjiang-%d", i);
    PhysicalModeExecutionContext ctx;
    ctx.memory_cgroup_uuid = unique_id;
    ctx.high_memory = 1024 * 1024;  // 1MB for example
    setup_cgroup_for_memory(ctx);
  }

  current_timestamp = std::chrono::system_clock::now();
  current_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                     current_timestamp.time_since_epoch())
                     .count();
  std::cout << "End timestamp: " << current_time << std::endl;
}

int main() {
  test_runtime();
  return 0;
}
