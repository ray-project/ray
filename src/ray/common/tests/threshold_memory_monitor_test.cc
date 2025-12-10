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

#include <sys/mman.h>
#include <sys/sysinfo.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <boost/filesystem.hpp>
#include <boost/thread/latch.hpp>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/common/memory_monitor.h"
#include "ray/util/process.h"

namespace {

/// Helper class to spawn a child process that consumes a specified fraction of system
/// memory. The memory is allocated using mmap with MAP_POPULATE to ensure pages are
/// actually committed.
class MemoryHog {
 public:
  /// Spawns a child process that allocates the specified fraction of total system memory.
  /// \param memory_fraction Fraction of total system memory to allocate
  explicit MemoryHog(float memory_fraction) : child_pid_(-1) {
    struct sysinfo info;
    if (sysinfo(&info) != 0) {
      return;
    }

    int64_t total_memory = static_cast<int64_t>(info.totalram) * info.mem_unit;
    allocation_size_ = static_cast<size_t>(total_memory * memory_fraction);

    // Create a pipe to signal when child has allocated memory
    int pipefd[2];
    if (pipe(pipefd) != 0) {
      return;
    }

    child_pid_ = fork();
    if (child_pid_ == 0) {
      // Child process: allocate memory and wait to be killed
      close(pipefd[0]);  // Close read end

      // Allocate memory using mmap with MAP_POPULATE to actually commit the pages
      void *mem = mmap(nullptr,
                       allocation_size_,
                       PROT_READ | PROT_WRITE,
                       MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE,
                       -1,
                       0);

      if (mem != MAP_FAILED) {
        // Touch every page to ensure memory is really allocated
        volatile char *ptr = static_cast<volatile char *>(mem);
        for (size_t i = 0; i < allocation_size_; i += 4096) {
          ptr[i] = 1;
        }

        // Signal parent that memory is allocated
        char ready = 1;
        if (write(pipefd[1], &ready, 1) < 0) {
          _exit(1);
        }
        close(pipefd[1]);

        // Sleep until killed
        while (true) {
          sleep(1);
        }
      }
      _exit(1);
    } else if (child_pid_ > 0) {
      // Parent process: wait for child to signal it's ready
      close(pipefd[1]);  // Close write end

      char ready = 0;
      // Wait for child to signal memory allocation is complete
      fd_set readfds;
      FD_ZERO(&readfds);
      FD_SET(pipefd[0], &readfds);
      struct timeval timeout;
      timeout.tv_sec = 30;  // 30 second timeout
      timeout.tv_usec = 0;

      if (select(pipefd[0] + 1, &readfds, nullptr, nullptr, &timeout) > 0) {
        if (read(pipefd[0], &ready, 1) < 0) {
          ready = 0;
        }
      }
      close(pipefd[0]);

      if (ready != 1) {
        // Child failed to allocate memory, clean up
        if (child_pid_ > 0) {
          kill(child_pid_, SIGKILL);
          waitpid(child_pid_, nullptr, 0);
        }
        child_pid_ = -1;
      }
    }
  }

  ~MemoryHog() {
    if (child_pid_ > 0) {
      kill(child_pid_, SIGKILL);
      waitpid(child_pid_, nullptr, 0);
    }
  }

  bool IsValid() const { return child_pid_ > 0; }

 private:
  pid_t child_pid_;
  size_t allocation_size_;
};

}  // namespace

namespace ray {

/// TODO(Kunchd): Get memory monitor test to work with new memory monitor implementation
class ThresholdMemoryMonitorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    thread_ = std::make_unique<std::thread>([this]() {
      boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work(
          io_context_.get_executor());
      io_context_.run();
    });
  }
  void TearDown() override {
    io_context_.stop();
    thread_->join();
    instance.reset();
  }
  std::unique_ptr<std::thread> thread_;
  instrumented_io_context io_context_;

  void MakeMemoryUsage(pid_t pid,
                       const std::string usage_kb,
                       const std::string proc_dir) {
    boost::filesystem::create_directory(proc_dir);
    boost::filesystem::create_directory(proc_dir + "/" + std::to_string(pid));

    std::string usage_filename = proc_dir + "/" + std::to_string(pid) + "/smaps_rollup";

    std::ofstream usage_file;
    usage_file.open(usage_filename);
    usage_file << "SomeHeader" << std::endl;
    usage_file << "Private_Clean: " << usage_kb << " kB" << std::endl;
    usage_file.close();
  }

  ThresholdMemoryMonitor &MakeThresholdMemoryMonitor(
      float usage_threshold,
      int64_t min_memory_free_bytes,
      uint64_t monitor_interval_ms,
      KillWorkersCallback kill_workers_callback) {
    instance = std::make_unique<ThresholdMemoryMonitor>(io_context_,
                                                        kill_workers_callback,
                                                        usage_threshold,
                                                        min_memory_free_bytes,
                                                        monitor_interval_ms);
    return *instance;
  }
  std::unique_ptr<ThresholdMemoryMonitor> instance;
};

TEST_F(ThresholdMemoryMonitorTest, TestThresholdZeroMonitorAlwaysAboveThreshold) {
  ASSERT_TRUE(ThresholdMemoryMonitor::IsUsageAboveThreshold({1, 10}, 0));
}

TEST_F(ThresholdMemoryMonitorTest, TestThresholdOneMonitorAlwaysBelowThreshold) {
  ASSERT_FALSE(ThresholdMemoryMonitor::IsUsageAboveThreshold({9, 10}, 10));
}

TEST_F(ThresholdMemoryMonitorTest, TestUsageAtThresholdReportsFalse) {
  ASSERT_FALSE(ThresholdMemoryMonitor::IsUsageAboveThreshold({4, 10}, 5));
  ASSERT_FALSE(ThresholdMemoryMonitor::IsUsageAboveThreshold({5, 10}, 5));
  ASSERT_TRUE(ThresholdMemoryMonitor::IsUsageAboveThreshold({6, 10}, 5));
}

TEST_F(ThresholdMemoryMonitorTest, TestGetNodeAvailableMemoryAlwaysPositive) {
  {
    auto &monitor = MakeThresholdMemoryMonitor(
        0 /*usage_threshold*/,
        -1 /*min_memory_free_bytes*/,
        0 /*refresh_interval_ms*/,
        [](MemorySnapshot system_memory) { FAIL() << "Expected monitor to not run"; });
    auto [used_bytes, total_bytes] = monitor.GetMemoryBytes();
    ASSERT_GT(total_bytes, 0);
    ASSERT_GT(total_bytes, used_bytes);
  }
}

TEST_F(ThresholdMemoryMonitorTest, TestGetNodeTotalMemoryEqualsFreeOrCGroup) {
  {
    auto &monitor = MakeThresholdMemoryMonitor(
        0 /*usage_threshold*/,
        -1 /*min_memory_free_bytes*/,
        0 /*refresh_interval_ms*/,
        [](MemorySnapshot system_memory) { FAIL() << "Expected monitor to not run"; });
    auto [used_bytes, total_bytes] = monitor.GetMemoryBytes();
    auto [cgroup_used_bytes, cgroup_total_bytes] = monitor.GetCGroupMemoryBytes();

    auto cmd_out = Process::Exec("free -b");
    std::string title;
    std::string total;
    std::string used;
    std::string free;
    std::string shared;
    std::string cache;
    std::string available;
    std::istringstream cmd_out_ss(cmd_out);
    cmd_out_ss >> total >> used >> free >> shared >> cache >> available;
    cmd_out_ss >> title >> total >> used >> free >> shared >> cache >> available;

    int64_t free_total_bytes;
    std::istringstream total_ss(total);
    total_ss >> free_total_bytes;

    ASSERT_TRUE(total_bytes == free_total_bytes || total_bytes == cgroup_total_bytes);
  }
}

TEST_F(ThresholdMemoryMonitorTest, TestMonitorTriggerCanDetectMemoryUsage) {
  std::shared_ptr<boost::latch> has_checked_once = std::make_shared<boost::latch>(1);

  MakeThresholdMemoryMonitor(0.0 /*usage_threshold*/,
                             -1 /*min_memory_free_bytes*/,
                             1 /*refresh_interval_ms*/,
                             [has_checked_once](MemorySnapshot system_memory) {
                               ASSERT_GT(system_memory.total_bytes, 0);
                               ASSERT_GT(system_memory.used_bytes, 0);
                               has_checked_once->count_down();
                             });
  has_checked_once->wait();
}

// Expected to take around 30s or less to complete
TEST_F(ThresholdMemoryMonitorTest, TestMonitorDetectsMemoryUsageAboveThreshold) {
  MemoryHog memory_hog(0.30);
  ASSERT_TRUE(memory_hog.IsValid()) << "Failed to spawn memory-consuming child process";
  std::shared_ptr<boost::latch> has_checked_once = std::make_shared<boost::latch>(1);

  MakeThresholdMemoryMonitor(0.2 /*usage_threshold*/,
                             -1 /*min_memory_free_bytes*/,
                             1 /*refresh_interval_ms*/,
                             [has_checked_once](MemorySnapshot system_memory) {
                               ASSERT_GT(system_memory.total_bytes, 0);
                               ASSERT_GT(system_memory.used_bytes, 0);
                               has_checked_once->count_down();
                             });

  has_checked_once->wait();
  // MemoryHog destructor will kill the child process
}

TEST_F(ThresholdMemoryMonitorTest, TestMonitorDoesNotTriggerWhenBelowThreshold) {
  std::atomic<bool> callback_triggered{false};

  MakeThresholdMemoryMonitor(0.4 /*usage_threshold*/,
                             -1 /*min_memory_free_bytes*/,
                             1 /*refresh_interval_ms*/,
                             [&callback_triggered](MemorySnapshot system_memory) {
                               callback_triggered.store(true);
                             });

  std::this_thread::sleep_for(std::chrono::seconds(20));

  ASSERT_FALSE(callback_triggered.load())
      << "Callback should not have been triggered when memory is below threshold";
}

TEST_F(ThresholdMemoryMonitorTest, TestCgroupFilesValidReturnsWorkingSet) {
  std::string stat_file_name = UniqueID::FromRandom().Hex();
  std::ofstream stat_file;
  stat_file.open(stat_file_name);
  stat_file << "random_key "
            << "random_value" << std::endl;
  stat_file << "inactive_file "
            << "123" << std::endl;
  stat_file << "active_file "
            << "88" << std::endl;
  stat_file << "another_random_key "
            << "some_value" << std::endl;
  stat_file.close();

  std::string curr_file_name = UniqueID::FromRandom().Hex();
  std::ofstream curr_file;
  curr_file.open(curr_file_name);
  curr_file << "300" << std::endl;
  curr_file.close();

  int64_t used_bytes = ThresholdMemoryMonitor::GetCGroupMemoryUsedBytes(
      stat_file_name.c_str(), curr_file_name.c_str(), "inactive_file", "active_file");

  std::remove(stat_file_name.c_str());
  std::remove(curr_file_name.c_str());

  ASSERT_EQ(used_bytes, 300 - 123 - 88);
}

TEST_F(ThresholdMemoryMonitorTest, TestCgroupFilesValidKeyLastReturnsWorkingSet) {
  std::string stat_file_name = UniqueID::FromRandom().Hex();
  std::ofstream stat_file;
  stat_file.open(stat_file_name);
  stat_file << "random_key "
            << "random_value" << std::endl;
  stat_file << "inactive_file "
            << "123" << std::endl;
  stat_file << "active_file "
            << "88" << std::endl;
  stat_file.close();

  std::string curr_file_name = UniqueID::FromRandom().Hex();
  std::ofstream curr_file;
  curr_file.open(curr_file_name);
  curr_file << "300" << std::endl;
  curr_file.close();

  int64_t used_bytes = ThresholdMemoryMonitor::GetCGroupMemoryUsedBytes(
      stat_file_name.c_str(), curr_file_name.c_str(), "inactive_file", "active_file");

  std::remove(stat_file_name.c_str());
  std::remove(curr_file_name.c_str());

  ASSERT_EQ(used_bytes, 300 - 123 - 88);
}

TEST_F(ThresholdMemoryMonitorTest, TestCgroupFilesValidNegativeWorkingSet) {
  std::string stat_file_name = UniqueID::FromRandom().Hex();
  std::ofstream stat_file;
  stat_file.open(stat_file_name);
  stat_file << "random_key "
            << "random_value" << std::endl;
  stat_file << "inactive_file "
            << "300" << std::endl;
  stat_file << "active_file "
            << "100" << std::endl;
  stat_file.close();

  std::string curr_file_name = UniqueID::FromRandom().Hex();
  std::ofstream curr_file;
  curr_file.open(curr_file_name);
  curr_file << "123" << std::endl;
  curr_file.close();

  int64_t used_bytes = ThresholdMemoryMonitor::GetCGroupMemoryUsedBytes(
      stat_file_name.c_str(), curr_file_name.c_str(), "inactive_file", "active_file");

  std::remove(stat_file_name.c_str());
  std::remove(curr_file_name.c_str());

  ASSERT_EQ(used_bytes, 123 - 300 - 100);
}

TEST_F(ThresholdMemoryMonitorTest, TestCgroupFilesValidMissingFieldReturnskNull) {
  std::string file_name = UniqueID::FromRandom().Hex();
  std::string stat_file_name = UniqueID::FromRandom().Hex();
  std::ofstream stat_file;
  stat_file.open(stat_file_name);
  stat_file << "random_key "
            << "random_value" << std::endl;
  stat_file << "another_random_key "
            << "123" << std::endl;
  stat_file.close();

  std::string curr_file_name = UniqueID::FromRandom().Hex();
  std::ofstream curr_file;
  curr_file.open(curr_file_name);
  curr_file << "300" << std::endl;
  curr_file.close();

  int64_t used_bytes = ThresholdMemoryMonitor::GetCGroupMemoryUsedBytes(
      stat_file_name.c_str(), curr_file_name.c_str(), "inactive_file", "active_file");

  std::remove(stat_file_name.c_str());
  std::remove(curr_file_name.c_str());

  ASSERT_EQ(used_bytes, ThresholdMemoryMonitor::kNull);
}

TEST_F(ThresholdMemoryMonitorTest, TestCgroupNonexistentStatFileReturnskNull) {
  std::string stat_file_name = UniqueID::FromRandom().Hex();

  std::string curr_file_name = UniqueID::FromRandom().Hex();
  std::ofstream curr_file;
  curr_file.open(curr_file_name);
  curr_file << "300" << std::endl;
  curr_file.close();

  int64_t used_bytes = ThresholdMemoryMonitor::GetCGroupMemoryUsedBytes(
      stat_file_name.c_str(), curr_file_name.c_str(), "inactive_file", "active_file");
  std::remove(curr_file_name.c_str());

  ASSERT_EQ(used_bytes, ThresholdMemoryMonitor::kNull);
}

TEST_F(ThresholdMemoryMonitorTest, TestCgroupNonexistentUsageFileReturnskNull) {
  std::string curr_file_name = UniqueID::FromRandom().Hex();

  std::string stat_file_name = UniqueID::FromRandom().Hex();
  std::ofstream stat_file;
  stat_file.open(stat_file_name);
  stat_file << "random_key "
            << "random_value" << std::endl;
  stat_file << "inactive_file "
            << "300" << std::endl;
  stat_file << "active_file "
            << "88" << std::endl;
  stat_file.close();

  int64_t used_bytes = ThresholdMemoryMonitor::GetCGroupMemoryUsedBytes(
      stat_file_name.c_str(), curr_file_name.c_str(), "inactive_file", "active_file");
  std::remove(stat_file_name.c_str());

  ASSERT_EQ(used_bytes, ThresholdMemoryMonitor::kNull);
}

TEST_F(ThresholdMemoryMonitorTest, TestGetMemoryThresholdTakeGreaterOfTheTwoValues) {
  ASSERT_EQ(ThresholdMemoryMonitor::GetMemoryThreshold(100, 0.5, 0), 100);
  ASSERT_EQ(ThresholdMemoryMonitor::GetMemoryThreshold(100, 0.5, 60), 50);

  ASSERT_EQ(ThresholdMemoryMonitor::GetMemoryThreshold(100, 1, 10), 100);
  ASSERT_EQ(ThresholdMemoryMonitor::GetMemoryThreshold(100, 1, 100), 100);

  ASSERT_EQ(ThresholdMemoryMonitor::GetMemoryThreshold(100, 0.1, 100), 10);
  ASSERT_EQ(ThresholdMemoryMonitor::GetMemoryThreshold(100, 0, 10), 90);
  ASSERT_EQ(ThresholdMemoryMonitor::GetMemoryThreshold(100, 0, 100), 0);

  ASSERT_EQ(
      ThresholdMemoryMonitor::GetMemoryThreshold(100, 0, ThresholdMemoryMonitor::kNull),
      0);
  ASSERT_EQ(
      ThresholdMemoryMonitor::GetMemoryThreshold(100, 0.5, ThresholdMemoryMonitor::kNull),
      50);
  ASSERT_EQ(
      ThresholdMemoryMonitor::GetMemoryThreshold(100, 1, ThresholdMemoryMonitor::kNull),
      100);
}

TEST_F(ThresholdMemoryMonitorTest, TestGetPidsFromDirOnlyReturnsNumericFilenames) {
  std::string proc_dir = UniqueID::FromRandom().Hex();
  boost::filesystem::create_directory(proc_dir);

  std::string num_filename = proc_dir + "/123";
  std::string non_num_filename = proc_dir + "/123b";

  std::ofstream num_file;
  num_file.open(num_filename);
  num_file << num_filename;
  num_file.close();

  std::ofstream non_num_file;
  non_num_file.open(non_num_filename);
  non_num_file << non_num_filename;
  non_num_file.close();

  auto pids = ThresholdMemoryMonitor::GetPidsFromDir(proc_dir);

  boost::filesystem::remove_all(proc_dir);

  ASSERT_EQ(pids.size(), 1);
  ASSERT_EQ(pids[0], 123);
}

TEST_F(ThresholdMemoryMonitorTest, TestGetPidsFromNonExistentDirReturnsEmpty) {
  std::string proc_dir = UniqueID::FromRandom().Hex();
  auto pids = ThresholdMemoryMonitor::GetPidsFromDir(proc_dir);
  ASSERT_EQ(pids.size(), 0);
}

TEST_F(ThresholdMemoryMonitorTest, TestGetCommandLinePidExistReturnsValid) {
  std::string proc_dir = UniqueID::FromRandom().Hex();
  std::string pid_dir = proc_dir + "/123";
  boost::filesystem::create_directories(pid_dir);

  std::string cmdline_filename = pid_dir + "/" + ThresholdMemoryMonitor::kCommandlinePath;

  std::ofstream cmdline_file;
  cmdline_file.open(cmdline_filename);
  cmdline_file << "/my/very/custom/command --test passes!     ";
  cmdline_file.close();

  std::string commandline = ThresholdMemoryMonitor::GetCommandLineForPid(123, proc_dir);

  boost::filesystem::remove_all(proc_dir);

  ASSERT_EQ(commandline, "/my/very/custom/command --test passes!");
}

TEST_F(ThresholdMemoryMonitorTest, TestGetCommandLineMissingFileReturnsEmpty) {
  {
    std::string proc_dir = UniqueID::FromRandom().Hex();
    std::string commandline = ThresholdMemoryMonitor::GetCommandLineForPid(123, proc_dir);
    boost::filesystem::remove_all(proc_dir);
    ASSERT_EQ(commandline, "");
  }

  {
    std::string proc_dir = UniqueID::FromRandom().Hex();
    boost::filesystem::create_directory(proc_dir);
    std::string commandline = ThresholdMemoryMonitor::GetCommandLineForPid(123, proc_dir);
    boost::filesystem::remove_all(proc_dir);
    ASSERT_EQ(commandline, "");
  }

  {
    std::string proc_dir = UniqueID::FromRandom().Hex();
    std::string pid_dir = proc_dir + "/123";
    boost::filesystem::create_directories(pid_dir);
    std::string commandline = ThresholdMemoryMonitor::GetCommandLineForPid(123, proc_dir);
    boost::filesystem::remove_all(proc_dir);
    ASSERT_EQ(commandline, "");
  }
}

TEST_F(ThresholdMemoryMonitorTest, TestShortStringNotTruncated) {
  std::string out = ThresholdMemoryMonitor::TruncateString("im short", 20);
  ASSERT_EQ(out, "im short");
}

TEST_F(ThresholdMemoryMonitorTest, TestLongStringTruncated) {
  std::string out = ThresholdMemoryMonitor::TruncateString(std::string(7, 'k'), 5);
  ASSERT_EQ(out, "kkkkk...");
}

TEST_F(ThresholdMemoryMonitorTest, TestTopNLessThanNReturnsMemoryUsedDesc) {
  absl::flat_hash_map<pid_t, int64_t> usage;
  usage.insert({1, 111});
  usage.insert({2, 222});
  usage.insert({3, 333});

  auto list = ThresholdMemoryMonitor::GetTopNMemoryUsage(2, usage);

  ASSERT_EQ(list.size(), 2);
  ASSERT_EQ(std::get<0>(list[0]), 3);
  ASSERT_EQ(std::get<1>(list[0]), 333);
  ASSERT_EQ(std::get<0>(list[1]), 2);
  ASSERT_EQ(std::get<1>(list[1]), 222);
}

TEST_F(ThresholdMemoryMonitorTest, TestTopNMoreThanNReturnsAllDesc) {
  absl::flat_hash_map<pid_t, int64_t> usage;
  usage.insert({1, 111});
  usage.insert({2, 222});

  auto list = ThresholdMemoryMonitor::GetTopNMemoryUsage(3, usage);

  ASSERT_EQ(list.size(), 2);
  ASSERT_EQ(std::get<0>(list[0]), 2);
  ASSERT_EQ(std::get<1>(list[0]), 222);
  ASSERT_EQ(std::get<0>(list[1]), 1);
  ASSERT_EQ(std::get<1>(list[1]), 111);
}

TEST_F(ThresholdMemoryMonitorTest, TestGetProcessMemoryUsageFiltersBadPids) {
  std::string proc_dir = UniqueID::FromRandom().Hex();
  MakeMemoryUsage(1, "111", proc_dir);

  // Invalid pids with no memory usage file.
  boost::filesystem::create_directory(proc_dir + "/2");
  boost::filesystem::create_directory(proc_dir + "/3");

  auto usage = ThresholdMemoryMonitor::GetProcessMemoryUsage(proc_dir);

  ASSERT_EQ(usage.size(), 1);
  ASSERT_TRUE(usage.contains(1));
}

}  // namespace ray
