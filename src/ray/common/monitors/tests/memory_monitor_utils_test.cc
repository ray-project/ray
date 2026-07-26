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

#include "ray/common/monitors/memory_monitor_utils.h"

#include <boost/filesystem.hpp>
#include <fstream>
#include <memory>
#include <string>
#include <unordered_map>

#include "gtest/gtest.h"
#include "ray/common/cgroup2/cgroup_manager.h"
#include "ray/common/cgroup2/cgroup_test_utils.h"
#include "ray/common/cgroup2/fake_cgroup_driver.h"
#include "ray/common/cgroup2/noop_cgroup_manager.h"
#include "ray/common/id.h"
#include "ray/common/monitors/memory_monitor_test_fixture.h"
#include "ray/common/ray_config.h"
#include "ray/common/status.h"
#include "ray/util/process.h"

namespace ray {

class MemoryMonitorUtilsTest : public MemoryMonitorTestFixture {};

TEST_F(MemoryMonitorUtilsTest, TestGetNodeAvailableMemoryAlwaysPositive) {
  {
    MemoryUsageSnapshot system_memory =
        MemoryMonitorUtils::TakeSystemMemoryUsageSnapshot("");
    ASSERT_GT(system_memory.total_bytes, 0);
    ASSERT_GT(system_memory.total_bytes, system_memory.used_bytes);
  }
}

TEST_F(MemoryMonitorUtilsTest,
       TestTakeSystemMemoryUsageSnapshotUsesCgroupWhenLowerThanSystem) {
  int64_t cgroup_total_bytes = 1024 * 1024 * 1024;   // 1 GB
  int64_t cgroup_current_bytes = 500 * 1024 * 1024;  // 500 MB current usage
  int64_t inactive_file_bytes = 50 * 1024 * 1024;    // 50 MB inactive file cache
  int64_t active_file_bytes = 30 * 1024 * 1024;      // 30 MB active file cache
  int64_t expected_used_bytes =
      cgroup_current_bytes - inactive_file_bytes - active_file_bytes;

  std::string cgroup_dir = MockCgroupv2MemoryUsage(cgroup_total_bytes,
                                                   cgroup_current_bytes,
                                                   /*anon_memory_bytes=*/0,
                                                   /*shmem_memory_bytes=*/0,
                                                   inactive_file_bytes,
                                                   active_file_bytes);

  MemoryUsageSnapshot system_memory =
      MemoryMonitorUtils::TakeSystemMemoryUsageSnapshot(cgroup_dir);

  ASSERT_EQ(system_memory.total_bytes, cgroup_total_bytes);
  ASSERT_EQ(system_memory.used_bytes, expected_used_bytes);
}

// Pins count_swap_in_memory_monitor for the lifetime of the test. RayConfig is
// process-global so each swap-related test sets the value explicitly to avoid
// cross-test contamination.
namespace {
void SetCountSwapFlag(bool enabled) {
  RayConfig::instance().initialize(std::string(R"({"count_swap_in_memory_monitor": )") +
                                   (enabled ? "true" : "false") + "}");
}
}  // namespace

TEST_F(MemoryMonitorUtilsTest, TestGetNodeTotalMemoryEqualsFreeOrCGroup) {
  SetCountSwapFlag(false);
  {
    MemoryUsageSnapshot system_memory =
        MemoryMonitorUtils::TakeSystemMemoryUsageSnapshot("");
    MemoryMonitorUtils::CgroupMemoryBytes cgroup_memory =
        MemoryMonitorUtils::GetCGroupMemoryBytes("");

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

    ASSERT_TRUE(system_memory.total_bytes == free_total_bytes ||
                system_memory.total_bytes == cgroup_memory.total_bytes);
  }
}

TEST_F(MemoryMonitorUtilsTest, TestLinuxMemoryFoldsSwapIntoTotal) {
  SetCountSwapFlag(true);
  int64_t mem_total_kb = 16 * 1024 * 1024;     // 16 GiB
  int64_t mem_available_kb = 4 * 1024 * 1024;  // 4 GiB free
  int64_t swap_total_kb = 8 * 1024 * 1024;     // 8 GiB swap
  int64_t swap_free_kb = 6 * 1024 * 1024;      // 2 GiB swap used
  std::string proc_dir =
      MockProcMeminfo(mem_total_kb, mem_available_kb, swap_total_kb, swap_free_kb);

  MemoryUsageSnapshot system_memory = MemoryMonitorUtils::TakeSystemMemoryUsageSnapshot(
      "", /*include_swap=*/true, proc_dir);

  int64_t expected_total = (mem_total_kb + swap_total_kb) * 1024;
  int64_t expected_used =
      ((mem_total_kb - mem_available_kb) + (swap_total_kb - swap_free_kb)) * 1024;
  ASSERT_EQ(system_memory.total_bytes, expected_total);
  ASSERT_EQ(system_memory.used_bytes, expected_used);
}

TEST_F(MemoryMonitorUtilsTest, TestLinuxSwapIgnoredWhenFlagDisabled) {
  // The default-off path must ignore swap even when it is present in meminfo,
  // otherwise we'd silently change the OOM killer's behavior on every existing
  // deployment.
  SetCountSwapFlag(false);
  int64_t mem_total_kb = 16 * 1024 * 1024;
  int64_t mem_available_kb = 4 * 1024 * 1024;
  int64_t swap_total_kb = 8 * 1024 * 1024;
  int64_t swap_free_kb = 6 * 1024 * 1024;
  std::string proc_dir =
      MockProcMeminfo(mem_total_kb, mem_available_kb, swap_total_kb, swap_free_kb);

  MemoryUsageSnapshot system_memory = MemoryMonitorUtils::TakeSystemMemoryUsageSnapshot(
      "", /*include_swap=*/true, proc_dir);

  ASSERT_EQ(system_memory.total_bytes, mem_total_kb * 1024);
  ASSERT_EQ(system_memory.used_bytes, (mem_total_kb - mem_available_kb) * 1024);
}

TEST_F(MemoryMonitorUtilsTest, TestLinuxMemoryWithoutSwapMatchesRamOnly) {
  SetCountSwapFlag(true);
  int64_t mem_total_kb = 8 * 1024 * 1024;
  int64_t mem_available_kb = 2 * 1024 * 1024;
  std::string proc_dir =
      MockProcMeminfo(mem_total_kb, mem_available_kb, std::nullopt, std::nullopt);

  MemoryUsageSnapshot system_memory = MemoryMonitorUtils::TakeSystemMemoryUsageSnapshot(
      "", /*include_swap=*/true, proc_dir);

  ASSERT_EQ(system_memory.total_bytes, mem_total_kb * 1024);
  ASSERT_EQ(system_memory.used_bytes, (mem_total_kb - mem_available_kb) * 1024);
}

TEST_F(MemoryMonitorUtilsTest, TestCgroupV2SwapAddedToTotalAndUsed) {
  SetCountSwapFlag(true);
  int64_t cgroup_total_bytes = 4LL * 1024 * 1024 * 1024;    // 4 GiB RAM limit
  int64_t cgroup_current_bytes = 2LL * 1024 * 1024 * 1024;  // 2 GiB RAM used
  int64_t swap_max_bytes = 2LL * 1024 * 1024 * 1024;        // 2 GiB swap limit
  int64_t swap_current_bytes = 512LL * 1024 * 1024;         // 512 MiB swap used

  std::string cgroup_dir = MockCgroupv2MemoryUsage(cgroup_total_bytes,
                                                   cgroup_current_bytes,
                                                   /*anon_memory_bytes=*/0,
                                                   /*shmem_memory_bytes=*/0,
                                                   /*inactive_file_bytes=*/0,
                                                   /*active_file_bytes=*/0);
  MockCgroupv2Swap(cgroup_dir, swap_max_bytes, swap_current_bytes);

  MemoryMonitorUtils::CgroupMemoryBytes cgroup_memory =
      MemoryMonitorUtils::GetCGroupMemoryBytes(cgroup_dir, /*include_swap=*/true);

  // v2 keeps RAM and swap separate.
  ASSERT_TRUE(cgroup_memory.has_swap);
  ASSERT_EQ(cgroup_memory.total_bytes, cgroup_total_bytes);
  ASSERT_EQ(cgroup_memory.used_bytes, cgroup_current_bytes);
  ASSERT_EQ(cgroup_memory.swap_total_bytes, swap_max_bytes);
  ASSERT_EQ(cgroup_memory.swap_used_bytes, swap_current_bytes);
}

TEST_F(MemoryMonitorUtilsTest, TestCgroupV2SwapIgnoredWhenFlagDisabled) {
  SetCountSwapFlag(false);
  int64_t cgroup_total_bytes = 4LL * 1024 * 1024 * 1024;
  int64_t cgroup_current_bytes = 2LL * 1024 * 1024 * 1024;
  int64_t swap_max_bytes = 2LL * 1024 * 1024 * 1024;
  int64_t swap_current_bytes = 512LL * 1024 * 1024;

  std::string cgroup_dir = MockCgroupv2MemoryUsage(cgroup_total_bytes,
                                                   cgroup_current_bytes,
                                                   /*anon_memory_bytes=*/0,
                                                   /*shmem_memory_bytes=*/0,
                                                   /*inactive_file_bytes=*/0,
                                                   /*active_file_bytes=*/0);
  MockCgroupv2Swap(cgroup_dir, swap_max_bytes, swap_current_bytes);

  MemoryMonitorUtils::CgroupMemoryBytes cgroup_memory =
      MemoryMonitorUtils::GetCGroupMemoryBytes(cgroup_dir, /*include_swap=*/false);

  ASSERT_FALSE(cgroup_memory.has_swap);
  ASSERT_EQ(cgroup_memory.total_bytes, cgroup_total_bytes);
  ASSERT_EQ(cgroup_memory.used_bytes, cgroup_current_bytes);
  ASSERT_EQ(cgroup_memory.swap_total_bytes, 0);
}

TEST_F(MemoryMonitorUtilsTest, TestCgroupV2SwapcachedSubtractedFromSwapUsed) {
  // Swapcache pages are charged to BOTH memory.current and memory.swap.current
  // (https://docs.kernel.org/admin-guide/cgroup-v2.html#memory-interface-files),
  // so summing used + swap.current would double count them. The swap side must
  // report swap.current net of memory.stat's swapcached — the resident copies
  // are already counted on the RAM side, whichever RAM figure the caller
  // composes with (cgroup memory.current or host meminfo).
  SetCountSwapFlag(true);
  int64_t cgroup_total_bytes = 4LL * 1024 * 1024 * 1024;    // 4 GiB RAM limit
  int64_t cgroup_current_bytes = 2LL * 1024 * 1024 * 1024;  // 2 GiB RAM used
  int64_t swap_max_bytes = 2LL * 1024 * 1024 * 1024;        // 2 GiB swap limit
  int64_t swap_current_bytes = 512LL * 1024 * 1024;         // 512 MiB swap used
  int64_t swapcached_bytes = 128LL * 1024 * 1024;           // 128 MiB in swapcache

  std::string cgroup_dir = MockCgroupv2MemoryUsage(cgroup_total_bytes,
                                                   cgroup_current_bytes,
                                                   /*anon_memory_bytes=*/0,
                                                   /*shmem_memory_bytes=*/0,
                                                   /*inactive_file_bytes=*/0,
                                                   /*active_file_bytes=*/0,
                                                   swapcached_bytes);
  MockCgroupv2Swap(cgroup_dir, swap_max_bytes, swap_current_bytes);

  MemoryMonitorUtils::CgroupMemoryBytes cgroup_memory =
      MemoryMonitorUtils::GetCGroupMemoryBytes(cgroup_dir, /*include_swap=*/true);

  ASSERT_TRUE(cgroup_memory.has_swap);
  // The RAM side keeps the resident swapcache copies...
  ASSERT_EQ(cgroup_memory.used_bytes, cgroup_current_bytes);
  // ...so the swap side must not count them again.
  ASSERT_EQ(cgroup_memory.swap_used_bytes, swap_current_bytes - swapcached_bytes);
}

TEST_F(MemoryMonitorUtilsTest, TestCgroupV2SwapcachedIgnoredWhenFlagDisabled) {
  // Without swap accounting the swap counters are never read, so swapcached in
  // memory.stat must not affect the RAM-only view.
  SetCountSwapFlag(false);
  int64_t cgroup_total_bytes = 4LL * 1024 * 1024 * 1024;
  int64_t cgroup_current_bytes = 2LL * 1024 * 1024 * 1024;
  int64_t swapcached_bytes = 128LL * 1024 * 1024;

  std::string cgroup_dir = MockCgroupv2MemoryUsage(cgroup_total_bytes,
                                                   cgroup_current_bytes,
                                                   /*anon_memory_bytes=*/0,
                                                   /*shmem_memory_bytes=*/0,
                                                   /*inactive_file_bytes=*/0,
                                                   /*active_file_bytes=*/0,
                                                   swapcached_bytes);

  MemoryMonitorUtils::CgroupMemoryBytes cgroup_memory =
      MemoryMonitorUtils::GetCGroupMemoryBytes(cgroup_dir, /*include_swap=*/false);

  ASSERT_EQ(cgroup_memory.used_bytes, cgroup_current_bytes);
  ASSERT_EQ(cgroup_memory.swap_used_bytes, 0);
}

TEST_F(MemoryMonitorUtilsTest, TestCgroupV2UnlimitedSwapFallsBackToHostSwap) {
  // swap.max == "max" means the cgroup imposes no swap cap, so the practical
  // limit is whatever the host has. Mirrors the Python helper in
  // ray._common.utils.get_cgroup_aware_swap_memory. Per-cgroup swap usage
  // comes from memory.swap.current, NOT host SwapTotal-SwapFree — otherwise
  // other workloads' swap consumption would inflate Ray's view and trigger
  // the OOM monitor early.
  SetCountSwapFlag(true);
  int64_t cgroup_total_bytes = 4LL * 1024 * 1024 * 1024;    // 4 GiB
  int64_t cgroup_current_bytes = 2LL * 1024 * 1024 * 1024;  // 2 GiB
  int64_t cgroup_swap_current_bytes = 512LL * 1024 * 1024;  // 512 MiB
  int64_t mem_total_kb = 16 * 1024 * 1024;                  // unused but required
  int64_t mem_available_kb = 4 * 1024 * 1024;               // unused but required
  int64_t swap_total_kb = 8 * 1024 * 1024;                  // 8 GiB host swap
  int64_t swap_free_kb = 6 * 1024 * 1024;                   // 2 GiB host-wide used

  std::string cgroup_dir = MockCgroupv2MemoryUsage(cgroup_total_bytes,
                                                   cgroup_current_bytes,
                                                   /*anon_memory_bytes=*/0,
                                                   /*shmem_memory_bytes=*/0,
                                                   /*inactive_file_bytes=*/0,
                                                   /*active_file_bytes=*/0);
  MockCgroupv2Swap(cgroup_dir,
                   /*swap_max_bytes=*/std::nullopt,  // writes "max"
                   cgroup_swap_current_bytes);
  std::string proc_dir =
      MockProcMeminfo(mem_total_kb, mem_available_kb, swap_total_kb, swap_free_kb);

  MemoryMonitorUtils::CgroupMemoryBytes cgroup_memory =
      MemoryMonitorUtils::GetCGroupMemoryBytes(cgroup_dir,
                                               /*include_swap=*/true,
                                               proc_dir);

  ASSERT_TRUE(cgroup_memory.has_swap);
  ASSERT_EQ(cgroup_memory.total_bytes, cgroup_total_bytes);
  // Unlimited swap.max -> host swap total is the cap.
  ASSERT_EQ(cgroup_memory.swap_total_bytes, swap_total_kb * 1024);
  ASSERT_EQ(cgroup_memory.used_bytes, cgroup_current_bytes);
  // Swap used must reflect the cgroup's own swap.current (512 MiB), not the
  // host-wide 2 GiB that includes other workloads.
  ASSERT_EQ(cgroup_memory.swap_used_bytes, cgroup_swap_current_bytes);
}

TEST_F(MemoryMonitorUtilsTest, TestCgroupV2OverflowSwapFallsBackToHostSwap) {
  // Numeric swap.max that overflows int64 (e.g. ULLONG_MAX written by the
  // kernel as the "unlimited" sentinel on some configs) — same semantics as
  // the literal "max": fall back to host swap for the cap, but per-cgroup
  // memory.swap.current for usage.
  SetCountSwapFlag(true);
  int64_t cgroup_total_bytes = 4LL * 1024 * 1024 * 1024;
  int64_t cgroup_current_bytes = 2LL * 1024 * 1024 * 1024;
  int64_t cgroup_swap_current_bytes = 256LL * 1024 * 1024;  // 256 MiB
  int64_t mem_total_kb = 16 * 1024 * 1024;
  int64_t mem_available_kb = 4 * 1024 * 1024;
  int64_t swap_total_kb = 8 * 1024 * 1024;
  int64_t swap_free_kb = 4 * 1024 * 1024;  // 4 GiB host-wide used

  std::string cgroup_dir = MockCgroupv2MemoryUsage(cgroup_total_bytes,
                                                   cgroup_current_bytes,
                                                   /*anon_memory_bytes=*/0,
                                                   /*shmem_memory_bytes=*/0,
                                                   /*inactive_file_bytes=*/0,
                                                   /*active_file_bytes=*/0);
  // ULLONG_MAX overflows int64 — triggers std::stoll's out_of_range path,
  // which we treat the same as the literal "max".
  MockCgroupv2Swap(cgroup_dir,
                   /*swap_max_str=*/"18446744073709551615",
                   cgroup_swap_current_bytes);
  std::string proc_dir =
      MockProcMeminfo(mem_total_kb, mem_available_kb, swap_total_kb, swap_free_kb);

  MemoryMonitorUtils::CgroupMemoryBytes cgroup_memory =
      MemoryMonitorUtils::GetCGroupMemoryBytes(cgroup_dir,
                                               /*include_swap=*/true,
                                               proc_dir);

  ASSERT_TRUE(cgroup_memory.has_swap);
  ASSERT_EQ(cgroup_memory.total_bytes, cgroup_total_bytes);
  ASSERT_EQ(cgroup_memory.swap_total_bytes, swap_total_kb * 1024);
  ASSERT_EQ(cgroup_memory.used_bytes, cgroup_current_bytes);
  ASSERT_EQ(cgroup_memory.swap_used_bytes, cgroup_swap_current_bytes);
}

TEST_F(MemoryMonitorUtilsTest, TestCgroupV2UnlimitedRamBoundedSwapCountsCgroupSwap) {
  // cc:330: memory.max is unlimited ("max") but memory.swap.max is bounded.
  // GetCGroupMemoryBytes can't attach swap to an unlimited (kNull) RAM total, so
  // TakeSystemMemoryUsageSnapshot must compose host RAM with the cgroup's
  // bounded swap (NOT host swap), matching the scheduler's
  // get_cgroup_aware_swap_memory. Host swap is set far larger than the cgroup
  // cap so a regression that folds in host swap fails loudly.
  SetCountSwapFlag(true);
  int64_t cgroup_swap_max = 2LL * 1024 * 1024 * 1024;  // 2 GiB cgroup swap cap
  int64_t cgroup_swap_current = 512LL * 1024 * 1024;   // 512 MiB used
  int64_t mem_total_kb = 16 * 1024 * 1024;             // 16 GiB host RAM
  int64_t mem_available_kb = 4 * 1024 * 1024;          // 12 GiB host RAM used
  int64_t host_swap_total_kb = 8 * 1024 * 1024;        // 8 GiB host swap (ignored)
  int64_t host_swap_free_kb = 8 * 1024 * 1024;

  std::string cgroup_dir =
      MockCgroupv2MemoryUsage(/*total_bytes=*/1LL * 1024 * 1024 * 1024,
                              /*current_bytes=*/0,
                              /*anon_memory_bytes=*/0,
                              /*shmem_memory_bytes=*/0,
                              /*inactive_file_bytes=*/0,
                              /*active_file_bytes=*/0);
  // Overwrite memory.max with the unlimited sentinel.
  std::ofstream(cgroup_dir + "/memory.max") << "max";
  MockCgroupv2Swap(cgroup_dir, cgroup_swap_max, cgroup_swap_current);
  std::string proc_dir = MockProcMeminfo(
      mem_total_kb, mem_available_kb, host_swap_total_kb, host_swap_free_kb);

  MemoryUsageSnapshot system_memory = MemoryMonitorUtils::TakeSystemMemoryUsageSnapshot(
      cgroup_dir, /*include_swap=*/true, proc_dir);

  // RAM is unbounded at the cgroup -> host RAM dominates; swap is the cgroup's
  // bounded 2 GiB cap, not the 8 GiB host swap.
  ASSERT_EQ(system_memory.total_bytes, mem_total_kb * 1024 + cgroup_swap_max);
  ASSERT_EQ(system_memory.used_bytes,
            (mem_total_kb - mem_available_kb) * 1024 + cgroup_swap_current);
}

TEST_F(MemoryMonitorUtilsTest, TestCgroupV1MemswAddedToTotalAndUsed) {
  SetCountSwapFlag(true);
  int64_t ram_limit_bytes = 4LL * 1024 * 1024 * 1024;    // 4 GiB RAM limit
  int64_t ram_usage_bytes = 2LL * 1024 * 1024 * 1024;    // 2 GiB RAM used
  int64_t inactive_file_bytes = 200 * 1024 * 1024;       // 200 MiB
  int64_t active_file_bytes = 100 * 1024 * 1024;         // 100 MiB
  int64_t memsw_limit_bytes = 6LL * 1024 * 1024 * 1024;  // 6 GiB RAM+swap limit
  int64_t memsw_usage_bytes = 3LL * 1024 * 1024 * 1024;  // 3 GiB RAM+swap used

  std::string cgroup_dir = MockCgroupv1MemoryUsage(
      ram_limit_bytes, ram_usage_bytes, inactive_file_bytes, active_file_bytes);
  MockCgroupv1Memsw(cgroup_dir, memsw_limit_bytes, memsw_usage_bytes);

  MemoryMonitorUtils::CgroupMemoryBytes cgroup_memory =
      MemoryMonitorUtils::GetCGroupMemoryBytes(cgroup_dir, /*include_swap=*/true);

  // v1 memsw folds RAM+swap into total/used (combined path).
  ASSERT_TRUE(cgroup_memory.combined_ram_swap);
  ASSERT_FALSE(cgroup_memory.has_swap);
  ASSERT_EQ(cgroup_memory.total_bytes, memsw_limit_bytes);
  ASSERT_EQ(cgroup_memory.used_bytes,
            memsw_usage_bytes - inactive_file_bytes - active_file_bytes);
}

TEST_F(MemoryMonitorUtilsTest, TestCgroupV1MemswIgnoredWhenFlagDisabled) {
  SetCountSwapFlag(false);
  int64_t ram_limit_bytes = 4LL * 1024 * 1024 * 1024;
  int64_t ram_usage_bytes = 2LL * 1024 * 1024 * 1024;
  int64_t inactive_file_bytes = 200 * 1024 * 1024;
  int64_t active_file_bytes = 100 * 1024 * 1024;
  int64_t memsw_limit_bytes = 6LL * 1024 * 1024 * 1024;
  int64_t memsw_usage_bytes = 3LL * 1024 * 1024 * 1024;

  std::string cgroup_dir = MockCgroupv1MemoryUsage(
      ram_limit_bytes, ram_usage_bytes, inactive_file_bytes, active_file_bytes);
  MockCgroupv1Memsw(cgroup_dir, memsw_limit_bytes, memsw_usage_bytes);

  MemoryMonitorUtils::CgroupMemoryBytes cgroup_memory =
      MemoryMonitorUtils::GetCGroupMemoryBytes(cgroup_dir, /*include_swap=*/false);

  ASSERT_FALSE(cgroup_memory.has_swap);
  ASSERT_EQ(cgroup_memory.total_bytes, ram_limit_bytes);
  ASSERT_EQ(cgroup_memory.used_bytes,
            ram_usage_bytes - inactive_file_bytes - active_file_bytes);
}

TEST_F(MemoryMonitorUtilsTest, TestCgroupV1MemswFallsBackWhenUsageMissing) {
  // memsw.limit_in_bytes is present but memsw.usage_in_bytes is missing.
  // Total and used must come from the same view — otherwise the OOM threshold
  // would compare a RAM+swap cap against a RAM-only usage.
  SetCountSwapFlag(true);
  int64_t ram_limit_bytes = 4LL * 1024 * 1024 * 1024;
  int64_t ram_usage_bytes = 2LL * 1024 * 1024 * 1024;
  int64_t inactive_file_bytes = 200 * 1024 * 1024;
  int64_t active_file_bytes = 100 * 1024 * 1024;
  int64_t memsw_limit_bytes = 6LL * 1024 * 1024 * 1024;

  std::string cgroup_dir = MockCgroupv1MemoryUsage(
      ram_limit_bytes, ram_usage_bytes, inactive_file_bytes, active_file_bytes);
  std::ofstream(cgroup_dir + "/" + MemoryMonitorUtils::kCgroupsV1MemswMaxPath)
      << memsw_limit_bytes;

  MemoryMonitorUtils::CgroupMemoryBytes cgroup_memory =
      MemoryMonitorUtils::GetCGroupMemoryBytes(cgroup_dir, /*include_swap=*/true);

  // memsw.usage missing -> falls back to the RAM-only counters.
  ASSERT_FALSE(cgroup_memory.has_swap);
  ASSERT_EQ(cgroup_memory.total_bytes, ram_limit_bytes);
  ASSERT_EQ(cgroup_memory.used_bytes,
            ram_usage_bytes - inactive_file_bytes - active_file_bytes);
}

TEST_F(MemoryMonitorUtilsTest, TestCgroupFilesValidReturnsWorkingSet) {
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

  int64_t used_bytes = MemoryMonitorUtils::GetCGroupMemoryUsedBytes(
      stat_file_name.c_str(), curr_file_name.c_str(), "inactive_file", "active_file");

  std::remove(stat_file_name.c_str());
  std::remove(curr_file_name.c_str());

  ASSERT_EQ(used_bytes, 300 - 123 - 88);
}

TEST_F(MemoryMonitorUtilsTest, TestCgroupFilesValidKeyLastReturnsWorkingSet) {
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

  int64_t used_bytes = MemoryMonitorUtils::GetCGroupMemoryUsedBytes(
      stat_file_name.c_str(), curr_file_name.c_str(), "inactive_file", "active_file");

  std::remove(stat_file_name.c_str());
  std::remove(curr_file_name.c_str());

  ASSERT_EQ(used_bytes, 300 - 123 - 88);
}

TEST_F(MemoryMonitorUtilsTest, TestCgroupFilesValidNegativeWorkingSet) {
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

  int64_t used_bytes = MemoryMonitorUtils::GetCGroupMemoryUsedBytes(
      stat_file_name.c_str(), curr_file_name.c_str(), "inactive_file", "active_file");

  std::remove(stat_file_name.c_str());
  std::remove(curr_file_name.c_str());

  ASSERT_EQ(used_bytes, 123 - 300 - 100);
}

TEST_F(MemoryMonitorUtilsTest, TestCgroupFilesValidMissingFieldReturnskNull) {
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

  int64_t used_bytes = MemoryMonitorUtils::GetCGroupMemoryUsedBytes(
      stat_file_name.c_str(), curr_file_name.c_str(), "inactive_file", "active_file");

  std::remove(stat_file_name.c_str());
  std::remove(curr_file_name.c_str());

  ASSERT_EQ(used_bytes, MemoryMonitorInterface::kNull);
}

TEST_F(MemoryMonitorUtilsTest, TestCgroupNonexistentStatFileReturnskNull) {
  std::string stat_file_name = UniqueID::FromRandom().Hex();

  std::string curr_file_name = UniqueID::FromRandom().Hex();
  std::ofstream curr_file;
  curr_file.open(curr_file_name);
  curr_file << "300" << std::endl;
  curr_file.close();

  int64_t used_bytes = MemoryMonitorUtils::GetCGroupMemoryUsedBytes(
      stat_file_name.c_str(), curr_file_name.c_str(), "inactive_file", "active_file");
  std::remove(curr_file_name.c_str());

  ASSERT_EQ(used_bytes, MemoryMonitorInterface::kNull);
}

TEST_F(MemoryMonitorUtilsTest, TestCgroupNonexistentUsageFileReturnskNull) {
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

  int64_t used_bytes = MemoryMonitorUtils::GetCGroupMemoryUsedBytes(
      stat_file_name.c_str(), curr_file_name.c_str(), "inactive_file", "active_file");
  std::remove(stat_file_name.c_str());

  ASSERT_EQ(used_bytes, MemoryMonitorInterface::kNull);
}

TEST_F(MemoryMonitorUtilsTest, TestGetMemoryThresholdTakeGreaterOfTheTwoValues) {
  NoopCgroupManager noop_cgroup_manager;
  ASSERT_EQ(
      MemoryMonitorUtils::GetMemoryThreshold(100, 0.5, 0, false, noop_cgroup_manager),
      100);
  ASSERT_EQ(
      MemoryMonitorUtils::GetMemoryThreshold(100, 0.5, 60, false, noop_cgroup_manager),
      50);

  ASSERT_EQ(
      MemoryMonitorUtils::GetMemoryThreshold(100, 1, 10, false, noop_cgroup_manager),
      100);
  ASSERT_EQ(
      MemoryMonitorUtils::GetMemoryThreshold(100, 1, 100, false, noop_cgroup_manager),
      100);

  ASSERT_EQ(
      MemoryMonitorUtils::GetMemoryThreshold(100, 0.1, 100, false, noop_cgroup_manager),
      10);
  ASSERT_EQ(
      MemoryMonitorUtils::GetMemoryThreshold(100, 0, 10, false, noop_cgroup_manager), 90);
  ASSERT_EQ(
      MemoryMonitorUtils::GetMemoryThreshold(100, 0, 100, false, noop_cgroup_manager), 0);

  ASSERT_EQ(MemoryMonitorUtils::GetMemoryThreshold(
                100, 0, MemoryMonitorInterface::kNull, false, noop_cgroup_manager),
            0);
  ASSERT_EQ(MemoryMonitorUtils::GetMemoryThreshold(
                100, 0.5, MemoryMonitorInterface::kNull, false, noop_cgroup_manager),
            50);
  ASSERT_EQ(MemoryMonitorUtils::GetMemoryThreshold(
                100, 1, MemoryMonitorInterface::kNull, false, noop_cgroup_manager),
            100);
}

TEST_F(
    MemoryMonitorUtilsTest,
    TestGetMemoryThresholdWithResourceIsolationUsesUpperBoundConstraintToComputeThreshold) {
  // Pin swap accounting off so the threshold is just memory.high, independent
  // of whichever swap-related test ran before (RayConfig is process-global).
  SetCountSwapFlag(false);
  // Create a fake cgroup directory using MockCgroupv2MemoryUsage.
  std::string cgroup_dir = MockCgroupv2MemoryUsage(
      /*total_bytes=*/16LL * 1024 * 1024 * 1024,
      /*current_bytes=*/5LL * 1024 * 1024 * 1024,
      /*anon_memory_bytes=*/0,
      /*shmem_memory_bytes=*/0,
      /*inactive_file_bytes=*/100 * 1024 * 1024,
      /*active_file_bytes=*/50 * 1024 * 1024);

  // Create a CgroupManager backed by a fake cgroup driver on the mock cgroup directory.
  std::shared_ptr<std::unordered_map<std::string, FakeCgroup>> cgroups =
      std::make_shared<std::unordered_map<std::string, FakeCgroup>>();
  cgroups->emplace(cgroup_dir, FakeCgroup{cgroup_dir, {5}, {}, {"cpu", "memory"}, {}});
  std::unique_ptr<FakeCgroupDriver> driver = FakeCgroupDriver::Create(cgroups);

  int64_t user_memory_max_bytes = 10LL * 1024 * 1024 * 1024;  // 10 GB
  int64_t user_memory_high_bytes = 8LL * 1024 * 1024 * 1024;  // 8 GB
  StatusOr<std::unique_ptr<CgroupManager>> result =
      CgroupManager::Create(cgroup_dir,
                            "node_id_123",
                            /*system_reserved_cpu_weight=*/100,
                            /*system_memory_bytes_min=*/1LL * 1024 * 1024 * 1024,
                            /*system_memory_bytes_low=*/1LL * 1024 * 1024 * 1024,
                            user_memory_high_bytes,
                            user_memory_max_bytes,
                            std::move(driver));
  std::unique_ptr<CgroupManager> cgroup_manager = std::move(result.value());

  int64_t expected_default_mode_threshold = user_memory_high_bytes;
  ASSERT_EQ(MemoryMonitorUtils::GetMemoryThreshold(
                /*total_memory_bytes=*/16LL * 1024 * 1024 * 1024,
                /*usage_threshold=*/0.5,
                /*min_memory_free_bytes=*/MemoryMonitorInterface::kNull,
                /*resource_isolation_enabled=*/true,
                *cgroup_manager),
            expected_default_mode_threshold);
}

TEST_F(MemoryMonitorUtilsTest, TestGetMemoryThresholdResourceIsolationAddsRootSwap) {
  // B1: under resource isolation the threshold's swap budget comes from the
  // ROOT cgroup's swap.max (the slice's effective inherited cap, and the same
  // path the scheduler's get_cgroup_aware_swap_memory reads), added on top of
  // the user slice's memory.high.
  SetCountSwapFlag(true);
  int64_t root_swap_max = 2LL * 1024 * 1024 * 1024;  // 2 GiB
  std::string cgroup_dir = MockCgroupv2MemoryUsage(
      /*total_bytes=*/16LL * 1024 * 1024 * 1024,
      /*current_bytes=*/5LL * 1024 * 1024 * 1024,
      /*anon_memory_bytes=*/0,
      /*shmem_memory_bytes=*/0,
      /*inactive_file_bytes=*/0,
      /*active_file_bytes=*/0);
  // Root cgroup swap budget read by GetMemoryThreshold via the filesystem.
  MockCgroupv2Swap(cgroup_dir, root_swap_max, /*swap_current_bytes=*/0);

  std::shared_ptr<std::unordered_map<std::string, FakeCgroup>> cgroups =
      std::make_shared<std::unordered_map<std::string, FakeCgroup>>();
  cgroups->emplace(cgroup_dir, FakeCgroup{cgroup_dir, {5}, {}, {"cpu", "memory"}, {}});
  std::unique_ptr<FakeCgroupDriver> driver = FakeCgroupDriver::Create(cgroups);

  int64_t user_memory_max_bytes = 10LL * 1024 * 1024 * 1024;
  int64_t user_memory_high_bytes = 8LL * 1024 * 1024 * 1024;
  StatusOr<std::unique_ptr<CgroupManager>> result =
      CgroupManager::Create(cgroup_dir,
                            "node_id_123",
                            /*system_reserved_cpu_weight=*/100,
                            /*system_memory_bytes_min=*/1LL * 1024 * 1024 * 1024,
                            /*system_memory_bytes_low=*/1LL * 1024 * 1024 * 1024,
                            user_memory_high_bytes,
                            user_memory_max_bytes,
                            std::move(driver));
  std::unique_ptr<CgroupManager> cgroup_manager = std::move(result.value());

  // Threshold = user slice memory.high + root cgroup swap.max.
  ASSERT_EQ(MemoryMonitorUtils::GetMemoryThreshold(
                /*total_memory_bytes=*/16LL * 1024 * 1024 * 1024,
                /*usage_threshold=*/0.5,
                /*min_memory_free_bytes=*/MemoryMonitorInterface::kNull,
                /*resource_isolation_enabled=*/true,
                *cgroup_manager,
                /*root_cgroup_path=*/cgroup_dir),
            user_memory_high_bytes + root_swap_max);
}

TEST_F(MemoryMonitorUtilsTest, TestGetPidsFromDirOnlyReturnsNumericFilenames) {
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

  auto pids = MemoryMonitorUtils::GetPidsFromDir(proc_dir);

  boost::filesystem::remove_all(proc_dir);

  ASSERT_EQ(pids.size(), 1);
  ASSERT_EQ(pids[0], 123);
}

TEST_F(MemoryMonitorUtilsTest, TestGetPidsFromNonExistentDirReturnsEmpty) {
  std::string proc_dir = UniqueID::FromRandom().Hex();
  auto pids = MemoryMonitorUtils::GetPidsFromDir(proc_dir);
  ASSERT_EQ(pids.size(), 0);
}

TEST_F(MemoryMonitorUtilsTest, TestGetCommandLinePidExistReturnsValid) {
  std::string proc_dir = UniqueID::FromRandom().Hex();
  std::string pid_dir = proc_dir + "/123";
  boost::filesystem::create_directories(pid_dir);

  std::string cmdline_filename = pid_dir + "/" + MemoryMonitorUtils::kCommandlinePath;

  std::ofstream cmdline_file;
  cmdline_file.open(cmdline_filename);
  cmdline_file << "/my/very/custom/command --test passes!     ";
  cmdline_file.close();

  std::string commandline = MemoryMonitorUtils::GetCommandLineForPid(123, proc_dir);

  boost::filesystem::remove_all(proc_dir);

  ASSERT_EQ(commandline, "/my/very/custom/command --test passes!");
}

TEST_F(MemoryMonitorUtilsTest, TestGetCommandLineMissingFileReturnsEmpty) {
  {
    std::string proc_dir = UniqueID::FromRandom().Hex();
    std::string commandline = MemoryMonitorUtils::GetCommandLineForPid(123, proc_dir);
    boost::filesystem::remove_all(proc_dir);
    ASSERT_EQ(commandline, "");
  }

  {
    std::string proc_dir = UniqueID::FromRandom().Hex();
    boost::filesystem::create_directory(proc_dir);
    std::string commandline = MemoryMonitorUtils::GetCommandLineForPid(123, proc_dir);
    boost::filesystem::remove_all(proc_dir);
    ASSERT_EQ(commandline, "");
  }

  {
    std::string proc_dir = UniqueID::FromRandom().Hex();
    std::string pid_dir = proc_dir + "/123";
    boost::filesystem::create_directories(pid_dir);
    std::string commandline = MemoryMonitorUtils::GetCommandLineForPid(123, proc_dir);
    boost::filesystem::remove_all(proc_dir);
    ASSERT_EQ(commandline, "");
  }
}

TEST_F(MemoryMonitorUtilsTest, TestShortStringNotTruncated) {
  std::string out = MemoryMonitorUtils::TruncateString("im short", 20);
  ASSERT_EQ(out, "im short");
}

TEST_F(MemoryMonitorUtilsTest, TestLongStringTruncated) {
  std::string out = MemoryMonitorUtils::TruncateString(std::string(7, 'k'), 5);
  ASSERT_EQ(out, "kkkkk...");
}

TEST_F(MemoryMonitorUtilsTest, TestTopNLessThanNReturnsMemoryUsedDesc) {
  absl::flat_hash_map<pid_t, int64_t> usage;
  usage.insert({1, 111});
  usage.insert({2, 222});
  usage.insert({3, 333});

  auto list = MemoryMonitorUtils::GetTopNMemoryUsage(2, usage);

  ASSERT_EQ(list.size(), 2);
  ASSERT_EQ(std::get<0>(list[0]), 3);
  ASSERT_EQ(std::get<1>(list[0]), 333);
  ASSERT_EQ(std::get<0>(list[1]), 2);
  ASSERT_EQ(std::get<1>(list[1]), 222);
}

TEST_F(MemoryMonitorUtilsTest, TestTopNMoreThanNReturnsAllDesc) {
  absl::flat_hash_map<pid_t, int64_t> usage;
  usage.insert({1, 111});
  usage.insert({2, 222});

  auto list = MemoryMonitorUtils::GetTopNMemoryUsage(3, usage);

  ASSERT_EQ(list.size(), 2);
  ASSERT_EQ(std::get<0>(list[0]), 2);
  ASSERT_EQ(std::get<1>(list[0]), 222);
  ASSERT_EQ(std::get<0>(list[1]), 1);
  ASSERT_EQ(std::get<1>(list[1]), 111);
}

TEST_F(MemoryMonitorUtilsTest, TestTakePerProcessMemorySnapshotFiltersBadPids) {
  std::string proc_dir = MockProcMemoryUsage(1, "111");

  // Invalid pids with no memory usage file.
  StatusOr<std::unique_ptr<TempDirectory>> proc_2_memory_usage_file_or =
      TempDirectory::Create(proc_dir + "/2");
  RAY_CHECK(proc_2_memory_usage_file_or.ok())
      << "Failed to create temp directory: "
      << proc_2_memory_usage_file_or.status().message();
  std::unique_ptr<TempFile> proc_2_memory_usage_file =
      std::make_unique<TempFile>(proc_dir + "/2/smaps_rollup");
  StatusOr<std::unique_ptr<TempDirectory>> proc_3_memory_usage_file_or =
      TempDirectory::Create(proc_dir + "/3");
  RAY_CHECK(proc_3_memory_usage_file_or.ok())
      << "Failed to create temp directory: "
      << proc_3_memory_usage_file_or.status().message();
  std::unique_ptr<TempFile> proc_3_memory_usage_file =
      std::make_unique<TempFile>(proc_dir + "/3/smaps_rollup");

  auto usage = MemoryMonitorUtils::TakePerProcessMemorySnapshot(proc_dir);

  ASSERT_EQ(usage.size(), 1);
  ASSERT_TRUE(usage.contains(1));
}

TEST_F(MemoryMonitorUtilsTest, TestTakeCgroupSnapshotEmptyPathReturnsNotFound) {
  StatusSetOr<CgroupMemorySnapshot, StatusT::NotFound> result =
      MemoryMonitorUtils::TakeCgroupMemorySnapshot("");
  ASSERT_TRUE(result.has_error());
  ASSERT_TRUE(std::holds_alternative<StatusT::NotFound>(result.error()));
}

TEST_F(MemoryMonitorUtilsTest, TestTakeCgroupSnapshotNonexistentPathReturnsNotFound) {
  StatusSetOr<CgroupMemorySnapshot, StatusT::NotFound> result =
      MemoryMonitorUtils::TakeCgroupMemorySnapshot("/nonexistent/cgroup/path");
  ASSERT_TRUE(result.has_error());
  ASSERT_TRUE(std::holds_alternative<StatusT::NotFound>(result.error()));
}

TEST_F(MemoryMonitorUtilsTest, TestTakeCgroupv2SnapshotReturnsCorrectAnonAndShmem) {
  int64_t total_bytes = 1LL * 1024 * 1024 * 1024;  // 1 GB
  int64_t current_bytes = 500 * 1024 * 1024;       // 500 MB
  int64_t anon_bytes = 200 * 1024 * 1024;          // 200 MB
  int64_t shmem_bytes = 100 * 1024 * 1024;         // 100 MB
  int64_t inactive_file_bytes = 50 * 1024 * 1024;  // 50 MB
  int64_t active_file_bytes = 30 * 1024 * 1024;    // 30 MB

  std::string cgroup_dir = MockCgroupv2MemoryUsage(total_bytes,
                                                   current_bytes,
                                                   anon_bytes,
                                                   shmem_bytes,
                                                   inactive_file_bytes,
                                                   active_file_bytes);
  StatusSetOr<CgroupMemorySnapshot, StatusT::NotFound> result =
      MemoryMonitorUtils::TakeCgroupMemorySnapshot(cgroup_dir);

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result.value().anon_memory_bytes, anon_bytes);
  ASSERT_EQ(result.value().shmem_memory_bytes, shmem_bytes);
}

TEST_F(MemoryMonitorUtilsTest, TestTakeCgroupv1SnapshotReturnsNotFound) {
  int64_t total_bytes = 1LL * 1024 * 1024 * 1024;  // 1 GB
  int64_t current_bytes = 500 * 1024 * 1024;       // 500 MB
  int64_t inactive_file_bytes = 50 * 1024 * 1024;  // 50 MB
  int64_t active_file_bytes = 30 * 1024 * 1024;    // 30 MB

  std::string cgroup_dir = MockCgroupv1MemoryUsage(
      total_bytes, current_bytes, inactive_file_bytes, active_file_bytes);
  StatusSetOr<CgroupMemorySnapshot, StatusT::NotFound> result =
      MemoryMonitorUtils::TakeCgroupMemorySnapshot(cgroup_dir);

  ASSERT_TRUE(result.has_error());
  ASSERT_TRUE(std::holds_alternative<StatusT::NotFound>(result.error()));
}

TEST_F(MemoryMonitorUtilsTest,
       TestTakeCgroupv2SnapshotMissingAnonOrShmemReturnsNotFound) {
  std::string cgroup_dir_anon = MockCgroupv2MemoryUsage(
      /*total_bytes=*/1LL * 1024 * 1024 * 1024,
      /*current_bytes=*/500 * 1024 * 1024,
      /*anon_memory_bytes=*/std::nullopt,
      /*shmem_memory_bytes=*/100 * 1024 * 1024,
      /*inactive_file_bytes=*/50 * 1024 * 1024,
      /*active_file_bytes=*/30 * 1024 * 1024);
  StatusSetOr<CgroupMemorySnapshot, StatusT::NotFound> result_anon =
      MemoryMonitorUtils::TakeCgroupMemorySnapshot(cgroup_dir_anon);

  ASSERT_TRUE(result_anon.has_error());
  ASSERT_TRUE(std::holds_alternative<StatusT::NotFound>(result_anon.error()));

  std::string cgroup_dir_shmem = MockCgroupv2MemoryUsage(
      /*total_bytes=*/1LL * 1024 * 1024 * 1024,
      /*current_bytes=*/500 * 1024 * 1024,
      /*anon_memory_bytes=*/500 * 1024 * 1024,
      /*shmem_memory_bytes=*/std::nullopt,
      /*inactive_file_bytes=*/50 * 1024 * 1024,
      /*active_file_bytes=*/30 * 1024 * 1024);
  StatusSetOr<CgroupMemorySnapshot, StatusT::NotFound> result_shmem =
      MemoryMonitorUtils::TakeCgroupMemorySnapshot(cgroup_dir_shmem);

  ASSERT_TRUE(result_shmem.has_error());
  ASSERT_TRUE(std::holds_alternative<StatusT::NotFound>(result_shmem.error()));
}

TEST_F(MemoryMonitorUtilsTest,
       TestTakeUserAndSystemSliceMemoryUsageSnapshotOnCgroupV1ReturnsNotFound) {
  std::string user_cgroup_dir = MockCgroupv1MemoryUsage(
      /*total_bytes=*/1LL * 1024 * 1024 * 1024,
      /*current_bytes=*/500 * 1024 * 1024,
      /*inactive_file_bytes=*/30 * 1024 * 1024,
      /*active_file_bytes=*/20 * 1024 * 1024);
  std::string system_cgroup_dir = MockCgroupv1MemoryUsage(
      /*total_bytes=*/1LL * 1024 * 1024 * 1024,
      /*current_bytes=*/500 * 1024 * 1024,
      /*inactive_file_bytes=*/30 * 1024 * 1024,
      /*active_file_bytes=*/20 * 1024 * 1024);
  StatusSetOr<std::pair<MemoryUsageSnapshot, MemoryUsageSnapshot>, StatusT::NotFound>
      result = MemoryMonitorUtils::TakeUserAndSystemSliceMemoryUsageSnapshot(
          user_cgroup_dir, system_cgroup_dir);
  ASSERT_TRUE(result.has_error());
  ASSERT_TRUE(std::holds_alternative<StatusT::NotFound>(result.error()));
}

TEST_F(MemoryMonitorUtilsTest,
       TestTakeUserAndSystemSliceMemoryUsageSnapshotValidPathsReturnsCorrectUsedBytes) {
  // Pin the flag off so the assertion against
  // TakeSystemMemoryUsageSnapshot's host total is independent of which
  // swap-related test ran before this one (RayConfig is process-global).
  SetCountSwapFlag(false);
  int64_t user_anon_bytes = 200 * 1024 * 1024;    // 200 MB
  int64_t user_shmem_bytes = 100 * 1024 * 1024;   // 100 MB
  int64_t system_shmem_bytes = 50 * 1024 * 1024;  // 50 MB

  std::string user_cgroup_dir = MockCgroupv2MemoryUsage(
      /*total_bytes=*/1LL * 1024 * 1024 * 1024,
      /*current_bytes=*/500 * 1024 * 1024,
      /*anon_memory_bytes=*/user_anon_bytes,
      /*shmem_memory_bytes=*/user_shmem_bytes,
      /*inactive_file_bytes=*/30 * 1024 * 1024,
      /*active_file_bytes=*/20 * 1024 * 1024);
  std::string system_cgroup_dir = MockCgroupv2MemoryUsage(
      /*total_bytes=*/1LL * 1024 * 1024 * 1024,
      /*current_bytes=*/200 * 1024 * 1024,
      /*anon_memory_bytes=*/0,
      /*shmem_memory_bytes=*/system_shmem_bytes,
      /*inactive_file_bytes=*/10 * 1024 * 1024,
      /*active_file_bytes=*/10 * 1024 * 1024);

  int64_t expected_used_bytes = user_anon_bytes + user_shmem_bytes + system_shmem_bytes;
  StatusSetOr<std::pair<MemoryUsageSnapshot, MemoryUsageSnapshot>, StatusT::NotFound>
      result = MemoryMonitorUtils::TakeUserAndSystemSliceMemoryUsageSnapshot(
          user_cgroup_dir, system_cgroup_dir);
  MemoryUsageSnapshot host_memory = MemoryMonitorUtils::TakeSystemMemoryUsageSnapshot("");
  ASSERT_TRUE(result.has_value());
  auto [user_slice_memory, system_slice_memory] = result.value();
  // The user slice approximates user application usage from cgroup anon + shmem.
  ASSERT_EQ(user_slice_memory.used_bytes, expected_used_bytes);
  ASSERT_EQ(user_slice_memory.total_bytes, host_memory.total_bytes);
  // The system slice is the remaining host usage not attributed to the user slice.
  ASSERT_EQ(system_slice_memory.total_bytes, host_memory.total_bytes);
  ASSERT_EQ(system_slice_memory.used_bytes, host_memory.used_bytes - expected_used_bytes);
}

// Deterministic /proc/meminfo for the user-slice swap tests, so host total is
// independent of the machine running the test.
namespace {
constexpr int64_t kUserSliceHostMemKb = 16 * 1024 * 1024;      // 16 GiB
constexpr int64_t kUserSliceHostMemAvailKb = 4 * 1024 * 1024;  // 4 GiB
}  // namespace

TEST_F(MemoryMonitorUtilsTest, TestUserSliceSwapAddedToTotalAndUsed) {
  SetCountSwapFlag(true);
  int64_t user_anon = 200 * 1024 * 1024;
  int64_t user_shmem = 100 * 1024 * 1024;
  int64_t system_shmem = 50 * 1024 * 1024;
  int64_t root_swap_max = 1LL * 1024 * 1024 * 1024;  // budget comes from the root
  int64_t user_swap_cur = 300 * 1024 * 1024;         // used comes from the user leaf

  std::string user_dir = MockCgroupv2MemoryUsage(/*total_bytes=*/1LL * 1024 * 1024 * 1024,
                                                 /*current_bytes=*/0,
                                                 user_anon,
                                                 user_shmem,
                                                 /*inactive_file=*/0,
                                                 /*active_file=*/0);
  // User-leaf swap.max just needs to be non-zero so the leaf swap.current is read
  // as used; it does NOT set the budget (the root cap does).
  MockCgroupv2Swap(user_dir, /*swap_max_bytes=*/2LL * 1024 * 1024 * 1024, user_swap_cur);
  std::string system_dir =
      MockCgroupv2MemoryUsage(/*total_bytes=*/1LL * 1024 * 1024 * 1024,
                              /*current_bytes=*/0,
                              /*anon=*/0,
                              system_shmem,
                              /*inactive_file=*/0,
                              /*active_file=*/0);
  // The root cgroup holds the effective swap budget for both slices.
  std::string root_dir = MockCgroupv2MemoryUsage(/*total_bytes=*/1LL * 1024 * 1024 * 1024,
                                                 /*current_bytes=*/0,
                                                 /*anon=*/0,
                                                 /*shmem=*/0,
                                                 /*inactive_file=*/0,
                                                 /*active_file=*/0);
  MockCgroupv2Swap(root_dir, root_swap_max, /*swap_current_bytes=*/0);
  std::string proc_dir = MockProcMeminfo(
      kUserSliceHostMemKb, kUserSliceHostMemAvailKb, std::nullopt, std::nullopt);

  auto result = MemoryMonitorUtils::TakeUserAndSystemSliceMemoryUsageSnapshot(
      user_dir, system_dir, proc_dir, root_dir);

  ASSERT_TRUE(result.has_value());
  auto [user_slice, system_slice] = result.value();
  // Budget (total) comes from the root cgroup swap.max; usage comes from the
  // user leaf's swap.current.
  ASSERT_EQ(user_slice.used_bytes, user_anon + user_shmem + system_shmem + user_swap_cur);
  ASSERT_EQ(user_slice.total_bytes, kUserSliceHostMemKb * 1024 + root_swap_max);
  // The system slice carries the same swap budget so the reserved-memory check
  // (system_total - threshold) stays RAM-consistent; its usage is RAM-only.
  ASSERT_EQ(system_slice.total_bytes, kUserSliceHostMemKb * 1024 + root_swap_max);
  int64_t host_used_bytes = (kUserSliceHostMemKb - kUserSliceHostMemAvailKb) * 1024;
  ASSERT_EQ(system_slice.used_bytes,
            host_used_bytes - (user_anon + user_shmem + system_shmem));
}

TEST_F(MemoryMonitorUtilsTest, TestUserSliceSwapIgnoredWhenFlagDisabled) {
  // Flag off must be a hard no-op: even if swap files are present on both
  // slices, neither used nor total may grow. This guards the default
  // behavior on every existing deployment.
  SetCountSwapFlag(false);
  int64_t user_anon = 200 * 1024 * 1024;
  int64_t user_shmem = 100 * 1024 * 1024;
  int64_t system_shmem = 50 * 1024 * 1024;

  std::string user_dir = MockCgroupv2MemoryUsage(/*total_bytes=*/1LL * 1024 * 1024 * 1024,
                                                 /*current_bytes=*/0,
                                                 user_anon,
                                                 user_shmem,
                                                 0,
                                                 0);
  MockCgroupv2Swap(user_dir,
                   /*swap_max_bytes=*/1LL * 1024 * 1024 * 1024,
                   /*swap_current_bytes=*/300 * 1024 * 1024);
  std::string system_dir =
      MockCgroupv2MemoryUsage(/*total_bytes=*/1LL * 1024 * 1024 * 1024,
                              /*current_bytes=*/0,
                              /*anon=*/0,
                              system_shmem,
                              0,
                              0);
  MockCgroupv2Swap(system_dir,
                   /*swap_max_bytes=*/512 * 1024 * 1024,
                   /*swap_current_bytes=*/100 * 1024 * 1024);
  std::string root_dir = MockCgroupv2MemoryUsage(/*total_bytes=*/1LL * 1024 * 1024 * 1024,
                                                 /*current_bytes=*/0,
                                                 /*anon=*/0,
                                                 /*shmem=*/0,
                                                 0,
                                                 0);
  MockCgroupv2Swap(root_dir,
                   /*swap_max_bytes=*/1LL * 1024 * 1024 * 1024,
                   /*swap_current_bytes=*/0);
  std::string proc_dir = MockProcMeminfo(
      kUserSliceHostMemKb, kUserSliceHostMemAvailKb, std::nullopt, std::nullopt);

  auto result = MemoryMonitorUtils::TakeUserAndSystemSliceMemoryUsageSnapshot(
      user_dir, system_dir, proc_dir, root_dir);

  ASSERT_TRUE(result.has_value());
  auto [user_slice, system_slice] = result.value();
  ASSERT_EQ(user_slice.used_bytes, user_anon + user_shmem + system_shmem);
  ASSERT_EQ(user_slice.total_bytes, kUserSliceHostMemKb * 1024);
  ASSERT_EQ(system_slice.total_bytes, kUserSliceHostMemKb * 1024);
}

TEST_F(MemoryMonitorUtilsTest, TestUserSliceUnlimitedSwapFallsBackToHostSwap) {
  // Root swap.max == "max" means no swap cap at the root, so the practical
  // budget is host swap (matches GetMemoryThreshold and the Python helper).
  // Usage still comes from the user leaf's swap.current.
  SetCountSwapFlag(true);
  int64_t user_anon = 200 * 1024 * 1024;
  int64_t user_shmem = 100 * 1024 * 1024;
  int64_t system_shmem = 50 * 1024 * 1024;
  int64_t user_swap_current = 256 * 1024 * 1024;
  int64_t swap_total_kb = 4 * 1024 * 1024;  // 4 GiB host swap
  int64_t swap_free_kb = 3 * 1024 * 1024;   // 1 GiB used host-wide

  std::string user_dir = MockCgroupv2MemoryUsage(/*total_bytes=*/1LL * 1024 * 1024 * 1024,
                                                 /*current_bytes=*/0,
                                                 user_anon,
                                                 user_shmem,
                                                 0,
                                                 0);
  // Non-zero user-leaf swap.max so the leaf swap.current is read as used.
  MockCgroupv2Swap(user_dir,
                   /*swap_max_bytes=*/2LL * 1024 * 1024 * 1024,
                   /*swap_current_bytes=*/user_swap_current);
  std::string system_dir =
      MockCgroupv2MemoryUsage(/*total_bytes=*/1LL * 1024 * 1024 * 1024,
                              /*current_bytes=*/0,
                              /*anon=*/0,
                              system_shmem,
                              0,
                              0);
  std::string root_dir = MockCgroupv2MemoryUsage(/*total_bytes=*/1LL * 1024 * 1024 * 1024,
                                                 /*current_bytes=*/0,
                                                 /*anon=*/0,
                                                 /*shmem=*/0,
                                                 0,
                                                 0);
  MockCgroupv2Swap(root_dir,
                   /*swap_max_bytes=*/std::nullopt,  // writes "max" (unlimited)
                   /*swap_current_bytes=*/0);
  std::string proc_dir = MockProcMeminfo(
      kUserSliceHostMemKb, kUserSliceHostMemAvailKb, swap_total_kb, swap_free_kb);

  auto result = MemoryMonitorUtils::TakeUserAndSystemSliceMemoryUsageSnapshot(
      user_dir, system_dir, proc_dir, root_dir);

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result.value().first.used_bytes,
            user_anon + user_shmem + system_shmem + user_swap_current);
  ASSERT_EQ(result.value().first.total_bytes,
            kUserSliceHostMemKb * 1024 + swap_total_kb * 1024);
}

TEST_F(MemoryMonitorUtilsTest, TestUserSliceOverflowSwapFallsBackToHostSwap) {
  // Root swap.max that overflows int64 — std::stoll raises out_of_range, same
  // semantics as the literal "max": fall back to host swap for the budget.
  SetCountSwapFlag(true);
  int64_t user_anon = 200 * 1024 * 1024;
  int64_t user_shmem = 100 * 1024 * 1024;
  int64_t system_shmem = 50 * 1024 * 1024;
  int64_t user_swap_current = 128 * 1024 * 1024;
  int64_t swap_total_kb = 4 * 1024 * 1024;
  int64_t swap_free_kb = 4 * 1024 * 1024;

  std::string user_dir = MockCgroupv2MemoryUsage(/*total_bytes=*/1LL * 1024 * 1024 * 1024,
                                                 /*current_bytes=*/0,
                                                 user_anon,
                                                 user_shmem,
                                                 0,
                                                 0);
  MockCgroupv2Swap(user_dir,
                   /*swap_max_bytes=*/2LL * 1024 * 1024 * 1024,
                   /*swap_current_bytes=*/user_swap_current);
  std::string system_dir =
      MockCgroupv2MemoryUsage(/*total_bytes=*/1LL * 1024 * 1024 * 1024,
                              /*current_bytes=*/0,
                              /*anon=*/0,
                              system_shmem,
                              0,
                              0);
  std::string root_dir = MockCgroupv2MemoryUsage(/*total_bytes=*/1LL * 1024 * 1024 * 1024,
                                                 /*current_bytes=*/0,
                                                 /*anon=*/0,
                                                 /*shmem=*/0,
                                                 0,
                                                 0);
  MockCgroupv2Swap(root_dir,
                   /*swap_max_str=*/"18446744073709551615",
                   /*swap_current_bytes=*/0);
  std::string proc_dir = MockProcMeminfo(
      kUserSliceHostMemKb, kUserSliceHostMemAvailKb, swap_total_kb, swap_free_kb);

  auto result = MemoryMonitorUtils::TakeUserAndSystemSliceMemoryUsageSnapshot(
      user_dir, system_dir, proc_dir, root_dir);

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result.value().first.used_bytes,
            user_anon + user_shmem + system_shmem + user_swap_current);
  ASSERT_EQ(result.value().first.total_bytes,
            kUserSliceHostMemKb * 1024 + swap_total_kb * 1024);
}

TEST_F(MemoryMonitorUtilsTest, TestUserSliceZeroSwapMaxIgnoresCurrent) {
  // swap.max == 0 means swap is disabled. At the root it yields a zero budget;
  // at the user leaf, swap.current may still report a stale/transitioning value
  // that must NOT be surfaced as used bytes against a zero total.
  SetCountSwapFlag(true);
  int64_t user_anon = 200 * 1024 * 1024;
  int64_t user_shmem = 100 * 1024 * 1024;
  int64_t system_shmem = 50 * 1024 * 1024;
  // Sentinel value that must NOT appear in the result if swap.max == 0.
  constexpr int64_t kSwapCurrentSentinel = 12345;

  std::string user_dir = MockCgroupv2MemoryUsage(/*total_bytes=*/1LL * 1024 * 1024 * 1024,
                                                 /*current_bytes=*/0,
                                                 user_anon,
                                                 user_shmem,
                                                 0,
                                                 0);
  MockCgroupv2Swap(user_dir,
                   /*swap_max_bytes=*/std::optional<int64_t>{0},
                   /*swap_current_bytes=*/kSwapCurrentSentinel);
  std::string system_dir =
      MockCgroupv2MemoryUsage(/*total_bytes=*/1LL * 1024 * 1024 * 1024,
                              /*current_bytes=*/0,
                              /*anon=*/0,
                              system_shmem,
                              0,
                              0);
  std::string root_dir = MockCgroupv2MemoryUsage(/*total_bytes=*/1LL * 1024 * 1024 * 1024,
                                                 /*current_bytes=*/0,
                                                 /*anon=*/0,
                                                 /*shmem=*/0,
                                                 0,
                                                 0);
  MockCgroupv2Swap(root_dir,
                   /*swap_max_bytes=*/std::optional<int64_t>{0},
                   /*swap_current_bytes=*/kSwapCurrentSentinel);
  std::string proc_dir = MockProcMeminfo(
      kUserSliceHostMemKb, kUserSliceHostMemAvailKb, std::nullopt, std::nullopt);

  auto result = MemoryMonitorUtils::TakeUserAndSystemSliceMemoryUsageSnapshot(
      user_dir, system_dir, proc_dir, root_dir);

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result.value().first.used_bytes, user_anon + user_shmem + system_shmem);
  ASSERT_EQ(result.value().first.total_bytes, kUserSliceHostMemKb * 1024);
}

TEST_F(MemoryMonitorUtilsTest, TestUserSliceMissingSwapFiles) {
  // Hosts without cgroup swap accounting compiled in won't have these files
  // at all. The helper must degrade to the pre-flag behavior, not error.
  SetCountSwapFlag(true);
  int64_t user_anon = 200 * 1024 * 1024;
  int64_t user_shmem = 100 * 1024 * 1024;
  int64_t system_shmem = 50 * 1024 * 1024;

  std::string user_dir = MockCgroupv2MemoryUsage(/*total_bytes=*/1LL * 1024 * 1024 * 1024,
                                                 /*current_bytes=*/0,
                                                 user_anon,
                                                 user_shmem,
                                                 0,
                                                 0);
  std::string system_dir =
      MockCgroupv2MemoryUsage(/*total_bytes=*/1LL * 1024 * 1024 * 1024,
                              /*current_bytes=*/0,
                              /*anon=*/0,
                              system_shmem,
                              0,
                              0);
  // Root cgroup with no swap files either — no swap budget.
  std::string root_dir = MockCgroupv2MemoryUsage(/*total_bytes=*/1LL * 1024 * 1024 * 1024,
                                                 /*current_bytes=*/0,
                                                 /*anon=*/0,
                                                 /*shmem=*/0,
                                                 0,
                                                 0);
  std::string proc_dir = MockProcMeminfo(
      kUserSliceHostMemKb, kUserSliceHostMemAvailKb, std::nullopt, std::nullopt);

  auto result = MemoryMonitorUtils::TakeUserAndSystemSliceMemoryUsageSnapshot(
      user_dir, system_dir, proc_dir, root_dir);

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result.value().first.used_bytes, user_anon + user_shmem + system_shmem);
  ASSERT_EQ(result.value().first.total_bytes, kUserSliceHostMemKb * 1024);
}

}  // namespace ray
