// Copyright 2017 The Ray Authors.
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

#include <filesystem>

#include "gtest/gtest.h"
#include "ray/object_manager/plasma/plasma_allocator.h"

using namespace std::filesystem;

namespace plasma {
namespace {
const int64_t kMB = 1024 * 1024;
std::string CreateTestDir() {
  path directory = std::filesystem::temp_directory_path() / GenerateUUIDV4();
  create_directories(directory);
  return directory.string();
}

std::string executeCommand(const std::string &cmd) {
  std::string result;
  char buffer[128];
  FILE *pipe = popen(cmd.data(), "r");
  if (pipe == nullptr) {
    return "Error";
  }

  while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
    result += buffer;
  }

  pclose(pipe);
  return result;
}

int getNumberOfMappedFiles() {
#ifdef __linux__
  int pid = getpid();
  std::ifstream proc_maps("/proc/" + std::to_string(pid) + "/maps");
  if (!proc_maps.is_open()) {
    std::cerr << "Failed to open /proc/" << pid << "/maps" << std::endl;
    return -1;
  }

  int count = 0;
  std::string line;
  while (std::getline(proc_maps, line)) {
    count++;
  }
  return count;
#elif defined(__APPLE__)
  std::string command = "vmmap --summary " + std::to_string(getpid()) +
                        " | grep \"mapped file\" | awk '{print $NF}'";
  std::string result = executeCommand(command);
  return std::atoi(result.data());
#else
  std::cerr << "Platform not supported" << std::endl;
  return -1;
#endif
}

};  // namespace

TEST(FallbackPlasmaAllocatorTest, FallbackPassThroughTest) {
  auto plasma_directory = CreateTestDir();
  auto fallback_directory = CreateTestDir();
  int64_t kLimit = 256 * sizeof(size_t) + 2 * kMB;
  int64_t object_size = 900 * 1024;
  PlasmaAllocator allocator(plasma_directory,
                            fallback_directory,
                            /* hugepage_enabled */ false,
                            kLimit);

  EXPECT_EQ(kLimit, allocator.GetFootprintLimit());

  {
    auto allocation_1 = allocator.Allocate(object_size);
    EXPECT_TRUE(allocation_1.has_value());
    EXPECT_FALSE(allocation_1->fallback_allocated);

    auto allocation_2 = allocator.Allocate(object_size);
    EXPECT_TRUE(allocation_2.has_value());
    EXPECT_FALSE(allocation_2->fallback_allocated);

    EXPECT_EQ(2 * object_size, allocator.Allocated());

    allocator.Free(std::move(allocation_1.value()));
    auto allocation_3 = allocator.Allocate(object_size);
    EXPECT_TRUE(allocation_3.has_value());
    EXPECT_EQ(0, allocator.FallbackAllocated());
    EXPECT_EQ(2 * object_size, allocator.Allocated());

    allocator.Free(std::move(allocation_2.value()));
    allocator.Free(std::move(allocation_3.value()));
    EXPECT_EQ(0, allocator.Allocated());
  }

  int64_t expect_allocated = 0;
  int64_t expect_fallback_allocated = 0;
  std::vector<Allocation> allocations;
  std::vector<Allocation> fallback_allocations;
  for (int i = 0; i < 2; i++) {
    auto allocation = allocator.Allocate(kMB);
    expect_allocated += kMB;
    EXPECT_TRUE(allocation.has_value());
    EXPECT_FALSE(allocation->fallback_allocated);
    EXPECT_EQ(expect_allocated, allocator.Allocated());
    EXPECT_EQ(0, allocator.FallbackAllocated());
    allocations.push_back(std::move(allocation.value()));
  }

  // over allocation yields failure.
  {
    auto allocation = allocator.Allocate(kMB);
    // allocation failure.
    EXPECT_FALSE(allocation.has_value());
    EXPECT_EQ(0, allocator.FallbackAllocated());
    EXPECT_EQ(expect_allocated, allocator.Allocated());
  }

  // fallback allocation succeeds when fallback allocation enabled.
  {
    for (int i = 0; i < 2; i++) {
      auto allocation = allocator.FallbackAllocate(kMB);
      expect_allocated += kMB;
      expect_fallback_allocated += kMB;
      EXPECT_TRUE(allocation.has_value());
      EXPECT_TRUE(allocation->fallback_allocated);
      EXPECT_EQ(expect_allocated, allocator.Allocated());
      EXPECT_EQ(expect_fallback_allocated, allocator.FallbackAllocated());
      fallback_allocations.push_back(std::move(allocation.value()));
    }
  }

  {
    // free up 1 fallback allocation.
    auto allocation = std::move(fallback_allocations.back());
    fallback_allocations.pop_back();
    allocator.Free(std::move(allocation));
    EXPECT_EQ(3 * kMB, allocator.Allocated());
    EXPECT_EQ(1 * kMB, allocator.FallbackAllocated());
  }

  {
    // free up 1 allocation from primary mmap.
    auto allocation = std::move(allocations.back());
    allocations.pop_back();
    allocator.Free(std::move(allocation));
    EXPECT_EQ(2 * kMB, allocator.Allocated());
    EXPECT_EQ(1 * kMB, allocator.FallbackAllocated());

    // now we can allocate from primary.
    auto new_allocation = allocator.Allocate(kMB);
    EXPECT_TRUE(new_allocation.has_value());
    allocations.push_back(std::move(new_allocation.value()));
    EXPECT_EQ(3 * kMB, allocator.Allocated());
    EXPECT_EQ(1 * kMB, allocator.FallbackAllocated());
  }
  // clean up
  RAY_LOG(INFO) << "cleaning up. stats: allocated " << allocator.Allocated()
                << ", fallback allocated " << allocator.FallbackAllocated()
                << ", allocations.size() == " << allocations.size()
                << ", fallback_allocations.size() == " << fallback_allocations.size();
  for (auto &allocation : allocations) {
    allocator.Free(std::move(allocation));
  }
  allocations.clear();
  for (auto &allocation : fallback_allocations) {
    allocator.Free(std::move(allocation));
  }
  fallback_allocations.clear();

  EXPECT_EQ(0, allocator.Allocated());
  EXPECT_EQ(0, allocator.FallbackAllocated());
}

TEST(FallbackPlasmaAllocatorTest, FallbackFilesAreClosedAfterFree) {
  auto plasma_directory = CreateTestDir();
  auto fallback_directory = CreateTestDir();
  int64_t kLimit = 256 * sizeof(size_t) + 2 * kMB;
  PlasmaAllocator allocator(plasma_directory,
                            fallback_directory,
                            /* hugepage_enabled */ false,
                            kLimit);

  EXPECT_EQ(kLimit, allocator.GetFootprintLimit());

  std::vector<Allocation> allocations;
  std::vector<Allocation> fallback_allocations;

  // First fill up the allocator up to kLimit.
  for (int i = 0; i < 2; i++) {
    auto allocation = allocator.Allocate(kMB);
    EXPECT_TRUE(allocation.has_value());
    EXPECT_FALSE(allocation->fallback_allocated);
    allocations.push_back(std::move(allocation.value()));
  }

  EXPECT_EQ(1, getNumberOfMappedFiles()) << "expect 1 mmaped file = 1 major one";

  // Fallback-allocates 2 blocks
  for (int i = 0; i < 2; i++) {
    auto allocation = allocator.FallbackAllocate(kMB);
    EXPECT_TRUE(allocation.has_value());
    EXPECT_TRUE(allocation->fallback_allocated);
    fallback_allocations.push_back(std::move(allocation.value()));
  }
  EXPECT_EQ(3, getNumberOfMappedFiles())
      << "expect 3 mmaped file = 1 major one and 2 fallback allocations";

  // clean up
  for (auto &allocation : allocations) {
    allocator.Free(std::move(allocation));
  }
  allocations.clear();
  for (auto &allocation : fallback_allocations) {
    allocator.Free(std::move(allocation));
  }
  fallback_allocations.clear();
  EXPECT_EQ(1, getNumberOfMappedFiles()) << "expect 1 mmaped file = 1 major one";
}

}  // namespace plasma

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
