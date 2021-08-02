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

#include <boost/filesystem.hpp>
#include "gtest/gtest.h"
#include "ray/object_manager/plasma/plasma_allocator.h"

using namespace boost::filesystem;

namespace plasma {
namespace {
const int64_t kMB = 1024 * 1024;
std::string CreateTestDir() {
  path directory = temp_directory_path() / unique_path();
  create_directories(directory);
  return directory.string();
}
};  // namespace

TEST(PlasmaAllocatorTest, NoFallbackPassThroughTest) {
  auto plasma_directory = CreateTestDir();
  auto fallback_directory = CreateTestDir();
  int64_t kLimit = 256 * sizeof(size_t) + 10 * kMB;
  PlasmaAllocator allocator(plasma_directory, fallback_directory,
                            /* hugepage_enabled */ false, kLimit,
                            /* fallback_enabled */ false);

  EXPECT_EQ(kLimit, allocator.GetFootprintLimit());

  int64_t expect_allocated = 0;
  std::vector<Allocation> allocations;
  for (int i = 0; i < 10; i++) {
    auto allocation = allocator.Allocate(kMB);
    expect_allocated += kMB;
    EXPECT_TRUE(allocation.has_value());
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

  // fallback allocation fails when fallback allocation disabled
  {
    auto allocation = allocator.FallbackAllocate(kMB);
    // allocation failure.
    EXPECT_FALSE(allocation.has_value());
    EXPECT_EQ(0, allocator.FallbackAllocated());
    EXPECT_EQ(expect_allocated, allocator.Allocated());
  }

  {
    // free up space.
    auto allocation = std::move(allocations.back());
    allocations.pop_back();
    allocator.Free(std::move(allocation));
    EXPECT_EQ(9 * kMB, allocator.Allocated());

    // now that we can allocate another entry.
    auto new_allocation = allocator.Allocate(kMB);
    EXPECT_TRUE(new_allocation.has_value());
    EXPECT_EQ(10 * kMB, allocator.Allocated());
    allocations.push_back(std::move(new_allocation.value()));
  }
}
}  // namespace plasma

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
