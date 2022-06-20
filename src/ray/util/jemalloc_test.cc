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

#include <jemalloc/jemalloc.h>
#include "gtest/gtest.h"

TEST(JemallocTest, TestAllocation) {
    size_t epoch = 1;
    size_t previous_allocated = 0;
    for (auto i = 0ul; i < 10; ++i) {
        void* ptr = 0;
        ASSERT_EQ(0, posix_memalign(&ptr, 16, 100000));
        size_t sz, allocated, active, metadata, resident, mapped;
        sz = sizeof(size_t);
        mallctl("thread.tcache.flush", NULL, NULL, NULL, 0);
        mallctl("epoch", NULL, NULL, &epoch, sizeof(epoch));
        if (mallctl("stats.allocated", &allocated, &sz, NULL, 0) == 0
            && mallctl("stats.active", &active, &sz, NULL, 0) == 0
            && mallctl("stats.metadata", &metadata, &sz, NULL, 0) == 0
            && mallctl("stats.resident", &resident, &sz, NULL, 0) == 0
            && mallctl("stats.mapped", &mapped, &sz, NULL, 0) == 0) {
            if (previous_allocated != 0) {
              ASSERT_TRUE(previous_allocated + 100000 <= allocated);
            }
            previous_allocated = allocated;
            fprintf(stderr,
                "Current allocated/active/metadata/resident/mapped: %zu/%zu/%zu/%zu/%zu\n",
                allocated, active, metadata, resident, mapped);
        }
    }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
