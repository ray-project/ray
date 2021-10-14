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

#include "ray/util/short_alloc.h"
#include <gtest/gtest.h>
#include <chrono>
#include <sstream>
#include <vector>

std::size_t g_total_heap_alloc_size = 0;
std::size_t g_alloc_counter = 0;

void *operator new(std::size_t s) {
  g_total_heap_alloc_size += s;
  ++g_alloc_counter;
  return malloc(s);
}

void operator delete(void *p) throw() {
  --g_alloc_counter;
  free(p);
}

void Reset() {
  g_total_heap_alloc_size = 0;
  g_alloc_counter = 0;
}

void NoHeapMemoryUsed() {
  std::cout << g_total_heap_alloc_size << '\n';
  std::cout << g_alloc_counter << '\n';
  EXPECT_EQ(g_alloc_counter, 0);
  EXPECT_EQ(g_total_heap_alloc_size, 0);
}

void HeapMemoryUsed() {
  std::cout << g_total_heap_alloc_size << '\n';
  std::cout << g_alloc_counter << '\n';
  EXPECT_TRUE(g_alloc_counter > 0);
  EXPECT_TRUE(g_total_heap_alloc_size > 0);
}

using short_string =
    std::basic_string<char, std::char_traits<char>, short_alloc<char, 1024>>;

template <class T, std::size_t BufSize = 200>
using SmallVector = std::vector<T, short_alloc<T, BufSize, alignof(T)>>;

TEST(BasicTest, BasicTest) {
  Reset();
  NoHeapMemoryUsed();
  // Create the stack-based arena from which to allocate
  SmallVector<int>::allocator_type::arena_type a;
  // Create the vector which uses that arena.
  SmallVector<int> v{a};
  // Exercise the vector and note that new/delete are not getting called.
  v.push_back(1);
  NoHeapMemoryUsed();
  v.push_back(2);
  NoHeapMemoryUsed();

  short_string::allocator_type::arena_type arena;
  short_string str(arena);

  str = "hello";
  NoHeapMemoryUsed();
  EXPECT_TRUE(str == "hello");
  str.append(" world");
  NoHeapMemoryUsed();
}

TEST(BasicTest, HeapAlloc) {
  Reset();
  NoHeapMemoryUsed();

  // If the stack memory unavaliable, will use heap memory.
  SmallVector<int>::allocator_type::arena_type a;
  SmallVector<int> v{a};
  for (int i = 0; i < 50; ++i) {
    v.push_back(i);
  }

  HeapMemoryUsed();
}

class ScopedTimer {
 public:
  ScopedTimer(const char *name)
      : m_name(name), m_beg(std::chrono::high_resolution_clock::now()) {}
  ~ScopedTimer() {
    auto end = std::chrono::high_resolution_clock::now();
    auto dur = std::chrono::duration_cast<std::chrono::nanoseconds>(end - m_beg);
    std::cout << m_name << " : " << dur.count() << " ns\n";
  }

 private:
  const char *m_name;
  std::chrono::time_point<std::chrono::high_resolution_clock> m_beg;
};

// short_string will be 10x faster on linux.
TEST(PefTest, PerfTest) {
  const int rounds = 1000000;

  for (int i = 0; i < 5; ++i) {
    {
      ScopedTimer timer("std::ostringstream test");
      for (int i = 0; i < rounds; ++i) {
        std::ostringstream str;
        str << "hello"
            << " it is a long string."
            << " it is a long string.";
      }
    }

    {
      short_string::allocator_type::arena_type arena;
      ScopedTimer timer("short_alloc string test");
      for (int i = 0; i < rounds; ++i) {
        short_string str(arena);
        str.append("hello")
            .append(" it is a long string.")
            .append(" it is a long string.");
      }
    }
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  auto result = RUN_ALL_TESTS();
  return result;
}