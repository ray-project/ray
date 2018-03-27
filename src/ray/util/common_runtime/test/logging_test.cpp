#include <gtest/gtest.h>
#include <deque>
#include <iostream>
#include <list>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common_runtime.h"

TEST(LoggingTest, LogBasicTypes) {
  CrLogger logger(1);
  {  // unsigned integral
    uint16_t u16 = 54321U;
    uint32_t u32 = 987654321U;
    uint64_t u64 = 1234567890987654321U;
    logger << u16 << "\n";
    logger << u32 << "\n";
    logger << u64 << "\n";
    logger << "\n";
  }
  {  // signed integral
    int16_t i16 = -32768;
    int32_t i32 = -2147483648;
    int64_t i64 = 1234567890987654321LL;
    logger << i16 << "\n";
    logger << i32 << "\n";
    logger << i64 << "\n";
    logger << "\n";
  }
  {  // floating point
    float f = 3.14;
    double d = 3.14159;
    long double ld = -214367185288213461523.42354142654672452L;
    logger << f << "\n";
    logger << d << "\n";
    logger << ld << "\n";
    logger << "\n";
  }
  {  // ordered container
    double a[] = {1.2, 2.4, 3.6, 4.8, 6.0};
    std::vector<double> vector(std::begin(a), std::end(a));
    std::deque<double> deque(std::begin(a), std::end(a));
    std::list<double> list(std::begin(a), std::end(a));
    logger << vector << "\n";
    logger << vector << "\n";
    logger << vector << "\n";
    logger << "\n";
  }
  {  // set
    int64_t a[] = {1, 2, 3, 1, 2, 3, 0x0efefefefefefefe};
    std::set<int64_t> set(std::begin(a), std::end(a));
    std::multiset<int64_t> multiset(std::begin(a), std::end(a));
    std::unordered_set<int64_t> unordered_set(std::begin(a), std::end(a));
    std::unordered_multiset<int64_t> unordered_multiset(std::begin(a), std::end(a));
    logger << set << "\n";
    logger << multiset << "\n";
    logger << unordered_set << "\n";
    logger << unordered_multiset << "\n";
    logger << "\n";
  }
  {  // map
    std::vector<std::pair<int, std::string>> init = {
        {1, "hello"}, {2, "world"}, {2, "everyone"}};
    std::map<int, std::string> map(std::begin(init), std::end(init));
    std::multimap<int, std::string> multimap(std::begin(init), std::end(init));
    std::unordered_map<int, std::string> unordered_map(std::begin(init), std::end(init));
    std::unordered_multimap<int, std::string> unordered_multimap(std::begin(init),
                                                                 std::end(init));
    logger << map << "\n";
    logger << multimap << "\n";
    logger << unordered_map << "\n";
    logger << unordered_multimap << "\n";
    logger << "\n";
  }
}