#include "src/ray/util/shared_lru.h"

#include <gtest/gtest.h>

#include <string>

namespace ray::utils::container {

namespace {
constexpr size_t kTestCacheSz = 1;
}  // namespace

TEST(SharedLruCache, PutAndGet) {
  SharedLruCache<std::string, std::string> cache{kTestCacheSz};

  // No value initially.
  auto val = cache.Get("1");
  EXPECT_FALSE(val.has_value());

  // Check put and get.
  cache.Put("1", "1");
  val = cache.Get("1");
  EXPECT_TRUE(val.has_value());
  EXPECT_EQ(*val, "1");

  // Check key eviction.
  cache.Put("2", "2");
  val = cache.Get("1");
  EXPECT_FALSE(val.has_value());
  val = cache.Get("2");
  EXPECT_TRUE(val.has_value());
  EXPECT_EQ(*val, "2");

  // Check deletion.
  EXPECT_FALSE(cache.Delete("1"));
  EXPECT_TRUE(cache.Delete("2"));
  val = cache.Get("2");
  EXPECT_FALSE(val.has_value());
}

}  // namespace ray::utils::container
