#include "buffer_pool.h"
#include "gtest/gtest.h"

using namespace ray::streaming;

TEST(BUFFER_POOL_TEST, GetBufferTest) {
  uint64_t min_buffer_size = 1024 * 1024;  // 1M
  int buffer_nums = 5;
  uint64_t pool_size = buffer_nums * min_buffer_size;
  BufferPool pool(pool_size, min_buffer_size);
  for (int i = 0; i < buffer_nums; ++i) {
    ray::streaming::Buffer buffer = {};
    ASSERT_EQ(pool.GetBuffer(&buffer), StreamingStatus::OK);
    EXPECT_EQ(buffer.Size(), min_buffer_size);
    std::cout << "address: " << static_cast<void *>(buffer.Data()) << std::endl;
    ASSERT_EQ(pool.MarkUsed(buffer.Data(), buffer.Size()), StreamingStatus::OK);
  }
}

TEST(BUFFER_POOL_TEST, ReleaseBuffer) {
  uint64_t min_buffer_size = 1024 * 1024;  // 1M
  int buffer_nums = 5;
  uint64_t pool_size = buffer_nums * min_buffer_size;
  BufferPool pool(pool_size, min_buffer_size);
  std::vector<ray::streaming::Buffer> buffers;
  for (int i = 0; i < buffer_nums; ++i) {
    ray::streaming::Buffer buffer = {};
    ASSERT_EQ(pool.GetBuffer(&buffer), StreamingStatus::OK);
    EXPECT_EQ(buffer.Size(), min_buffer_size);
    std::cout << "buffer start: " << reinterpret_cast<uint64_t>(buffer.Data())
              << std::endl;
    ASSERT_EQ(pool.MarkUsed(buffer.Data(), buffer.Size()), StreamingStatus::OK);
    buffers.push_back(buffer);
  }
  for (auto &buffer : buffers) {
    // std::cout << "usage: " << pool.PrintUsage() << std::endl;
    ASSERT_EQ(pool.Release(buffer.Data(), buffer.Size()), StreamingStatus::OK);
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
