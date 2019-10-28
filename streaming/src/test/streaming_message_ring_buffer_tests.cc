#include "gtest/gtest.h"
#include "ray/util/logging.h"

#include <unistd.h>
#include <iostream>
#include <set>
#include <thread>
#include "streaming.h"
#include "streaming_message.h"
#include "streaming_ring_buffer.h"
#include "test/test_utils.h"

using namespace ray;
using namespace ray::streaming;

size_t data_n = 1000000;
TEST(StreamingRingBufferTest, streaming_message_ring_buffer_test) {
  for (int k = 0; k < 10000; ++k) {
    StreamingRingBuffer r_buf(3, StreamingRingBufferType::SPSC_LOCK);
    for (int i = 0; i < 5; ++i) {
      uint8_t data[] = {1, 1, 3};
      data[0] = i;
      StreamingMessagePtr message =
          util::MakeMessagePtr(data, 3, i, StreamingMessageType::Message);
      EXPECT_EQ(r_buf.Push(message), true);
      size_t ith = i >= 3 ? 3 : (i + 1);
      EXPECT_EQ(r_buf.Size(), ith);
    }
    int th = 2;

    while (!r_buf.IsEmpty()) {
      StreamingMessagePtr messagePtr = r_buf.Front();
      r_buf.Pop();
      EXPECT_EQ(messagePtr->GetDataSize(), 3);
      EXPECT_EQ(*(messagePtr->RawData()), th++);
    }
  }
}

TEST(StreamingRingBufferTest, spsc_test) {
  size_t m_num = 1000;
  StreamingRingBuffer r_buf(m_num, StreamingRingBufferType::SPSC);
  std::thread thread([&r_buf]() {
    for (size_t j = 0; j < data_n; ++j) {
      StreamingMessagePtr message =
          util::MakeMessagePtr(reinterpret_cast<uint8_t *>(&j), sizeof(size_t), j,
                               StreamingMessageType::Message);
      while (r_buf.IsFull()) {
      }
      r_buf.Push(message);
    }
  });
  size_t count = 0;
  while (count < data_n) {
    while (r_buf.IsEmpty()) {
    }
    auto &msg = r_buf.Front();
    EXPECT_EQ(std::memcmp(msg->RawData(), &count, sizeof(size_t)), 0);
    r_buf.Pop();
    count++;
  }
  thread.join();
  EXPECT_EQ(count, data_n);
}

TEST(StreamingRingBufferTest, mutex_test) {
  size_t m_num = data_n;
  StreamingRingBuffer r_buf(m_num, StreamingRingBufferType::SPSC_LOCK);
  std::thread thread([&r_buf]() {
    for (size_t j = 0; j < data_n; ++j) {
      StreamingMessagePtr message =
          util::MakeMessagePtr(reinterpret_cast<uint8_t *>(&j), sizeof(size_t), j,
                               StreamingMessageType::Message);
      while (r_buf.IsFull()) {
      }
      r_buf.Push(message);
    }
  });
  size_t count = 0;
  while (count < data_n) {
    while (r_buf.IsEmpty()) {
    }
    auto msg = r_buf.Front();
    EXPECT_EQ(std::memcmp(msg->RawData(), &count, sizeof(size_t)), 0);
    r_buf.Pop();
    count++;
  }
  thread.join();
  EXPECT_EQ(count, data_n);
}

TEST(StreamingRingBufferTest, spsc_list_test) {
  size_t m_num = 1000 * sizeof(size_t);
  StreamingRingBuffer r_buf(m_num, StreamingRingBufferType::SPSC_LIST);
  std::thread thread([&r_buf]() {
    for (size_t j = 0; j < data_n; ++j) {
      StreamingMessagePtr message =
          util::MakeMessagePtr(reinterpret_cast<uint8_t *>(&j), sizeof(size_t), j,
                               StreamingMessageType::Message);
      while (r_buf.IsFull()) {
      }
      r_buf.Push(message);
    }
  });
  size_t count = 0;
  while (count < data_n) {
    while (r_buf.IsEmpty()) {
    }
    auto &msg = r_buf.Front();
    EXPECT_EQ(std::memcmp(msg->RawData(), &count, sizeof(size_t)), 0);
    r_buf.Pop();
    count++;
  }
  thread.join();
  EXPECT_EQ(count, data_n);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
