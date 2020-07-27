#include <iostream>
#include <set>
#include <thread>

#include "gtest/gtest.h"
#include "message/message.h"
#include "ray/util/logging.h"
#include "ring_buffer.h"

using namespace ray;
using namespace ray::streaming;

size_t data_n = 1000000;
TEST(StreamingRingBufferTest, streaming_message_ring_buffer_test) {
  for (int k = 0; k < 10000; ++k) {
    StreamingRingBuffer ring_buffer(3, StreamingRingBufferType::SPSC_LOCK);
    for (int i = 0; i < 5; ++i) {
      uint8_t data[] = {1, 1, 3};
      data[0] = i;
      StreamingMessagePtr message =
          std::make_shared<StreamingMessage>(data, 3, i, StreamingMessageType::Message);
      EXPECT_EQ(ring_buffer.Push(message), true);
      size_t ith = i >= 3 ? 3 : (i + 1);
      EXPECT_EQ(ring_buffer.Size(), ith);
    }
    int th = 2;

    while (!ring_buffer.IsEmpty()) {
      StreamingMessagePtr message_ptr = ring_buffer.Front();
      ring_buffer.Pop();
      EXPECT_EQ(message_ptr->GetDataSize(), 3);
      EXPECT_EQ(*(message_ptr->RawData()), th++);
    }
  }
}

TEST(StreamingRingBufferTest, spsc_test) {
  size_t m_num = 1000;
  StreamingRingBuffer ring_buffer(m_num, StreamingRingBufferType::SPSC);
  std::thread thread([&ring_buffer]() {
    for (size_t j = 0; j < data_n; ++j) {
      StreamingMessagePtr message = std::make_shared<StreamingMessage>(
          reinterpret_cast<uint8_t *>(&j), static_cast<uint32_t>(sizeof(size_t)), j,
          StreamingMessageType::Message);
      while (ring_buffer.IsFull()) {
      }
      ring_buffer.Push(message);
    }
  });
  size_t count = 0;
  while (count < data_n) {
    while (ring_buffer.IsEmpty()) {
    }
    auto &msg = ring_buffer.Front();
    EXPECT_EQ(std::memcmp(msg->RawData(), &count, sizeof(size_t)), 0);
    ring_buffer.Pop();
    count++;
  }
  thread.join();
  EXPECT_EQ(count, data_n);
}

TEST(StreamingRingBufferTest, mutex_test) {
  size_t m_num = data_n;
  StreamingRingBuffer ring_buffer(m_num, StreamingRingBufferType::SPSC_LOCK);
  std::thread thread([&ring_buffer]() {
    for (size_t j = 0; j < data_n; ++j) {
      StreamingMessagePtr message = std::make_shared<StreamingMessage>(
          reinterpret_cast<uint8_t *>(&j), static_cast<uint32_t>(sizeof(size_t)), j,
          StreamingMessageType::Message);
      while (ring_buffer.IsFull()) {
      }
      ring_buffer.Push(message);
    }
  });
  size_t count = 0;
  while (count < data_n) {
    while (ring_buffer.IsEmpty()) {
    }
    auto msg = ring_buffer.Front();
    EXPECT_EQ(std::memcmp(msg->RawData(), &count, sizeof(size_t)), 0);
    ring_buffer.Pop();
    count++;
  }
  thread.join();
  EXPECT_EQ(count, data_n);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
