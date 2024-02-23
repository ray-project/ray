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

#include <limits>

#include "absl/random/random.h"
#include "absl/strings/str_format.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/object_manager/common.h"

using namespace ray;
using namespace testing;

namespace {

void Write(uint8_t *object_ptr, int num_writes, int num_readers) {
  PlasmaObjectHeader *header = reinterpret_cast<ray::PlasmaObjectHeader *>(object_ptr);
  for (int i = 0; i < num_writes; i++) {
    std::string data = "hello" + std::to_string(i);
    std::string metadata = std::to_string(data.length());
    if (!header->WriteAcquire(data.length(), metadata.length(), num_readers).ok()) {
      return;
    }
    auto data_ptr = object_ptr + sizeof(PlasmaObjectHeader);
    std::memcpy(data_ptr, data.data(), data.length());
    std::memcpy(data_ptr + data.length(), metadata.data(), metadata.length());
    if (!header->WriteRelease().ok()) {
      return;
    }
  }
}

void Read(uint8_t *object_ptr,
          int num_reads,
          std::shared_ptr<std::vector<std::string>> data_results,
          std::shared_ptr<std::vector<std::string>> metadata_results) {
  PlasmaObjectHeader *header = reinterpret_cast<ray::PlasmaObjectHeader *>(object_ptr);
  auto data_ptr = object_ptr + sizeof(PlasmaObjectHeader);

  int64_t version_to_read = 1;
  for (int i = 0; i < num_reads; i++) {
    int64_t version_read = 0;
    auto status = header->ReadAcquire(version_to_read, &version_read);
    if (!status.ok()) {
      data_results->push_back("error");
      metadata_results->push_back("error");
      return;
    }

    RAY_CHECK(version_read == version_to_read);

    const char *data_str = reinterpret_cast<const char *>(data_ptr);
    data_results->push_back(std::string(data_str, header->data_size));

    uint8_t *metadata_ptr = data_ptr + header->data_size;
    const char *metadata_str = reinterpret_cast<const char *>(metadata_ptr);
    metadata_results->push_back(std::string(metadata_str, header->metadata_size));

    if (!header->ReadRelease(version_read).ok()) {
      data_results->push_back("error");
      metadata_results->push_back("error");
      return;
    }
    version_to_read++;
  }
}

}  // namespace

TEST(MutableObjectTest, TestBasic) {
  uint8_t *object_ptr =
      reinterpret_cast<uint8_t *>(malloc(sizeof(PlasmaObjectHeader) + 100));

  PlasmaObjectHeader *header = reinterpret_cast<ray::PlasmaObjectHeader *>(object_ptr);
  header->Init();

  auto data_results = std::make_shared<std::vector<std::string>>();
  auto metadata_results = std::make_shared<std::vector<std::string>>();
  int num_reads = 10000;
  std::thread t(Read, object_ptr, num_reads, data_results, metadata_results);

  for (int i = 0; i < num_reads; i++) {
    std::string data = "hello" + std::to_string(i);
    std::string metadata = std::to_string(data.length());
    RAY_CHECK_OK(header->WriteAcquire(data.length(), metadata.length(), 1));
    auto data_ptr = object_ptr + sizeof(PlasmaObjectHeader);
    std::memcpy(data_ptr, data.data(), data.length());
    std::memcpy(data_ptr + data.length(), metadata.data(), metadata.length());
    RAY_CHECK_OK(header->WriteRelease());
  }

  t.join();

  for (int i = 0; i < num_reads; i++) {
    std::string data("hello" + std::to_string(i));
    ASSERT_EQ((*data_results)[i], data);
    ASSERT_EQ((*metadata_results)[i], std::to_string(data.length()));
  }
}

TEST(MutableObjectTest, TestMultipleReaders) {
  uint8_t *object_ptr =
      reinterpret_cast<uint8_t *>(malloc(sizeof(PlasmaObjectHeader) + 100));

  PlasmaObjectHeader *header = reinterpret_cast<ray::PlasmaObjectHeader *>(object_ptr);
  header->Init();

  int num_reads = 10000;
  int num_readers = 24;

  std::vector<std::shared_ptr<std::vector<std::string>>> data_results;
  std::vector<std::shared_ptr<std::vector<std::string>>> metadata_results;
  std::vector<std::thread> threads;
  for (int i = 0; i < num_readers; i++) {
    data_results.push_back(std::make_shared<std::vector<std::string>>());
    metadata_results.push_back(std::make_shared<std::vector<std::string>>());
    std::thread t(Read, object_ptr, num_reads, data_results[i], metadata_results[i]);
    threads.emplace_back(std::move(t));
  }

  for (int i = 0; i < num_reads; i++) {
    std::string data = "hello" + std::to_string(i);
    std::string metadata = std::to_string(data.length());
    RAY_CHECK_OK(header->WriteAcquire(data.length(), metadata.length(), num_readers));
    auto data_ptr = object_ptr + sizeof(PlasmaObjectHeader);
    std::memcpy(data_ptr, data.data(), data.length());
    std::memcpy(data_ptr + data.length(), metadata.data(), metadata.length());
    RAY_CHECK_OK(header->WriteRelease());
  }

  for (auto &t : threads) {
    t.join();
  }

  for (int j = 0; j < num_reads; j++) {
    std::string data("hello" + std::to_string(j));
    for (int i = 0; i < num_readers; i++) {
      ASSERT_EQ((*data_results[i])[j], data);
      ASSERT_EQ((*metadata_results[i])[j], std::to_string(data.length()));
    }
  }
}

TEST(MutableObjectTest, TestWriterFails) {
  uint8_t *object_ptr =
      reinterpret_cast<uint8_t *>(malloc(sizeof(PlasmaObjectHeader) + 100));

  PlasmaObjectHeader *header = reinterpret_cast<ray::PlasmaObjectHeader *>(object_ptr);
  header->Init();

  int num_reads = 10000;
  int num_reads_success = num_reads / 2;
  int num_readers = 24;

  std::vector<std::shared_ptr<std::vector<std::string>>> data_results;
  std::vector<std::shared_ptr<std::vector<std::string>>> metadata_results;
  std::vector<std::thread> threads;
  for (int i = 0; i < num_readers; i++) {
    data_results.push_back(std::make_shared<std::vector<std::string>>());
    metadata_results.push_back(std::make_shared<std::vector<std::string>>());
    std::thread t(Read, object_ptr, num_reads, data_results[i], metadata_results[i]);
    threads.emplace_back(std::move(t));
  }

  std::thread writer_t(Write, object_ptr, num_reads_success, num_readers);
  writer_t.join();

  RAY_CHECK_OK(header->WriteAcquire(10, 10, num_readers));

  header->SetErrorUnlocked();

  for (auto &t : threads) {
    t.join();
  }

  for (int i = 0; i < num_readers; i++) {
    ASSERT_EQ(data_results[i]->size(), num_reads_success + 1);
  }
  for (int i = 1; i < num_readers; i++) {
    for (int j = 0; j < static_cast<int>(data_results[i]->size()); j++) {
      std::string data("hello" + std::to_string(j));
      if (j == static_cast<int>(data_results[i]->size()) - 1) {
        ASSERT_EQ((*data_results[i])[j], "error");
        ASSERT_EQ((*metadata_results[i])[j], "error");
      } else {
        ASSERT_EQ((*data_results[i])[j], data);
        ASSERT_EQ((*metadata_results[i])[j], std::to_string(data.size()));
      }
    }
  }
}

TEST(MutableObjectTest, TestWriterFailsAfterAcquire) {
  uint8_t *object_ptr =
      reinterpret_cast<uint8_t *>(malloc(sizeof(PlasmaObjectHeader) + 100));

  PlasmaObjectHeader *header = reinterpret_cast<ray::PlasmaObjectHeader *>(object_ptr);
  header->Init();

  int num_reads = 10000;
  int num_reads_success = num_reads / 2;
  int num_readers = 24;

  std::vector<std::shared_ptr<std::vector<std::string>>> data_results;
  std::vector<std::shared_ptr<std::vector<std::string>>> metadata_results;
  std::vector<std::thread> threads;
  for (int i = 0; i < num_readers; i++) {
    data_results.push_back(std::make_shared<std::vector<std::string>>());
    metadata_results.push_back(std::make_shared<std::vector<std::string>>());
    std::thread t(Read, object_ptr, num_reads, data_results[i], metadata_results[i]);
    threads.emplace_back(std::move(t));
  }

  std::thread writer_t(Write, object_ptr, num_reads_success, num_readers);
  writer_t.join();

  RAY_CHECK_OK(header->WriteAcquire(10, 10, num_readers));

  header->SetErrorUnlocked();

  for (auto &t : threads) {
    t.join();
  }

  for (int i = 0; i < num_readers; i++) {
    ASSERT_EQ(data_results[i]->size(), num_reads_success + 1);
  }
  for (int i = 1; i < num_readers; i++) {
    for (int j = 0; j < static_cast<int>(data_results[i]->size()); j++) {
      std::string data("hello" + std::to_string(j));
      if (j == static_cast<int>(data_results[i]->size()) - 1) {
        ASSERT_EQ((*data_results[i])[j], "error");
        ASSERT_EQ((*metadata_results[i])[j], "error");
      } else {
        ASSERT_EQ((*data_results[i])[j], data);
        ASSERT_EQ((*metadata_results[i])[j], std::to_string(data.size()));
      }
    }
  }
}

TEST(MutableObjectTest, TestReaderFails) {
  uint8_t *object_ptr =
      reinterpret_cast<uint8_t *>(malloc(sizeof(PlasmaObjectHeader) + 100));

  PlasmaObjectHeader *header = reinterpret_cast<ray::PlasmaObjectHeader *>(object_ptr);
  header->Init();

  int num_reads = 10000;
  int num_reads_success = num_reads / 2;
  int num_readers = 24;

  std::vector<std::shared_ptr<std::vector<std::string>>> data_results;
  std::vector<std::shared_ptr<std::vector<std::string>>> metadata_results;
  std::vector<std::thread> threads;

  data_results.push_back(std::make_shared<std::vector<std::string>>());
  metadata_results.push_back(std::make_shared<std::vector<std::string>>());
  std::thread failed_reader_t(
      Read, object_ptr, num_reads_success, data_results[0], metadata_results[0]);

  for (int i = 1; i < num_readers; i++) {
    data_results.push_back(std::make_shared<std::vector<std::string>>());
    metadata_results.push_back(std::make_shared<std::vector<std::string>>());
    std::thread t(Read, object_ptr, num_reads, data_results[i], metadata_results[i]);
    threads.emplace_back(std::move(t));
  }

  std::thread writer_t(Write, object_ptr, num_reads, num_readers);

  failed_reader_t.join();
  header->SetErrorUnlocked();

  writer_t.join();
  for (auto &t : threads) {
    t.join();
  }

  ASSERT_EQ(data_results[0]->size(), num_reads_success);
  for (int i = 1; i < num_readers; i++) {
    ASSERT_TRUE(static_cast<size_t>(num_reads_success) <= data_results[i]->size());
    ASSERT_TRUE(data_results[i]->size() <= static_cast<size_t>(num_reads_success + 2));
  }
  for (int i = 1; i < num_readers; i++) {
    for (int j = 0; j < static_cast<int>(data_results[i]->size()); j++) {
      std::string data("hello" + std::to_string(j));
      if (j == static_cast<int>(data_results[i]->size()) - 1) {
        ASSERT_EQ((*data_results[i])[j], "error");
        ASSERT_EQ((*metadata_results[i])[j], "error");
      } else {
        ASSERT_EQ((*data_results[i])[j], data);
        ASSERT_EQ((*metadata_results[i])[j], std::to_string(data.size()));
      }
    }
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
