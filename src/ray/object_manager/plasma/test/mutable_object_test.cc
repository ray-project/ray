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
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/random/random.h"
#include "absl/strings/str_format.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/core_worker/experimental_mutable_object_manager.h"
#include "ray/object_manager/common.h"

using testing::Test;

namespace ray {
namespace experimental {

#if defined(__APPLE__) || defined(__linux__)

namespace {

constexpr size_t kNumReads = 10000;
constexpr size_t kNumReadsSuccess = kNumReads / 2;
constexpr size_t kNumReaders = 24;

uint8_t *GetData(PlasmaObjectHeader *header) {
  uint8_t *raw_header = reinterpret_cast<uint8_t *>(header);
  return raw_header + sizeof(PlasmaObjectHeader);
}

uint8_t *GetMetadata(PlasmaObjectHeader *header) {
  return GetData(header) + header->data_size;
}

// Writes `num_write` times to the mutable object.
void Write(PlasmaObjectHeader *header,
           PlasmaObjectHeader::Semaphores &sem,
           size_t num_writes,
           size_t num_readers) {
  RAY_CHECK(header);
  RAY_CHECK(sem.header_sem);
  RAY_CHECK(sem.object_sem);

  for (size_t i = 0; i < num_writes; i++) {
    std::string data = absl::StrCat("hello", i);
    std::string metadata = std::to_string(data.size());
    if (!header->WriteAcquire(sem, data.size(), metadata.size(), num_readers).ok()) {
      return;
    }
    memcpy(GetData(header), data.data(), data.size());
    memcpy(GetMetadata(header), metadata.data(), metadata.size());
    if (!header->WriteRelease(sem).ok()) {
      return;
    }
  }
}

// Reads `num_reads` times to the mutable object.
void Read(PlasmaObjectHeader *header,
          PlasmaObjectHeader::Semaphores &sem,
          size_t num_reads,
          std::vector<std::string> &data_results,
          std::vector<std::string> &metadata_results) {
  RAY_CHECK(header);
  RAY_CHECK(sem.header_sem);
  RAY_CHECK(sem.object_sem);

  int64_t version_to_read = 1;
  for (size_t i = 0; i < num_reads; i++) {
    int64_t version_read = 0;
    if (!header
             ->ReadAcquire(
                 ObjectID::FromRandom(), sem, version_to_read, version_read, nullptr)
             .ok()) {
      data_results.push_back("error");
      metadata_results.push_back("error");
      return;
    }
    RAY_CHECK_EQ(version_read, version_to_read);

    const char *data_str = reinterpret_cast<const char *>(GetData(header));
    data_results.push_back(std::string(data_str, header->data_size));

    const char *metadata_str = reinterpret_cast<const char *>(GetMetadata(header));
    metadata_results.push_back(std::string(metadata_str, header->metadata_size));

    if (!header->ReadRelease(sem, version_read).ok()) {
      data_results.push_back("error");
      metadata_results.push_back("error");
      return;
    }
    version_to_read++;
  }
}

// Creates a new mutable object. It is the caller's responsibility to free the backing
// store.
std::unique_ptr<plasma::MutableObject> MakeObject() {
  constexpr size_t kPayloadSize = 128;
  constexpr size_t kSize = sizeof(PlasmaObjectHeader) + kPayloadSize;

  plasma::PlasmaObject info{};
  info.header_offset = 0;
  info.data_offset = sizeof(PlasmaObjectHeader);
  info.allocated_size = kPayloadSize;

  uint8_t *ptr = static_cast<uint8_t *>(malloc(kSize));
  RAY_CHECK(ptr);
  auto ret = std::make_unique<plasma::MutableObject>(ptr, info);
  ret->header->Init();
  return ret;
}

}  // namespace

// Tests that a single reader can read from a single writer.
TEST(MutableObjectTest, TestBasic) {
  MutableObjectManager manager;
  ObjectID object_id = ObjectID::FromRandom();
  plasma::PlasmaObjectHeader *header;
  {
    std::unique_ptr<plasma::MutableObject> object = MakeObject();
    header = object->header;
    header->Init();
    ASSERT_TRUE(
        manager.RegisterChannel(object_id, std::move(object), /*reader=*/true).ok());
  }
  manager.OpenSemaphores(object_id, header);
  PlasmaObjectHeader::Semaphores sem;
  ASSERT_TRUE(manager.GetSemaphores(object_id, sem));

  std::vector<std::string> data_results;
  std::vector<std::string> metadata_results;
  std::thread t(Read,
                header,
                std::ref(sem),
                kNumReads,
                std::ref(data_results),
                std::ref(metadata_results));

  for (size_t i = 0; i < kNumReads; i++) {
    std::string data = absl::StrCat("hello", i);
    std::string metadata = std::to_string(data.size());

    ASSERT_TRUE(header->WriteAcquire(sem, data.size(), metadata.size(), 1).ok());
    uint8_t *data_ptr = GetData(header);
    std::memcpy(data_ptr, data.data(), data.size());
    std::memcpy(data_ptr + data.size(), metadata.data(), metadata.size());
    ASSERT_TRUE(header->WriteRelease(sem).ok());
  }

  t.join();
  manager.DestroySemaphores(object_id);
  free(header);

  for (size_t i = 0; i < kNumReads; i++) {
    std::string data = absl::StrCat("hello", i);
    ASSERT_EQ(data_results[i], data);
    ASSERT_EQ(metadata_results[i], std::to_string(data.size()));
  }
}

// Tests that multiple readers can read from a single writer.
TEST(MutableObjectTest, TestMultipleReaders) {
  MutableObjectManager manager;
  ObjectID object_id = ObjectID::FromRandom();
  plasma::PlasmaObjectHeader *header;
  {
    std::unique_ptr<plasma::MutableObject> object = MakeObject();
    header = object->header;
    header->Init();
    ASSERT_TRUE(
        manager.RegisterChannel(object_id, std::move(object), /*reader=*/true).ok());
  }
  manager.OpenSemaphores(object_id, header);
  PlasmaObjectHeader::Semaphores sem;
  ASSERT_TRUE(manager.GetSemaphores(object_id, sem));

  std::vector<std::vector<std::string>> data_results(/*count=*/kNumReaders,
                                                     std::vector<std::string>());
  std::vector<std::vector<std::string>> metadata_results(/*count=*/kNumReaders,
                                                         std::vector<std::string>());
  std::vector<std::thread> threads;
  for (size_t i = 0; i < kNumReaders; i++) {
    std::thread t(Read,
                  header,
                  std::ref(sem),
                  kNumReads,
                  std::ref(data_results[i]),
                  std::ref(metadata_results[i]));
    threads.emplace_back(std::move(t));
  }

  for (size_t i = 0; i < kNumReads; i++) {
    std::string data = absl::StrCat("hello", i);
    std::string metadata = std::to_string(data.size());
    ASSERT_TRUE(
        header->WriteAcquire(sem, data.size(), metadata.size(), kNumReaders).ok());
    uint8_t *data_ptr = GetData(header);
    std::memcpy(data_ptr, data.data(), data.size());
    std::memcpy(data_ptr + data.size(), metadata.data(), metadata.size());
    ASSERT_TRUE(header->WriteRelease(sem).ok());
  }

  for (std::thread &t : threads) {
    t.join();
  }
  manager.DestroySemaphores(object_id);
  free(header);

  for (size_t j = 0; j < kNumReads; j++) {
    std::string data = absl::StrCat("hello", j);
    for (size_t i = 0; i < kNumReaders; i++) {
      ASSERT_EQ(data_results[i][j], data);
      ASSERT_EQ(metadata_results[i][j], std::to_string(data.size()));
    }
  }
}

// Tests that multiple readers can detect a failure initiated by the writer.
TEST(MutableObjectTest, TestWriterFails) {
  MutableObjectManager manager;
  ObjectID object_id = ObjectID::FromRandom();
  plasma::PlasmaObjectHeader *header;
  {
    std::unique_ptr<plasma::MutableObject> object = MakeObject();
    header = object->header;
    header->Init();
    ASSERT_TRUE(
        manager.RegisterChannel(object_id, std::move(object), /*reader=*/true).ok());
  }
  manager.OpenSemaphores(object_id, header);
  PlasmaObjectHeader::Semaphores sem;
  ASSERT_TRUE(manager.GetSemaphores(object_id, sem));

  std::vector<std::vector<std::string>> data_results(/*count=*/kNumReaders,
                                                     std::vector<std::string>());
  std::vector<std::vector<std::string>> metadata_results(/*count=*/kNumReaders,
                                                         std::vector<std::string>());
  std::vector<std::thread> threads;
  for (size_t i = 0; i < kNumReaders; i++) {
    std::thread t(Read,
                  header,
                  std::ref(sem),
                  kNumReads,
                  std::ref(data_results[i]),
                  std::ref(metadata_results[i]));
    threads.emplace_back(std::move(t));
  }

  {
    std::thread writer(
        Write, header, std::ref(sem), /*num_writes=*/kNumReadsSuccess, kNumReaders);
    writer.join();
  }

  header->SetErrorUnlocked(sem);

  for (std::thread &t : threads) {
    t.join();
  }
  manager.DestroySemaphores(object_id);
  free(header);

  for (size_t i = 0; i < kNumReaders; i++) {
    ASSERT_GE(data_results[i].size(), kNumReadsSuccess);
    ASSERT_LE(data_results[i].size(), kNumReadsSuccess + 1);
  }
  for (size_t i = 1; i < kNumReaders; i++) {
    for (size_t j = 0; j < data_results[i].size(); j++) {
      std::string data;
      std::string metadata;
      if (j + 1 == data_results[i].size()) {
        data = "error";
        metadata = "error";
      } else {
        data = absl::StrCat("hello", j);
        metadata = std::to_string(data.size());
      }
      ASSERT_EQ(data_results[i][j], data);
      ASSERT_EQ(metadata_results[i][j], metadata);
    }
  }
}

// Tests that multiple readers can detect a failure initiated by the writer after
// `WriteAcquire()` is called.
TEST(MutableObjectTest, TestWriterFailsAfterAcquire) {
  MutableObjectManager manager;
  ObjectID object_id = ObjectID::FromRandom();
  plasma::PlasmaObjectHeader *header;
  {
    std::unique_ptr<plasma::MutableObject> object = MakeObject();
    header = object->header;
    header->Init();
    ASSERT_TRUE(
        manager.RegisterChannel(object_id, std::move(object), /*reader=*/true).ok());
  }
  manager.OpenSemaphores(object_id, header);
  PlasmaObjectHeader::Semaphores sem;
  ASSERT_TRUE(manager.GetSemaphores(object_id, sem));

  std::vector<std::vector<std::string>> data_results(/*count=*/kNumReaders,
                                                     std::vector<std::string>());
  std::vector<std::vector<std::string>> metadata_results(/*count=*/kNumReaders,
                                                         std::vector<std::string>());
  std::vector<std::thread> threads;
  for (size_t i = 0; i < kNumReaders; i++) {
    std::thread t(Read,
                  header,
                  std::ref(sem),
                  kNumReads,
                  std::ref(data_results[i]),
                  std::ref(metadata_results[i]));
    threads.emplace_back(std::move(t));
  }

  {
    std::thread writer(Write, header, std::ref(sem), kNumReadsSuccess, kNumReaders);
    writer.join();
  }

  ASSERT_TRUE(
      header
          ->WriteAcquire(
              sem, /*write_data_size=*/10, /*write_metadata_size=*/10, kNumReaders)
          .ok());
  header->SetErrorUnlocked(sem);

  for (std::thread &t : threads) {
    t.join();
  }
  manager.DestroySemaphores(object_id);
  free(header);

  for (size_t i = 0; i < kNumReaders; i++) {
    ASSERT_EQ(data_results[i].size(), kNumReadsSuccess + 1);
  }
  for (size_t i = 1; i < kNumReaders; i++) {
    for (size_t j = 0; j < data_results[i].size(); j++) {
      std::string data;
      std::string metadata;
      if (j + 1 == data_results[i].size()) {
        data = "error";
        metadata = "error";
      } else {
        data = absl::StrCat("hello", j);
        metadata = std::to_string(data.size());
      }
      ASSERT_EQ(data_results[i][j], data);
      ASSERT_EQ(metadata_results[i][j], metadata);
    }
  }
}

// Tests that multiple readers can detect a failure initiated by another reader.
TEST(MutableObjectTest, TestReaderFails) {
  MutableObjectManager manager;
  ObjectID object_id = ObjectID::FromRandom();
  plasma::PlasmaObjectHeader *header;
  {
    std::unique_ptr<plasma::MutableObject> object = MakeObject();
    header = object->header;
    header->Init();
    ASSERT_TRUE(
        manager.RegisterChannel(object_id, std::move(object), /*reader=*/true).ok());
  }
  manager.OpenSemaphores(object_id, header);
  PlasmaObjectHeader::Semaphores sem;
  ASSERT_TRUE(manager.GetSemaphores(object_id, sem));

  std::vector<std::vector<std::string>> data_results(/*count=*/kNumReaders,
                                                     std::vector<std::string>());
  std::vector<std::vector<std::string>> metadata_results(/*count=*/kNumReaders,
                                                         std::vector<std::string>());
  std::vector<std::thread> threads;

  std::thread failed_reader(Read,
                            header,
                            std::ref(sem),
                            kNumReadsSuccess,
                            std::ref(data_results[0]),
                            std::ref(metadata_results[0]));

  for (size_t i = 1; i < kNumReaders; i++) {
    std::thread t(Read,
                  header,
                  std::ref(sem),
                  kNumReads,
                  std::ref(data_results[i]),
                  std::ref(metadata_results[i]));
    threads.emplace_back(std::move(t));
  }

  std::thread writer(Write, header, std::ref(sem), kNumReads, kNumReaders);

  failed_reader.join();
  header->SetErrorUnlocked(sem);

  writer.join();
  for (std::thread &t : threads) {
    t.join();
  }
  manager.DestroySemaphores(object_id);
  free(header);

  ASSERT_EQ(data_results[0].size(), kNumReadsSuccess);
  for (size_t i = 1; i < kNumReaders; i++) {
    ASSERT_LE(kNumReadsSuccess, data_results[i].size());
    ASSERT_LE(data_results[i].size(), kNumReadsSuccess * 2);
  }
  for (size_t i = 1; i < kNumReaders; i++) {
    for (size_t j = 0; j < data_results[i].size(); j++) {
      std::string data;
      std::string metadata;
      if (j + 1 == data_results[i].size()) {
        data = "error";
        metadata = "error";
      } else {
        data = absl::StrCat("hello", j);
        metadata = std::to_string(data.size());
      }
      ASSERT_EQ(data_results[i][j], data);
      ASSERT_EQ(metadata_results[i][j], metadata);
    }
  }
}

// Tests that a writer can detect a failure while it is in `WriteAcquire()`.
TEST(MutableObjectTest, TestWriteAcquireDuringFailure) {
  MutableObjectManager manager;
  ObjectID object_id = ObjectID::FromRandom();
  plasma::PlasmaObjectHeader *header;
  {
    std::unique_ptr<plasma::MutableObject> object = MakeObject();
    header = object->header;
    header->Init();
    ASSERT_TRUE(
        manager.RegisterChannel(object_id, std::move(object), /*reader=*/false).ok());
  }
  manager.OpenSemaphores(object_id, header);
  PlasmaObjectHeader::Semaphores sem;
  ASSERT_TRUE(manager.GetSemaphores(object_id, sem));

  ASSERT_EQ(sem_wait(sem.object_sem), 0);
  ASSERT_EQ(sem_wait(sem.header_sem), 0);
  std::thread writer(Write, header, std::ref(sem), /*num_writes=*/kNumReads, kNumReaders);

  // Writer thread is blocked on `sem.object_sem`.
  header->SetErrorUnlocked(sem);
  // Writer thread is now unblocked.

  writer.join();
  // Check that the writer never entered the critical section.
  ASSERT_EQ(header->version, 0);

  // Check that another writer that calls `WriteAcquire()` exits on its own without
  // blocking on `sem.header_sem`.
  writer =
      std::thread(Write, header, std::ref(sem), /*num_writes=*/kNumReads, kNumReaders);
  writer.join();
  ASSERT_EQ(header->version, 0);
}

// Tests that a reader can detect a failure while it is in `ReadAcquire()`.
TEST(MutableObjectTest, TestReadAcquireDuringFailure) {
  MutableObjectManager manager;
  ObjectID object_id = ObjectID::FromRandom();
  plasma::PlasmaObjectHeader *header;
  {
    std::unique_ptr<plasma::MutableObject> object = MakeObject();
    header = object->header;
    header->Init();
    ASSERT_TRUE(
        manager.RegisterChannel(object_id, std::move(object), /*reader=*/true).ok());
  }
  manager.OpenSemaphores(object_id, header);
  PlasmaObjectHeader::Semaphores sem;
  ASSERT_TRUE(manager.GetSemaphores(object_id, sem));

  std::vector<std::string> data_results;
  std::vector<std::string> metadata_results;
  std::thread reader(Read,
                     header,
                     std::ref(sem),
                     kNumReads,
                     std::ref(data_results),
                     std::ref(metadata_results));

  {
    std::string data = "hello";
    std::string metadata = std::to_string(data.size());
    ASSERT_TRUE(
        header->WriteAcquire(sem, data.size(), metadata.size(), /*write_num_readers=*/1)
            .ok());
    ASSERT_TRUE(header->WriteRelease(sem).ok());
  }

  // Reader thread is blocked on `sem.header_sem`.
  header->SetErrorUnlocked(sem);
  // Reader thread is now unblocked.

  reader.join();
  // Check that the reader never entered the critical section.
  ASSERT_EQ(data_results.size(), 1);
  ASSERT_EQ(metadata_results.size(), 1);
  ASSERT_EQ(data_results.front(), "error");
  ASSERT_EQ(metadata_results.front(), "error");

  // Check that another reader that calls `ReadAcquire()` exits on its own without
  // blocking on `sem.header_sem`.
  data_results.clear();
  metadata_results.clear();
  reader = std::thread(Read,
                       header,
                       std::ref(sem),
                       kNumReads,
                       std::ref(data_results),
                       std::ref(metadata_results));
  reader.join();
  ASSERT_EQ(data_results.size(), 1);
  ASSERT_EQ(metadata_results.size(), 1);
  ASSERT_EQ(data_results.front(), "error");
  ASSERT_EQ(metadata_results.front(), "error");
}

// Tests that multiple readers can detect a failure while they are in `ReadAcquire()`.
TEST(MutableObjectTest, TestReadMultipleAcquireDuringFailure) {
  MutableObjectManager manager;
  ObjectID object_id = ObjectID::FromRandom();
  plasma::PlasmaObjectHeader *header;
  {
    std::unique_ptr<plasma::MutableObject> object = MakeObject();
    header = object->header;
    header->Init();
    ASSERT_TRUE(
        manager.RegisterChannel(object_id, std::move(object), /*reader=*/true).ok());
  }
  manager.OpenSemaphores(object_id, header);
  PlasmaObjectHeader::Semaphores sem;
  ASSERT_TRUE(manager.GetSemaphores(object_id, sem));

  std::vector<std::vector<std::string>> data_results(/*count=*/kNumReaders,
                                                     std::vector<std::string>());
  std::vector<std::vector<std::string>> metadata_results(/*count=*/kNumReaders,
                                                         std::vector<std::string>());
  std::vector<std::thread> threads;
  for (size_t i = 0; i < kNumReaders; i++) {
    std::thread t(Read,
                  header,
                  std::ref(sem),
                  kNumReads,
                  std::ref(data_results[i]),
                  std::ref(metadata_results[i]));
    threads.emplace_back(std::move(t));
  }

  {
    std::string data = "hello";
    std::string metadata = std::to_string(data.size());
    ASSERT_TRUE(
        header->WriteAcquire(sem, data.size(), metadata.size(), kNumReaders).ok());
    ASSERT_TRUE(header->WriteRelease(sem).ok());
  }

  // Reader threads are blocked on `sem.header_sem`.
  header->SetErrorUnlocked(sem);
  // Reader threads are now unblocked.

  for (std::thread &t : threads) {
    t.join();
  }
  // Check that the readers never entered the critical section.
  for (size_t i = 0; i < kNumReaders; i++) {
    ASSERT_EQ(data_results[i].size(), metadata_results[i].size());
    size_t size = data_results[i].size();
    ASSERT_GE(size, 1);
    ASSERT_LE(size, 2);
    ASSERT_EQ(data_results[i].back(), "error");
    ASSERT_EQ(metadata_results[i].back(), "error");
  }

  // Check that more readers that call `ReadAcquire()` exit on their own without blocking
  // on `sem.header_sem`.
  std::fill(data_results.begin(), data_results.end(), std::vector<std::string>());
  std::fill(metadata_results.begin(), metadata_results.end(), std::vector<std::string>());
  threads.clear();
  for (size_t i = 0; i < kNumReaders; i++) {
    std::thread t(Read,
                  header,
                  std::ref(sem),
                  kNumReads,
                  std::ref(data_results[i]),
                  std::ref(metadata_results[i]));
    threads.emplace_back(std::move(t));
  }
  for (std::thread &t : threads) {
    t.join();
  }
  for (size_t i = 0; i < kNumReaders; i++) {
    ASSERT_EQ(data_results[i].size(), metadata_results[i].size());
    size_t size = data_results[i].size();
    ASSERT_GE(size, 1);
    ASSERT_LE(size, 2);
    ASSERT_EQ(data_results[i].back(), "error");
    ASSERT_EQ(metadata_results[i].back(), "error");
  }
}

// Tests that MutableObjectManager instances destruct properly when there are multiple
// instances.
// The core worker and the raylet each have their own MutableObjectManager instance, and
// when both a reader and a writer are on the same machine, the reader and writer will
// each register the same object with separate MutableObjectManager instances. Thus, we
// must ensure that the object and its associated metadata (such as the semaphores) are
// destructed the proper number of times.
TEST(MutableObjectTest, TestMutableObjectManagerDestruct) {
  MutableObjectManager manager1;
  MutableObjectManager manager2;
  ObjectID object_id = ObjectID::FromRandom();
  std::string unique_name;

  {
    std::unique_ptr<plasma::MutableObject> object1 = MakeObject();
    object1->header->Init();
    unique_name = std::string(object1->header->unique_name);
    ASSERT_TRUE(
        manager1.RegisterChannel(object_id, std::move(object1), /*reader=*/true).ok());
  }
  {
    std::unique_ptr<plasma::MutableObject> object2 = MakeObject();
    object2->header->Init();
    memset(object2->header->unique_name, 0, sizeof(object2->header->unique_name));
    memcpy(object2->header->unique_name, unique_name.c_str(), unique_name.size());
    ASSERT_TRUE(
        manager2.RegisterChannel(object_id, std::move(object2), /*reader=*/false).ok());
  }
  // The purpose of this test is to ensure that neither the MutableObjectManager instance
  // destructor crashes when the two instances go out of scope below at the end of this
  // function.
}

#endif  // defined(__APPLE__) || defined(__linux__)

}  // namespace experimental
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
