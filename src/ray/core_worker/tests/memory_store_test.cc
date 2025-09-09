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

#include "ray/core_worker/store_provider/memory_store/memory_store.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "mock/ray/core_worker/memory_store.h"
#include "ray/common/status.h"
#include "ray/common/status_or.h"
#include "ray/common/test_utils.h"

namespace ray {
namespace core {

namespace {

std::shared_ptr<ray::LocalMemoryBuffer> MakeLocalMemoryBufferFromString(
    const std::string &str) {
  auto metadata = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(str.data()));
  auto meta_buffer =
      std::make_shared<ray::LocalMemoryBuffer>(metadata, str.size(), /*copy_data=*/true);
  return meta_buffer;
}

}  // namespace

TEST(TestMemoryStore, TestReportUnhandledErrors) {
  std::vector<std::shared_ptr<RayObject>> results;
  WorkerContext context(WorkerType::WORKER, WorkerID::FromRandom(), JobID::FromInt(0));
  int unhandled_count = 0;

  InstrumentedIOContextWithThread io_context("TestReportUnhandledErrors");

  std::shared_ptr<CoreWorkerMemoryStore> memory_store =
      std::make_shared<CoreWorkerMemoryStore>(
          io_context.GetIoService(),
          nullptr,
          nullptr,
          nullptr,
          [&](const RayObject &obj) { unhandled_count++; });
  RayObject obj1(rpc::ErrorType::TASK_EXECUTION_EXCEPTION);
  RayObject obj2(rpc::ErrorType::TASK_EXECUTION_EXCEPTION);
  auto id1 = ObjectID::FromRandom();
  auto id2 = ObjectID::FromRandom();

  // Check basic put and get.
  ASSERT_TRUE(memory_store->GetIfExists(id1) == nullptr);
  memory_store->Put(obj1, id1);
  memory_store->Put(obj2, id2);
  ASSERT_TRUE(memory_store->GetIfExists(id1) != nullptr);
  ASSERT_EQ(unhandled_count, 0);

  // Check delete without get.
  memory_store->Delete({id1, id2});
  ASSERT_EQ(unhandled_count, 1);
  unhandled_count = 0;

  // Check delete after get.
  memory_store->Put(obj1, id1);
  memory_store->Put(obj1, id2);
  RAY_UNUSED(memory_store->Get({id1}, 1, 100, context, false, &results));
  RAY_UNUSED(memory_store->Get({id2}, 1, 100, context, false, &results));
  memory_store->Delete({id1, id2});
  ASSERT_EQ(unhandled_count, 0);

  // Check delete after async get.
  memory_store->GetAsync({id2}, [](std::shared_ptr<RayObject> obj) {});
  memory_store->Put(obj1, id1);
  memory_store->Put(obj2, id2);
  memory_store->GetAsync({id1}, [](std::shared_ptr<RayObject> obj) {});
  memory_store->Delete({id1, id2});
  ASSERT_EQ(unhandled_count, 0);
}

TEST(TestMemoryStore, TestMemoryStoreStats) {
  /// Simple validation for test memory store stats.
  auto memory_store = DefaultCoreWorkerMemoryStoreWithThread::Create();

  // Iterate through the memory store and compare the values that are obtained by
  // GetMemoryStoreStatisticalData.
  auto fill_expected_memory_stats = [&](MemoryStoreStats &expected_item) {
    {
      absl::MutexLock lock(&memory_store->mu_);
      for (const auto &it : memory_store->objects_) {
        if (it.second->IsInPlasmaError()) {
          expected_item.num_in_plasma += 1;
        } else {
          expected_item.num_local_objects += 1;
          expected_item.num_local_objects_bytes += it.second->GetSize();
        }
      }
    }
  };

  RayObject obj1(rpc::ErrorType::OBJECT_IN_PLASMA);
  RayObject obj2(rpc::ErrorType::TASK_EXECUTION_EXCEPTION);
  RayObject obj3(rpc::ErrorType::TASK_EXECUTION_EXCEPTION);
  auto id1 = ObjectID::FromRandom();
  auto id2 = ObjectID::FromRandom();
  auto id3 = ObjectID::FromRandom();

  memory_store->Put(obj1, id1);
  memory_store->Put(obj2, id2);
  memory_store->Put(obj3, id3);
  memory_store->Delete({id3});

  MemoryStoreStats expected_item;
  fill_expected_memory_stats(expected_item);
  MemoryStoreStats item = memory_store->GetMemoryStoreStatisticalData();
  ASSERT_EQ(item.num_in_plasma, expected_item.num_in_plasma);
  ASSERT_EQ(item.num_local_objects, expected_item.num_local_objects);
  ASSERT_EQ(item.num_local_objects_bytes, expected_item.num_local_objects_bytes);

  // Delete all other objects and see if stats are recorded correctly.
  memory_store->Delete({id1, id2});

  MemoryStoreStats expected_item2;
  fill_expected_memory_stats(expected_item2);
  item = memory_store->GetMemoryStoreStatisticalData();
  ASSERT_EQ(item.num_in_plasma, expected_item2.num_in_plasma);
  ASSERT_EQ(item.num_local_objects, expected_item2.num_local_objects);
  ASSERT_EQ(item.num_local_objects_bytes, expected_item2.num_local_objects_bytes);

  memory_store->Put(obj1, id1);
  memory_store->Put(obj2, id2);
  memory_store->Put(obj3, id3);
  MemoryStoreStats expected_item3;
  fill_expected_memory_stats(expected_item3);
  item = memory_store->GetMemoryStoreStatisticalData();
  ASSERT_EQ(item.num_in_plasma, expected_item3.num_in_plasma);
  ASSERT_EQ(item.num_local_objects, expected_item3.num_local_objects);
  ASSERT_EQ(item.num_local_objects_bytes, expected_item3.num_local_objects_bytes);
}

/// A mock manager that manages all test buffers. This mocks
/// that memory pressure is able to be awared.
class MockBufferManager {
 public:
  int64_t GetBuferPressureInBytes() const { return buffer_pressure_in_bytes_; }

  void AcquireMemory(int64_t sz) { buffer_pressure_in_bytes_ += sz; }

  void ReleaseMemory(int64_t sz) { buffer_pressure_in_bytes_ -= sz; }

 private:
  int64_t buffer_pressure_in_bytes_ = 0;
};

class TestBuffer : public Buffer {
 public:
  explicit TestBuffer(MockBufferManager &manager, std::string data)
      : manager_(manager), data_(std::move(data)) {}

  uint8_t *Data() const override {
    return reinterpret_cast<uint8_t *>(const_cast<char *>(data_.data()));
  }

  size_t Size() const override { return data_.size(); }

  bool OwnsData() const override { return true; }

  bool IsPlasmaBuffer() const override { return false; }

  const MockBufferManager &GetBufferManager() const { return manager_; }

 private:
  MockBufferManager &manager_;
  std::string data_;
};

TEST(TestMemoryStore, TestObjectAllocator) {
  MockBufferManager mock_buffer_manager;
  auto my_object_allocator = [&mock_buffer_manager](const ray::RayObject &object,
                                                    const ObjectID &object_id) {
    auto buf = object.GetData();
    mock_buffer_manager.AcquireMemory(buf->Size());
    auto data_factory = [&mock_buffer_manager, object]() -> std::shared_ptr<ray::Buffer> {
      auto inner_buf = object.GetData();
      std::string data(reinterpret_cast<char *>(inner_buf->Data()), inner_buf->Size());
      return std::make_shared<TestBuffer>(mock_buffer_manager, data);
    };

    return std::make_shared<ray::RayObject>(object.GetMetadata(),
                                            object.GetNestedRefs(),
                                            std::move(data_factory),
                                            /*copy_data=*/true);
  };
  InstrumentedIOContextWithThread io_context("TestObjectAllocator");

  std::shared_ptr<CoreWorkerMemoryStore> memory_store =
      std::make_shared<CoreWorkerMemoryStore>(io_context.GetIoService(),
                                              nullptr,
                                              nullptr,
                                              nullptr,
                                              nullptr,
                                              std::move(my_object_allocator));
  const int32_t max_rounds = 1000;
  const std::string hello = "hello";
  for (auto i = 0; i < max_rounds; ++i) {
    auto hello_buffer = MakeLocalMemoryBufferFromString(hello);
    std::vector<rpc::ObjectReference> nested_refs;
    auto hello_object =
        std::make_shared<ray::RayObject>(hello_buffer, nullptr, nested_refs, true);
    memory_store->Put(*hello_object, ObjectID::FromRandom());
  }
  ASSERT_EQ(max_rounds * hello.size(), mock_buffer_manager.GetBuferPressureInBytes());
}

class TestMemoryStoreWait : public ::testing::Test {
 public:
  InstrumentedIOContextWithThread io_context;
  std::shared_ptr<CoreWorkerMemoryStore> memory_store;
  WorkerContext ctx;
  std::string buffer;
  std::vector<rpc::ObjectReference> nested_refs;
  ray::RayObject memory_store_object;
  ray::RayObject plasma_store_object;

 protected:
  TestMemoryStoreWait()
      : io_context("TestWait"),
        memory_store(std::make_shared<CoreWorkerMemoryStore>(io_context.GetIoService())),
        ctx(WorkerType::WORKER, WorkerID::FromRandom(), JobID::FromInt(1)),
        buffer("hello"),
        memory_store_object(
            MakeLocalMemoryBufferFromString(buffer), nullptr, nested_refs, true),
        plasma_store_object(rpc::ErrorType::OBJECT_IN_PLASMA) {}
};

TEST_F(TestMemoryStoreWait, TestWaitNoWaiting) {
  // Object 0 is ready in memory store
  // Object 1 is in plasma
  // Object 2 is in plasma
  // Object 3 is ready in memory store
  // num_objects is 2 (expect 0 and 4 in ready, 1 and 2 in plasma_object_ids)
  std::vector<ObjectID> object_ids = {ObjectID::FromRandom(),
                                      ObjectID::FromRandom(),
                                      ObjectID::FromRandom(),
                                      ObjectID::FromRandom()};
  absl::flat_hash_set<ObjectID> object_ids_set = {object_ids.begin(), object_ids.end()};
  int num_objects = 2;

  memory_store->Put(memory_store_object, object_ids[0]);
  memory_store->Put(plasma_store_object, object_ids[1]);
  memory_store->Put(plasma_store_object, object_ids[2]);
  memory_store->Put(memory_store_object, object_ids[3]);

  absl::flat_hash_set<ObjectID> ready, plasma_object_ids;
  const auto status = memory_store->Wait(
      object_ids_set, num_objects, 1000, ctx, &ready, &plasma_object_ids);

  ASSERT_TRUE(status.ok());
  ASSERT_EQ(ready.size(), 2);
  ASSERT_TRUE(ready.contains(object_ids[0]) && ready.contains(object_ids[3]));
  ASSERT_EQ(plasma_object_ids.size(), 2);
  ASSERT_TRUE(plasma_object_ids.contains(object_ids[1]) &&
              plasma_object_ids.contains(object_ids[2]));
}

TEST_F(TestMemoryStoreWait, TestWaitWithWaiting) {
  // Object 0 is ready in memory store
  // Object 1 is in plasma
  // Object 2 will be in plasma after wait is called
  // Object 3 will be in ready in memory store after wait is called
  // num_objects is 4 (expect 0 and 3 in ready, 1 and 2 in plasma_object_ids)
  std::vector<ObjectID> object_ids = {ObjectID::FromRandom(),
                                      ObjectID::FromRandom(),
                                      ObjectID::FromRandom(),
                                      ObjectID::FromRandom()};
  absl::flat_hash_set<ObjectID> object_ids_set = {object_ids.begin(), object_ids.end()};
  int num_objects = 4;

  memory_store->Put(memory_store_object, object_ids[0]);
  memory_store->Put(plasma_store_object, object_ids[1]);

  absl::flat_hash_set<ObjectID> ready, plasma_object_ids;
  auto future = std::async(std::launch::async, [&]() {
    return memory_store->Wait(
        object_ids_set, num_objects, 100, ctx, &ready, &plasma_object_ids);
  });
  ASSERT_EQ(future.wait_for(std::chrono::milliseconds(1)), std::future_status::timeout);
  memory_store->Put(plasma_store_object, object_ids[2]);
  ASSERT_EQ(future.wait_for(std::chrono::milliseconds(1)), std::future_status::timeout);
  memory_store->Put(memory_store_object, object_ids[3]);

  const auto status = future.get();

  ASSERT_TRUE(status.ok());
  ASSERT_EQ(ready.size(), 2);
  ASSERT_TRUE(ready.contains(object_ids[0]) && ready.contains(object_ids[3]));
  ASSERT_EQ(plasma_object_ids.size(), 2);
  ASSERT_TRUE(plasma_object_ids.contains(object_ids[1]) &&
              plasma_object_ids.contains(object_ids[2]));
}

TEST_F(TestMemoryStoreWait, TestWaitTimeout) {
  // object 0 in plasma
  // waits until 10ms timeout for 2 objects
  absl::flat_hash_set<ObjectID> object_ids_set = {ObjectID::FromRandom()};
  memory_store->Put(plasma_store_object, *object_ids_set.begin());
  int num_objects = 2;

  absl::flat_hash_set<ObjectID> ready, plasma_object_ids;
  const auto status = memory_store->Wait(
      object_ids_set, num_objects, 10, ctx, &ready, &plasma_object_ids);

  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(ready.empty());
  ASSERT_EQ(object_ids_set, plasma_object_ids);
}

}  // namespace core
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
