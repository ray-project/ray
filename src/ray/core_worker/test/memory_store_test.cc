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

#include "absl/synchronization/mutex.h"
#include "gtest/gtest.h"
#include "ray/common/test_util.h"

namespace ray {
namespace core {

inline std::shared_ptr<ray::LocalMemoryBuffer> MakeBufferFromString(const uint8_t *data,
                                                                    size_t data_size) {
  auto metadata = const_cast<uint8_t *>(data);
  auto meta_buffer =
      std::make_shared<ray::LocalMemoryBuffer>(metadata, data_size, /*copy_data=*/true);
  return meta_buffer;
}

inline std::shared_ptr<ray::LocalMemoryBuffer> MakeLocalMemoryBufferFromString(
    const std::string &str) {
  return MakeBufferFromString(reinterpret_cast<const uint8_t *>(str.data()), str.size());
}

TEST(TestMemoryStore, TestReportUnhandledErrors) {
  std::vector<std::shared_ptr<RayObject>> results;
  WorkerContext context(WorkerType::WORKER, WorkerID::FromRandom(), JobID::FromInt(0));
  int unhandled_count = 0;

  std::shared_ptr<CoreWorkerMemoryStore> provider =
      std::make_shared<CoreWorkerMemoryStore>(
          nullptr, nullptr, nullptr, [&](const RayObject &obj) { unhandled_count++; });
  RayObject obj1(rpc::ErrorType::TASK_EXECUTION_EXCEPTION);
  RayObject obj2(rpc::ErrorType::TASK_EXECUTION_EXCEPTION);
  auto id1 = ObjectID::FromRandom();
  auto id2 = ObjectID::FromRandom();

  // Check basic put and get.
  ASSERT_TRUE(provider->GetIfExists(id1) == nullptr);
  RAY_CHECK(provider->Put(obj1, id1));
  RAY_CHECK(provider->Put(obj2, id2));
  ASSERT_TRUE(provider->GetIfExists(id1) != nullptr);
  ASSERT_EQ(unhandled_count, 0);

  // Check delete without get.
  provider->Delete({id1, id2});
  ASSERT_EQ(unhandled_count, 1);
  unhandled_count = 0;

  // Check delete after get.
  RAY_CHECK(provider->Put(obj1, id1));
  RAY_CHECK(provider->Put(obj1, id2));
  RAY_UNUSED(provider->Get({id1}, 1, 100, context, false, &results));
  RAY_UNUSED(provider->Get({id2}, 1, 100, context, false, &results));
  provider->Delete({id1, id2});
  ASSERT_EQ(unhandled_count, 0);

  // Check delete after async get.
  provider->GetAsync({id2}, [](std::shared_ptr<RayObject> obj) {});
  RAY_CHECK(provider->Put(obj1, id1));
  RAY_CHECK(provider->Put(obj2, id2));
  provider->GetAsync({id1}, [](std::shared_ptr<RayObject> obj) {});
  provider->Delete({id1, id2});
  ASSERT_EQ(unhandled_count, 0);
}

TEST(TestMemoryStore, TestMemoryStoreStats) {
  /// Simple validation for test memory store stats.
  std::shared_ptr<CoreWorkerMemoryStore> provider =
      std::make_shared<CoreWorkerMemoryStore>(nullptr, nullptr, nullptr, nullptr);

  // Iterate through the memory store and compare the values that are obtained by
  // GetMemoryStoreStatisticalData.
  auto fill_expected_memory_stats = [&](MemoryStoreStats &expected_item) {
    {
      absl::MutexLock lock(&provider->mu_);
      for (const auto &it : provider->objects_) {
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

  RAY_CHECK(provider->Put(obj1, id1));
  RAY_CHECK(provider->Put(obj2, id2));
  RAY_CHECK(provider->Put(obj3, id3));
  provider->Delete({id3});

  MemoryStoreStats expected_item;
  fill_expected_memory_stats(expected_item);
  MemoryStoreStats item = provider->GetMemoryStoreStatisticalData();
  ASSERT_EQ(item.num_in_plasma, expected_item.num_in_plasma);
  ASSERT_EQ(item.num_local_objects, expected_item.num_local_objects);
  ASSERT_EQ(item.num_local_objects_bytes, expected_item.num_local_objects_bytes);

  // Delete all other objects and see if stats are recorded correctly.
  provider->Delete({id1, id2});

  MemoryStoreStats expected_item2;
  fill_expected_memory_stats(expected_item2);
  item = provider->GetMemoryStoreStatisticalData();
  ASSERT_EQ(item.num_in_plasma, expected_item2.num_in_plasma);
  ASSERT_EQ(item.num_local_objects, expected_item2.num_local_objects);
  ASSERT_EQ(item.num_local_objects_bytes, expected_item2.num_local_objects_bytes);

  RAY_CHECK(provider->Put(obj1, id1));
  RAY_CHECK(provider->Put(obj2, id2));
  RAY_CHECK(provider->Put(obj3, id3));
  MemoryStoreStats expected_item3;
  fill_expected_memory_stats(expected_item3);
  item = provider->GetMemoryStoreStatisticalData();
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
      auto buf = object.GetData();
      std::string data(reinterpret_cast<char *>(buf->Data()), buf->Size());
      return std::make_shared<TestBuffer>(mock_buffer_manager, data);
    };

    return std::make_shared<ray::RayObject>(object.GetMetadata(),
                                            object.GetNestedRefs(),
                                            std::move(data_factory),
                                            /*copy_data=*/true);
  };
  std::shared_ptr<CoreWorkerMemoryStore> memory_store =
      std::make_shared<CoreWorkerMemoryStore>(
          nullptr, nullptr, nullptr, nullptr, std::move(my_object_allocator));
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

}  // namespace core
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
