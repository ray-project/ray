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

#include <unistd.h>

#include <chrono>
#include <iostream>
#include <random>
#include <thread>

#include "gtest/gtest.h"
#include "ray/common/status.h"
#include "ray/common/test_util.h"
#include "ray/object_manager/plasma/store_runner.h"

namespace ray {

int64_t current_time_ms() {
  std::chrono::milliseconds ms_since_epoch =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now().time_since_epoch());
  return ms_since_epoch.count();
}

using namespace plasma;

#ifdef __linux__
#define SHM_DEFAULT_PATH "/dev/shm"
#else
#define SHM_DEFAULT_PATH "/tmp"
#endif

#define STORE_MEMORY (1 << 24)

class TestObjectDirectoryBase : public ::testing::Test {
 public:
  void SetUp() {
    RayLog::StartRayLog("Test", ray::RayLogLevel::INFO, "");
    ray::RayLog::InstallFailureSignalHandler();
    std::string socket_name = "/tmp/plasma";
    std::string shm_dir = "";
    std::string ext_store = "";
    plasma_store_runner.reset(
        new PlasmaStoreRunner(socket_name, STORE_MEMORY, false, shm_dir, ext_store));
    plasma_config.hugepages_enabled = false;
    plasma_config.shared_memory_directory = SHM_DEFAULT_PATH;
    std::shared_ptr<plasma::ExternalStore> external_store{nullptr};
    object_directory.reset(new ObjectDirectory(
        external_store, [this](const std::vector<ObjectInfoT> &infos) {
          this->notifications_counter += infos.size();
        }));
  }

  void TearDown() { RayLog::ShutDownRayLog(); }

  void Workload1(int repeat_times) {
    for (int r = 0; r < repeat_times; r++) {
      auto client = std::unique_ptr<Client>(new Client(-1));
      std::vector<ObjectID> objects;
      for (int i = 0; i < 30; i++) {
        objects.emplace_back(ObjectID::FromRandom());
      }
      PlasmaObject result;
      for (int i = 0; i < 30; i++) {
        RAY_CHECK_OK(object_directory->CreateObject(
            objects[i], true, (i * 30000) % 1721 + 24, 12, 0, client.get(), &result));
        object_directory->SealObjects({objects[i]});
      }
      for (int i = 0; i < 30; i++) {
        if (i % 3 != 0) {
          object_directory->ReleaseObject(objects[i], client.get());
        }
      }
      for (int i = 0; i < 30; i++) {
        if (i % 6 == 1) {
          object_directory->DeleteObject(objects[i]);
        }
      }
      object_directory->DisconnectClient(client.get());
    }
  }

  void Workload2(int repeat_times) {
    for (int r = 0; r < repeat_times; r++) {
      auto client = std::unique_ptr<Client>(new Client(-1));
      std::vector<ObjectID> objects;
      for (int i = 0; i < 30; i++) {
        objects.emplace_back(ObjectID::FromRandom());
      }
      PlasmaObject result;
      for (int i = 0; i < 30; i++) {
        RAY_CHECK_OK(object_directory->CreateObject(
            objects[i], true, (i * 30000) % 1721 + 24, 12, 0, client.get(), &result));
      }
      object_directory->DisconnectClient(client.get());
    }
  }

  void Workload3(int repeat_times) {
    for (int r = 0; r < repeat_times; r++) {
      auto client = std::unique_ptr<Client>(new Client(-1));
      std::vector<ObjectID> objects;
      for (int i = 0; i < 30; i++) {
        objects.emplace_back(ObjectID::FromRandom());
      }
      PlasmaObject result;
      for (int i = 0; i < 30; i++) {
        RAY_CHECK_OK(object_directory->CreateObject(
            objects[i], true, (i * 30000) % 1721 + 24, 12, 0, client.get(), &result));
        object_directory->SealObjects({objects[i]});
      }
      object_directory->DisconnectClient(client.get());
    }
  }

  void Workload4(int repeat_times) {
    for (int r = 0; r < repeat_times; r++) {
      auto client = std::unique_ptr<Client>(new Client(-1));
      std::vector<ObjectID> objects;
      for (int i = 0; i < 30; i++) {
        objects.emplace_back(ObjectID::FromRandom());
      }
      PlasmaObject result;
      for (int i = 0; i < 30; i++) {
        RAY_CHECK_OK(object_directory->CreateObject(
            objects[i], true, (i * 30000) % 1721 + 24, 12, 0, client.get(), &result));
        object_directory->SealObjects({objects[i]});
      }
      for (int i = 0; i < 30; i++) {
        RAY_CHECK(object_directory->ContainsObject(objects[i]) ==
                  ObjectStatus::OBJECT_FOUND);
        RAY_CHECK(object_directory->ContainsObject(ObjectID::FromRandom()) ==
                  ObjectStatus::OBJECT_NOT_FOUND);
      }
      object_directory->DisconnectClient(client.get());
    }
  }

  void Workload5(int repeat_times) {
    for (int r = 0; r < repeat_times; r++) {
      auto client = std::unique_ptr<Client>(new Client(-1));
      std::vector<ObjectID> objects;
      for (int i = 0; i < 30; i++) {
        objects.emplace_back(ObjectID::FromRandom());
      }
      PlasmaObject result;
      for (int i = 0; i < 30; i++) {
        RAY_CHECK_OK(object_directory->CreateAndSealObject(
            objects[i], true, "data_data_data_data_data_data",
            "metadata_metadata_metadata", 0, client.get(), &result));
      }
      object_directory->DisconnectClient(client.get());
    }
  }

  void PerformanceTest(int repeat_times) {
    Workload1(1);  // warm up
    int64_t start = current_time_ms();
    Workload1(repeat_times);
    Workload2(repeat_times);
    Workload3(repeat_times);
    Workload4(repeat_times);
    Workload5(repeat_times);
    int64_t end = current_time_ms();
    RAY_LOG(INFO) << "Time spent: " << end - start << "ms";
  }

  void ConcurrencyTest(int repeat_times) {
    int64_t evicted_memory;
    object_directory->EvictObjects(STORE_MEMORY, &evicted_memory);
    Workload1(1);  // warm up

    int64_t start = current_time_ms();
    PerformanceTest(repeat_times);
    int64_t sequential_time = current_time_ms() - start;

    object_directory->EvictObjects(STORE_MEMORY, &evicted_memory);
    notifications_counter = 0;
    Workload1(1);  // warm up
    start = current_time_ms();
    std::thread w1(&TestObjectDirectoryBase::Workload1, this, repeat_times);
    std::thread w2(&TestObjectDirectoryBase::Workload2, this, repeat_times);
    std::thread w3(&TestObjectDirectoryBase::Workload1, this, repeat_times);
    std::thread w4(&TestObjectDirectoryBase::Workload1, this, repeat_times);
    std::thread w5(&TestObjectDirectoryBase::Workload2, this, repeat_times);
    w1.join();
    w2.join();
    w3.join();
    w4.join();
    w5.join();
    int64_t parallel_time = current_time_ms() - start;
    object_directory->EvictObjects(STORE_MEMORY, &evicted_memory);
    RAY_LOG(INFO) << "sequential_time/concurrency_time: "
                  << (double)sequential_time / parallel_time << "x";
    RAY_CHECK(notifications_counter == 180 * repeat_times + 60) << "concurrency is broken";
  }

 protected:
  int notifications_counter = 0;
};

TEST_F(TestObjectDirectoryBase, ObjectDirectoryStressTest) {
  int64_t evicted_memory;
  PerformanceTest(300);
  object_directory->EvictObjects(STORE_MEMORY, &evicted_memory);
  PerformanceTest(1500);
  object_directory->EvictObjects(STORE_MEMORY, &evicted_memory);
  ConcurrencyTest(10000);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
