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
#include "ray/object_manager/plasma/object_lifecycle_manager.h"

using namespace ray;
using namespace testing;
using namespace plasma::flatbuf;

namespace plasma {
namespace {
int64_t Random(int64_t max) {
  static absl::BitGen bitgen;
  return absl::Uniform(bitgen, 1, max);
}
}  // namespace

class DummyAllocator : public IAllocator {
 public:
  absl::optional<Allocation> Allocate(size_t bytes) override {
    allocated_ += bytes;
    auto allocation = Allocation();
    allocation.size = bytes;
    return std::move(allocation);
  }

  absl::optional<Allocation> FallbackAllocate(size_t bytes) override {
    return absl::nullopt;
  }

  void Free(Allocation allocation) override { allocated_ -= allocation.size; }

  int64_t GetFootprintLimit() const override {
    return std::numeric_limits<int64_t>::max();
  }

  int64_t Allocated() const override { return allocated_; }

  int64_t FallbackAllocated() const override { return 0; }

 private:
  int64_t allocated_ = 0;
};

struct ObjectStatsCollectorTest : public Test {
  void SetUp() override {
    Test::SetUp();
    Reset();
  }

  void Reset() {
    allocator_ = std::make_unique<DummyAllocator>();
    manager_ =
        std::make_unique<ObjectLifecycleManager>(*allocator_, [](auto /* unused */) {});
    collector_ = &manager_->stats_collector_;
    object_store_ = dynamic_cast<ObjectStore *>(manager_->object_store_.get());
    used_ids_.clear();
    num_bytes_created_total_ = 0;
  }

  void ExpectStatsMatch() {
    int64_t num_objects_spillable = 0;
    int64_t num_bytes_spillable = 0;
    int64_t num_objects_unsealed = 0;
    int64_t num_bytes_unsealed = 0;
    int64_t num_objects_in_use = 0;
    int64_t num_bytes_in_use = 0;
    int64_t num_objects_evictable = 0;
    int64_t num_bytes_evictable = 0;

    int64_t num_objects_created_by_worker = 0;
    int64_t num_bytes_created_by_worker = 0;
    int64_t num_objects_restored = 0;
    int64_t num_bytes_restored = 0;
    int64_t num_objects_received = 0;
    int64_t num_bytes_received = 0;
    int64_t num_objects_errored = 0;
    int64_t num_bytes_errored = 0;

    for (const auto &obj_entry : object_store_->object_table_) {
      const auto &obj = obj_entry.second;

      if (obj->ref_count > 0) {
        num_objects_in_use++;
        num_bytes_in_use += obj->object_info.GetObjectSize();
      }

      if (obj->state == ObjectState::PLASMA_CREATED) {
        num_objects_unsealed++;
        num_bytes_unsealed += obj->object_info.GetObjectSize();
      } else {
        if (obj->ref_count == 1 &&
            obj->source == plasma::flatbuf::ObjectSource::CreatedByWorker) {
          num_objects_spillable++;
          num_bytes_spillable += obj->object_info.GetObjectSize();
        }

        if (obj->ref_count == 0) {
          num_objects_evictable++;
          num_bytes_evictable += obj->object_info.GetObjectSize();
        }
      }

      if (obj->source == plasma::flatbuf::ObjectSource::CreatedByWorker) {
        num_objects_created_by_worker++;
        num_bytes_created_by_worker += obj->object_info.GetObjectSize();
      } else if (obj->source == plasma::flatbuf::ObjectSource::RestoredFromStorage) {
        num_objects_restored++;
        num_bytes_restored += obj->object_info.GetObjectSize();
      } else if (obj->source == plasma::flatbuf::ObjectSource::ReceivedFromRemoteRaylet) {
        num_objects_received++;
        num_bytes_received += obj->object_info.GetObjectSize();
      } else if (obj->source == plasma::flatbuf::ObjectSource::ErrorStoredByRaylet) {
        num_objects_errored++;
        num_bytes_errored += obj->object_info.GetObjectSize();
      }
    }

    EXPECT_EQ(num_bytes_created_total_, collector_->num_bytes_created_total_);
    EXPECT_EQ(num_objects_spillable, collector_->num_objects_spillable_);
    EXPECT_EQ(num_bytes_spillable, collector_->num_bytes_spillable_);
    EXPECT_EQ(num_objects_unsealed, collector_->num_objects_unsealed_);
    EXPECT_EQ(num_bytes_unsealed, collector_->num_bytes_unsealed_);
    EXPECT_EQ(num_objects_in_use, collector_->num_objects_in_use_);
    EXPECT_EQ(num_bytes_in_use, collector_->num_bytes_in_use_);
    EXPECT_EQ(num_objects_evictable, collector_->num_objects_evictable_);
    EXPECT_EQ(num_bytes_evictable, collector_->num_bytes_evictable_);
    EXPECT_EQ(num_objects_created_by_worker, collector_->num_objects_created_by_worker_);
    EXPECT_EQ(num_bytes_created_by_worker, collector_->num_bytes_created_by_worker_);
    EXPECT_EQ(num_objects_restored, collector_->num_objects_restored_);
    EXPECT_EQ(num_bytes_restored, collector_->num_bytes_restored_);
    EXPECT_EQ(num_objects_received, collector_->num_objects_received_);
    EXPECT_EQ(num_bytes_received, collector_->num_bytes_received_);
    EXPECT_EQ(num_objects_errored, collector_->num_objects_errored_);
    EXPECT_EQ(num_bytes_errored, collector_->num_bytes_errored_);
  }

  ray::ObjectInfo CreateNewObjectInfo(int64_t data_size) {
    ObjectID id = ObjectID::FromRandom();
    while (used_ids_.count(id) > 0) {
      id = ObjectID::FromRandom();
    }
    used_ids_.insert(id);
    ray::ObjectInfo info;
    info.object_id = id;
    info.data_size = data_size;
    info.metadata_size = 0;
    return info;
  }

  void EvictObject(ObjectID id) { manager_->EvictObjects({id}); }

  std::unique_ptr<DummyAllocator> allocator_;
  std::unique_ptr<ObjectLifecycleManager> manager_;
  ObjectStatsCollector *collector_;
  ObjectStore *object_store_;
  std::unordered_set<ObjectID> used_ids_;
  int64_t num_bytes_created_total_;
};

TEST_F(ObjectStatsCollectorTest, CreateAndAbort) {
  std::vector<ObjectSource> sources = {ObjectSource::CreatedByWorker,
                                       ObjectSource::RestoredFromStorage,
                                       ObjectSource::ReceivedFromRemoteRaylet,
                                       ObjectSource::ErrorStoredByRaylet};

  for (auto source : sources) {
    int64_t size = Random(100);
    auto info = CreateNewObjectInfo(size++);
    manager_->CreateObject(info, source, false);
    num_bytes_created_total_ += info.GetObjectSize();
    ExpectStatsMatch();
  }

  for (auto id : used_ids_) {
    manager_->AbortObject(id);
    ExpectStatsMatch();
  }
}

TEST_F(ObjectStatsCollectorTest, CreateAndDelete) {
  std::vector<ObjectSource> sources = {ObjectSource::CreatedByWorker,
                                       ObjectSource::RestoredFromStorage,
                                       ObjectSource::ReceivedFromRemoteRaylet,
                                       ObjectSource::ErrorStoredByRaylet};

  for (auto source : sources) {
    int64_t size = Random(100);
    auto info = CreateNewObjectInfo(size);
    manager_->CreateObject(info, source, false);
    num_bytes_created_total_ += info.GetObjectSize();
    ExpectStatsMatch();
  }

  for (auto id : used_ids_) {
    int64_t ref_count = Random(10);
    for (int64_t i = 0; i < ref_count; i++) {
      manager_->AddReference(id);
    }
    if (Random(3) == 2) {
      manager_->SealObject(id);
    }
    manager_->DeleteObject(id);
    ExpectStatsMatch();
  }
}

TEST_F(ObjectStatsCollectorTest, Eviction) {
  std::vector<ObjectSource> sources = {ObjectSource::CreatedByWorker,
                                       ObjectSource::RestoredFromStorage,
                                       ObjectSource::ReceivedFromRemoteRaylet,
                                       ObjectSource::ErrorStoredByRaylet};

  int64_t size = 100;
  for (auto source : sources) {
    auto info = CreateNewObjectInfo(size++);
    manager_->CreateObject(info, source, false);
    num_bytes_created_total_ += info.GetObjectSize();
    ExpectStatsMatch();
  }

  for (auto id : used_ids_) {
    manager_->SealObject(id);
    EvictObject(id);
    ExpectStatsMatch();
  }
}

TEST_F(ObjectStatsCollectorTest, RefCountPassThrough) {
  auto info1 = CreateNewObjectInfo(100);
  auto id1 = info1.object_id;
  manager_->CreateObject(info1, ObjectSource::CreatedByWorker, false);
  num_bytes_created_total_ += info1.GetObjectSize();

  auto info2 = CreateNewObjectInfo(200);
  auto id2 = info2.object_id;
  manager_->CreateObject(info2, ObjectSource::RestoredFromStorage, false);
  num_bytes_created_total_ += info2.GetObjectSize();
  ExpectStatsMatch();

  manager_->AddReference(id1);
  ExpectStatsMatch();

  manager_->SealObject(id1);
  ExpectStatsMatch();

  manager_->AddReference(id1);
  ExpectStatsMatch();

  manager_->AddReference(id2);
  ExpectStatsMatch();

  manager_->SealObject(id2);
  ExpectStatsMatch();

  manager_->AddReference(id2);
  ExpectStatsMatch();

  manager_->RemoveReference(id2);
  ExpectStatsMatch();

  manager_->RemoveReference(id2);
  ExpectStatsMatch();

  manager_->RemoveReference(id1);
  ExpectStatsMatch();

  manager_->RemoveReference(id1);
  ExpectStatsMatch();

  manager_->DeleteObject(id1);
  ExpectStatsMatch();

  manager_->DeleteObject(id2);
  ExpectStatsMatch();
}
}  // namespace plasma