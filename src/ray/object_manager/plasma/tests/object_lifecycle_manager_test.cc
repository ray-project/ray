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

#include "ray/object_manager/plasma/object_lifecycle_manager.h"

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

using ray::ObjectID;
using ::testing::Test;

namespace plasma {

// Hand-written fake eviction policy. Records the object ids passed to each hook and
// exposes settable return values. RequireSpace can additionally be driven by a hook
// so tests can mutate the objects_to_evict output parameter.
class FakeEvictionPolicy : public IEvictionPolicy {
 public:
  void ObjectCreated(const ObjectID &object_id) override {
    object_created_ids.push_back(object_id);
  }
  int64_t RequireSpace(int64_t size, std::vector<ObjectID> &objects_to_evict) override {
    require_space_call_count++;
    if (require_space_hook) {
      return require_space_hook(size, objects_to_evict);
    }
    return require_space_return;
  }
  void BeginObjectAccess(const ObjectID &object_id) override {
    begin_object_access_ids.push_back(object_id);
  }
  void EndObjectAccess(const ObjectID &object_id) override {
    end_object_access_ids.push_back(object_id);
  }
  int64_t ChooseObjectsToEvict(int64_t num_bytes_required,
                               std::vector<ObjectID> &objects_to_evict) override {
    return choose_objects_to_evict_return;
  }
  void RemoveObject(const ObjectID &object_id) override {
    remove_object_ids.push_back(object_id);
  }
  std::string DebugString() const override { return "FakeEvictionPolicy"; }

  std::vector<ObjectID> object_created_ids;
  std::vector<ObjectID> begin_object_access_ids;
  std::vector<ObjectID> end_object_access_ids;
  std::vector<ObjectID> remove_object_ids;
  int require_space_call_count = 0;
  int64_t require_space_return = 0;
  int64_t choose_objects_to_evict_return = 0;
  std::function<int64_t(int64_t, std::vector<ObjectID> &)> require_space_hook;
};

// Hand-written fake object store. GetObject/CreateObject/SealObject return values from
// settable queues in call order, repeating the last element once the queue is exhausted
// (and returning nullptr when the queue is empty). Calls are recorded so tests can
// assert on interactions.
class FakeObjectStore : public IObjectStore {
 public:
  const LocalObject *CreateObject(const ray::ObjectInfo &,
                                  plasma::flatbuf::ObjectSource,
                                  bool fallback_allocate) override {
    if (fallback_allocate) {
      create_object_fallback_call_count++;
      return Next(create_object_fallback_returns, create_object_fallback_index);
    }
    create_object_call_count++;
    return Next(create_object_returns, create_object_index);
  }
  const LocalObject *GetObject(const ObjectID &object_id) const override {
    get_object_call_count++;
    get_object_ids.push_back(object_id);
    return Next(get_object_returns, get_object_index);
  }
  const LocalObject *SealObject(const ObjectID &object_id) override {
    seal_object_ids.push_back(object_id);
    return Next(seal_object_returns, seal_object_index);
  }
  bool DeleteObject(const ObjectID &object_id) override {
    delete_object_ids.push_back(object_id);
    return delete_object_return;
  }

  std::vector<const LocalObject *> get_object_returns;
  std::vector<const LocalObject *> create_object_returns;
  std::vector<const LocalObject *> create_object_fallback_returns;
  std::vector<const LocalObject *> seal_object_returns;
  bool delete_object_return = true;

  mutable std::vector<ObjectID> get_object_ids;
  std::vector<ObjectID> seal_object_ids;
  std::vector<ObjectID> delete_object_ids;

  mutable int get_object_call_count = 0;
  int create_object_call_count = 0;
  int create_object_fallback_call_count = 0;

 private:
  static const LocalObject *Next(const std::vector<const LocalObject *> &returns,
                                 size_t &index) {
    if (returns.empty()) {
      return nullptr;
    }
    if (index < returns.size()) {
      return returns[index++];
    }
    return returns.back();
  }

  mutable size_t get_object_index = 0;
  size_t create_object_index = 0;
  size_t create_object_fallback_index = 0;
  size_t seal_object_index = 0;
};

// Hand-written fake stats collector. The lifecycle manager only requires the three
// virtual hooks to be no-ops (the original test placed no expectations on them); all
// other bookkeeping methods use the base implementation.
class FakeObjectStatsCollector : public ObjectStatsCollector {
 public:
  void OnObjectCreated(const LocalObject &object) override {}
  void OnObjectSealed(const LocalObject &object) override {}
  void OnObjectDeleting(const LocalObject &object) override {}
};

struct ObjectLifecycleManagerTest : public Test {
  void SetUp() override {
    Test::SetUp();
    auto eviction_policy = std::make_unique<FakeEvictionPolicy>();
    auto object_store = std::make_unique<FakeObjectStore>();
    auto stats_collector = std::make_unique<FakeObjectStatsCollector>();
    eviction_policy_ = eviction_policy.get();
    object_store_ = object_store.get();
    stats_collector_ = stats_collector.get();
    auto delete_object_cb = [this](auto &id) { notify_deleted_ids_.push_back(id); };
    manager_ = std::make_unique<ObjectLifecycleManager>(
        ObjectLifecycleManager(std::move(object_store),
                               std::move(eviction_policy),
                               delete_object_cb,
                               std::move(stats_collector)));
    sealed_object_.state_ = ObjectState::PLASMA_SEALED;
    not_sealed_object_.state_ = ObjectState::PLASMA_CREATED;
    one_ref_object_.state_ = ObjectState::PLASMA_SEALED;
    one_ref_object_.ref_count_ = 1;
    two_ref_object_.state_ = ObjectState::PLASMA_SEALED;
    two_ref_object_.ref_count_ = 2;
  }

  FakeEvictionPolicy *eviction_policy_;
  FakeObjectStore *object_store_;
  FakeObjectStatsCollector *stats_collector_;
  std::unique_ptr<ObjectLifecycleManager> manager_;
  std::vector<ObjectID> notify_deleted_ids_;

  LocalObject object1_{Allocation()};
  LocalObject object2_{Allocation()};
  LocalObject sealed_object_{Allocation()};
  LocalObject not_sealed_object_{Allocation()};
  LocalObject one_ref_object_{Allocation()};
  LocalObject two_ref_object_{Allocation()};
  ObjectID id1_ = ObjectID::FromRandom();
  ObjectID id2_ = ObjectID::FromRandom();
  ObjectID id3_ = ObjectID::FromRandom();
};

TEST_F(ObjectLifecycleManagerTest, CreateObjectExists) {
  object_store_->get_object_returns = {&object1_};
  auto expected = std::pair<const LocalObject *, flatbuf::PlasmaError>(
      nullptr, flatbuf::PlasmaError::ObjectExists);
  auto result = manager_->CreateObject({}, {}, /*falback*/ false);
  EXPECT_EQ(expected, result);
  EXPECT_EQ(1, object_store_->get_object_call_count);
}

TEST_F(ObjectLifecycleManagerTest, CreateObjectSuccess) {
  object_store_->get_object_returns = {nullptr};
  object_store_->create_object_returns = {&object1_};
  auto expected = std::pair<const LocalObject *, flatbuf::PlasmaError>(
      &object1_, flatbuf::PlasmaError::OK);
  auto result = manager_->CreateObject({}, {}, /*falback*/ false);
  EXPECT_EQ(expected, result);
  EXPECT_EQ(1, object_store_->get_object_call_count);
  EXPECT_EQ(1, object_store_->create_object_call_count);
}

TEST_F(ObjectLifecycleManagerTest, CreateObjectTriggerGC) {
  object_store_->get_object_returns = {nullptr,
                                       // called during eviction.
                                       &sealed_object_,
                                       &sealed_object_};
  object_store_->create_object_returns = {
      nullptr,
      // once eviction finishes, createobject is called again.
      &object1_};

  // gc returns object to evict
  eviction_policy_->require_space_hook = [&](auto size, auto &to_evict) {
    to_evict.push_back(id1_);
    return 0;
  };

  // eviction
  object_store_->delete_object_return = true;

  auto expected = std::pair<const LocalObject *, flatbuf::PlasmaError>(
      &object1_, flatbuf::PlasmaError::OK);
  auto result = manager_->CreateObject({}, {}, /*falback*/ false);
  EXPECT_EQ(expected, result);

  EXPECT_EQ(3, object_store_->get_object_call_count);
  EXPECT_EQ(2, object_store_->create_object_call_count);
  EXPECT_EQ(1, eviction_policy_->require_space_call_count);
  EXPECT_EQ((std::vector<ObjectID>{id1_}), object_store_->delete_object_ids);
  EXPECT_EQ((std::vector<ObjectID>{id1_}), eviction_policy_->remove_object_ids);

  // evicton is notified.
  std::vector<ObjectID> expect_notified_ids{id1_};
  EXPECT_EQ(expect_notified_ids, notify_deleted_ids_);
}

TEST_F(ObjectLifecycleManagerTest, CreateObjectTriggerGCExhaused) {
  object_store_->get_object_returns = {nullptr};
  // create_object_returns left empty so non-fallback creation always fails.
  object_store_->create_object_fallback_returns = {&object1_};
  eviction_policy_->require_space_return = 0;
  auto expected = std::pair<const LocalObject *, flatbuf::PlasmaError>(
      &object1_, flatbuf::PlasmaError::OK);
  auto result = manager_->CreateObject({}, {}, /*falback*/ true);
  EXPECT_EQ(expected, result);
  EXPECT_EQ(11, object_store_->create_object_call_count);
  EXPECT_EQ(11, eviction_policy_->require_space_call_count);
  EXPECT_EQ(1, object_store_->create_object_fallback_call_count);
}

TEST_F(ObjectLifecycleManagerTest, CreateObjectWithoutFallback) {
  object_store_->get_object_returns = {nullptr};
  object_store_->create_object_returns = {nullptr};
  // evict failed;
  eviction_policy_->require_space_return = 1;
  auto expected = std::pair<const LocalObject *, flatbuf::PlasmaError>(
      nullptr, flatbuf::PlasmaError::OutOfMemory);
  auto result = manager_->CreateObject({}, {}, /*falback*/ false);
  EXPECT_EQ(expected, result);
  EXPECT_EQ(1, object_store_->create_object_call_count);
  EXPECT_EQ(1, eviction_policy_->require_space_call_count);
}

TEST_F(ObjectLifecycleManagerTest, CreateObjectWithFallback) {
  object_store_->get_object_returns = {nullptr};
  object_store_->create_object_returns = {nullptr};
  eviction_policy_->require_space_return = 1;
  object_store_->create_object_fallback_returns = {&object1_};
  auto expected = std::pair<const LocalObject *, flatbuf::PlasmaError>(
      &object1_, flatbuf::PlasmaError::OK);
  auto result = manager_->CreateObject({}, {}, /*falback*/ true);
  EXPECT_EQ(expected, result);
  EXPECT_EQ(1, object_store_->create_object_call_count);
  EXPECT_EQ(1, eviction_policy_->require_space_call_count);
  EXPECT_EQ(1, object_store_->create_object_fallback_call_count);
}

TEST_F(ObjectLifecycleManagerTest, CreateObjectWithFallbackFailed) {
  object_store_->get_object_returns = {nullptr};
  object_store_->create_object_returns = {nullptr};
  eviction_policy_->require_space_return = 1;
  object_store_->create_object_fallback_returns = {nullptr};
  auto expected = std::pair<const LocalObject *, flatbuf::PlasmaError>(
      nullptr, flatbuf::PlasmaError::OutOfMemory);
  auto result = manager_->CreateObject({}, {}, /*falback*/ true);
  EXPECT_EQ(expected, result);
  EXPECT_EQ(1, object_store_->create_object_call_count);
  EXPECT_EQ(1, eviction_policy_->require_space_call_count);
  EXPECT_EQ(1, object_store_->create_object_fallback_call_count);
}

TEST_F(ObjectLifecycleManagerTest, GetObject) {
  object_store_->get_object_returns = {&object2_};
  EXPECT_EQ(&object2_, manager_->GetObject(id1_));
  EXPECT_EQ((std::vector<ObjectID>{id1_}), object_store_->get_object_ids);
}

TEST_F(ObjectLifecycleManagerTest, SealObject) {
  object_store_->seal_object_returns = {&sealed_object_};
  EXPECT_EQ(&sealed_object_, manager_->SealObject(id1_));
  EXPECT_EQ((std::vector<ObjectID>{id1_}), object_store_->seal_object_ids);
}

TEST_F(ObjectLifecycleManagerTest, AbortFailure) {
  object_store_->get_object_returns = {nullptr, &sealed_object_};
  EXPECT_EQ(manager_->AbortObject(id1_), flatbuf::PlasmaError::ObjectNonexistent);
  EXPECT_EQ(manager_->AbortObject(id2_), flatbuf::PlasmaError::ObjectSealed);
  EXPECT_EQ((std::vector<ObjectID>{id1_, id2_}), object_store_->get_object_ids);
}

TEST_F(ObjectLifecycleManagerTest, AbortSuccess) {
  object_store_->get_object_returns = {&not_sealed_object_};
  object_store_->delete_object_return = true;
  EXPECT_EQ(manager_->AbortObject(id3_), flatbuf::PlasmaError::OK);
  EXPECT_EQ(2, object_store_->get_object_call_count);
  EXPECT_EQ((std::vector<ObjectID>{id3_}), object_store_->delete_object_ids);
  EXPECT_EQ((std::vector<ObjectID>{id3_}), eviction_policy_->remove_object_ids);
  // aborted object is not notified.
  EXPECT_TRUE(notify_deleted_ids_.empty());
}

TEST_F(ObjectLifecycleManagerTest, DeleteFailure) {
  object_store_->get_object_returns = {nullptr, &not_sealed_object_, &one_ref_object_};
  EXPECT_EQ(flatbuf::PlasmaError::ObjectNonexistent, manager_->DeleteObject(id1_));

  {
    EXPECT_EQ(flatbuf::PlasmaError::ObjectNotSealed, manager_->DeleteObject(id2_));
    absl::flat_hash_set<ObjectID> expected_eagerly_deletion_objects{id2_};
    EXPECT_EQ(expected_eagerly_deletion_objects, manager_->earger_deletion_objects_);
  }

  {
    manager_->earger_deletion_objects_.clear();
    EXPECT_EQ(flatbuf::PlasmaError::ObjectInUse, manager_->DeleteObject(id3_));
    absl::flat_hash_set<ObjectID> expected_eagerly_deletion_objects{id3_};
    EXPECT_EQ(expected_eagerly_deletion_objects, manager_->earger_deletion_objects_);
  }
}

TEST_F(ObjectLifecycleManagerTest, DeleteSuccess) {
  object_store_->get_object_returns = {&sealed_object_};
  object_store_->delete_object_return = true;

  EXPECT_EQ(flatbuf::PlasmaError::OK, manager_->DeleteObject(id1_));
  EXPECT_EQ(2, object_store_->get_object_call_count);
  EXPECT_EQ((std::vector<ObjectID>{id1_}), object_store_->delete_object_ids);
  EXPECT_EQ((std::vector<ObjectID>{id1_}), eviction_policy_->remove_object_ids);
  std::vector<ObjectID> expect_notified_ids{id1_};
  EXPECT_EQ(expect_notified_ids, notify_deleted_ids_);
}

TEST_F(ObjectLifecycleManagerTest, AddReference) {
  object_store_->get_object_returns = {nullptr, &object1_, &one_ref_object_};

  { EXPECT_FALSE(manager_->AddReference(id1_)); }

  {
    EXPECT_TRUE(manager_->AddReference(id2_));
    EXPECT_EQ(1, object1_.GetRefCount());
    EXPECT_EQ((std::vector<ObjectID>{id2_}), eviction_policy_->begin_object_access_ids);
  }

  {
    EXPECT_TRUE(manager_->AddReference(id3_));
    EXPECT_EQ(2, one_ref_object_.GetRefCount());
    // No new BeginObjectAccess since the object already had a reference.
    EXPECT_EQ((std::vector<ObjectID>{id2_}), eviction_policy_->begin_object_access_ids);
  }
}

TEST_F(ObjectLifecycleManagerTest, RemoveReferenceFailure) {
  object_store_->get_object_returns = {nullptr, &object1_};

  { EXPECT_FALSE(manager_->RemoveReference(id1_)); }

  { EXPECT_FALSE(manager_->RemoveReference(id2_)); }
}

TEST_F(ObjectLifecycleManagerTest, RemoveReferenceTwoRef) {
  object_store_->get_object_returns = {&two_ref_object_};
  EXPECT_TRUE(manager_->RemoveReference(id1_));
  EXPECT_EQ(1, two_ref_object_.GetRefCount());
}

TEST_F(ObjectLifecycleManagerTest, RemoveReferenceOneRefSealed) {
  EXPECT_TRUE(one_ref_object_.Sealed());
  object_store_->get_object_returns = {&one_ref_object_};
  EXPECT_TRUE(manager_->RemoveReference(id1_));
  EXPECT_EQ(0, one_ref_object_.GetRefCount());
  EXPECT_EQ((std::vector<ObjectID>{id1_}), eviction_policy_->end_object_access_ids);
}

TEST_F(ObjectLifecycleManagerTest, RemoveReferenceOneRefEagerlyDeletion) {
  manager_->earger_deletion_objects_.emplace(id1_);

  object_store_->get_object_returns = {&one_ref_object_};
  object_store_->delete_object_return = true;

  EXPECT_TRUE(manager_->RemoveReference(id1_));
  EXPECT_EQ(0, one_ref_object_.GetRefCount());
  EXPECT_EQ((std::vector<ObjectID>{id1_}), eviction_policy_->end_object_access_ids);
  EXPECT_EQ((std::vector<ObjectID>{id1_}), object_store_->delete_object_ids);
  EXPECT_EQ((std::vector<ObjectID>{id1_}), eviction_policy_->remove_object_ids);

  std::vector<ObjectID> expect_notified_ids{id1_};
  EXPECT_EQ(expect_notified_ids, notify_deleted_ids_);
}
}  // namespace plasma
