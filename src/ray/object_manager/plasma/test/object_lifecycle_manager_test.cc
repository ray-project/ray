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

#include <limits>

#include "absl/random/random.h"
#include "absl/strings/str_format.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

using namespace ray;
using namespace testing;

namespace plasma {

class MockEvictionPolicy : public IEvictionPolicy {
 public:
  MOCK_METHOD1(ObjectCreated, void(const ObjectID &));
  MOCK_METHOD2(RequireSpace, int64_t(int64_t, std::vector<ObjectID> &));
  MOCK_METHOD1(BeginObjectAccess, void(const ObjectID &));
  MOCK_METHOD1(EndObjectAccess, void(const ObjectID &));
  MOCK_METHOD2(ChooseObjectsToEvict, int64_t(int64_t, std::vector<ObjectID> &));
  MOCK_METHOD1(RemoveObject, void(const ObjectID &));
  MOCK_CONST_METHOD0(DebugString, std::string());
};

class MockObjectStore : public IObjectStore {
 public:
  MOCK_METHOD3(CreateObject,
               const LocalObject *(const ray::ObjectInfo &,
                                   plasma::flatbuf::ObjectSource,
                                   bool));
  MOCK_CONST_METHOD1(GetObject, const LocalObject *(const ObjectID &));
  MOCK_METHOD1(SealObject, const LocalObject *(const ObjectID &));
  MOCK_METHOD1(DeleteObject, bool(const ObjectID &));
  MOCK_CONST_METHOD1(GetDebugDump, void(std::stringstream &buffer));
};

struct ObjectLifecycleManagerTest : public Test {
  void SetUp() override {
    Test::SetUp();
    auto eviction_policy = std::make_unique<MockEvictionPolicy>();
    auto object_store = std::make_unique<MockObjectStore>();
    eviction_policy_ = eviction_policy.get();
    object_store_ = object_store.get();
    manager_ = std::make_unique<ObjectLifecycleManager>(ObjectLifecycleManager(
        std::move(object_store), std::move(eviction_policy), [this](auto &id) {
          notify_deleted_ids_.push_back(id);
        }));
    sealed_object_.state = ObjectState::PLASMA_SEALED;
    not_sealed_object_.state = ObjectState::PLASMA_CREATED;
    one_ref_object_.state = ObjectState::PLASMA_SEALED;
    one_ref_object_.ref_count = 1;
    two_ref_object_.state = ObjectState::PLASMA_SEALED;
    two_ref_object_.ref_count = 2;
  }

  MockEvictionPolicy *eviction_policy_;
  MockObjectStore *object_store_;
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
  EXPECT_CALL(*object_store_, GetObject(_)).Times(1).WillOnce(Return(&object1_));
  auto expected = std::pair<const LocalObject *, flatbuf::PlasmaError>(
      nullptr, flatbuf::PlasmaError::ObjectExists);
  auto result = manager_->CreateObject({}, {}, /*falback*/ false);
  EXPECT_EQ(expected, result);
}

TEST_F(ObjectLifecycleManagerTest, CreateObjectSuccess) {
  EXPECT_CALL(*object_store_, GetObject(_)).Times(1).WillOnce(Return(nullptr));
  EXPECT_CALL(*object_store_, CreateObject(_, _, false))
      .Times(1)
      .WillOnce(Return(&object1_));
  auto expected = std::pair<const LocalObject *, flatbuf::PlasmaError>(
      &object1_, flatbuf::PlasmaError::OK);
  auto result = manager_->CreateObject({}, {}, /*falback*/ false);
  EXPECT_EQ(expected, result);
}

TEST_F(ObjectLifecycleManagerTest, CreateObjectTriggerGC) {
  EXPECT_CALL(*object_store_, GetObject(_))
      .Times(3)
      .WillOnce(Return(nullptr))
      // called during eviction.
      .WillOnce(Return(&sealed_object_))
      .WillOnce(Return(&sealed_object_));

  EXPECT_CALL(*object_store_, CreateObject(_, _, false))
      .Times(2)
      .WillOnce(Return(nullptr))
      // once eviction finishes, createobject is called again.
      .WillOnce(Return(&object1_));

  // gc returns object to evict
  EXPECT_CALL(*eviction_policy_, RequireSpace(_, _))
      .Times(1)
      .WillOnce(Invoke([&](auto size, auto &to_evict) {
        to_evict.push_back(id1_);
        return 0;
      }));

  // eviction
  EXPECT_CALL(*object_store_, DeleteObject(id1_)).Times(1).WillOnce(Return(true));
  EXPECT_CALL(*eviction_policy_, RemoveObject(id1_)).Times(1).WillOnce(Return());

  auto expected = std::pair<const LocalObject *, flatbuf::PlasmaError>(
      &object1_, flatbuf::PlasmaError::OK);
  auto result = manager_->CreateObject({}, {}, /*falback*/ false);
  EXPECT_EQ(expected, result);

  // evicton is notified.
  std::vector<ObjectID> expect_notified_ids{id1_};
  EXPECT_EQ(expect_notified_ids, notify_deleted_ids_);
}

TEST_F(ObjectLifecycleManagerTest, CreateObjectTriggerGCExhaused) {
  EXPECT_CALL(*object_store_, GetObject(_)).Times(1).WillOnce(Return(nullptr));
  EXPECT_CALL(*object_store_, CreateObject(_, _, false))
      .Times(11)
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(*eviction_policy_, RequireSpace(_, _)).Times(11).WillRepeatedly(Return(0));
  EXPECT_CALL(*object_store_, CreateObject(_, _, true))
      .Times(1)
      .WillOnce(Return(&object1_));
  auto expected = std::pair<const LocalObject *, flatbuf::PlasmaError>(
      &object1_, flatbuf::PlasmaError::OK);
  auto result = manager_->CreateObject({}, {}, /*falback*/ true);
  EXPECT_EQ(expected, result);
}

TEST_F(ObjectLifecycleManagerTest, CreateObjectWithoutFallback) {
  EXPECT_CALL(*object_store_, GetObject(_)).Times(1).WillOnce(Return(nullptr));
  EXPECT_CALL(*object_store_, CreateObject(_, _, false))
      .Times(1)
      .WillOnce(Return(nullptr));
  // evict failed;
  EXPECT_CALL(*eviction_policy_, RequireSpace(_, _)).Times(1).WillOnce(Return(1));
  auto expected = std::pair<const LocalObject *, flatbuf::PlasmaError>(
      nullptr, flatbuf::PlasmaError::OutOfMemory);
  auto result = manager_->CreateObject({}, {}, /*falback*/ false);
  EXPECT_EQ(expected, result);
}

TEST_F(ObjectLifecycleManagerTest, CreateObjectWithFallback) {
  EXPECT_CALL(*object_store_, GetObject(_)).Times(1).WillOnce(Return(nullptr));
  EXPECT_CALL(*object_store_, CreateObject(_, _, false))
      .Times(1)
      .WillOnce(Return(nullptr));
  EXPECT_CALL(*eviction_policy_, RequireSpace(_, _)).Times(1).WillOnce(Return(1));
  EXPECT_CALL(*object_store_, CreateObject(_, _, true))
      .Times(1)
      .WillOnce(Return(&object1_));
  auto expected = std::pair<const LocalObject *, flatbuf::PlasmaError>(
      &object1_, flatbuf::PlasmaError::OK);
  auto result = manager_->CreateObject({}, {}, /*falback*/ true);
  EXPECT_EQ(expected, result);
}

TEST_F(ObjectLifecycleManagerTest, CreateObjectWithFallbackFailed) {
  EXPECT_CALL(*object_store_, GetObject(_)).Times(1).WillOnce(Return(nullptr));
  EXPECT_CALL(*object_store_, CreateObject(_, _, false))
      .Times(1)
      .WillOnce(Return(nullptr));
  EXPECT_CALL(*eviction_policy_, RequireSpace(_, _)).Times(1).WillOnce(Return(1));
  EXPECT_CALL(*object_store_, CreateObject(_, _, true))
      .Times(1)
      .WillOnce(Return(nullptr));
  auto expected = std::pair<const LocalObject *, flatbuf::PlasmaError>(
      nullptr, flatbuf::PlasmaError::OutOfMemory);
  auto result = manager_->CreateObject({}, {}, /*falback*/ true);
  EXPECT_EQ(expected, result);
}

TEST_F(ObjectLifecycleManagerTest, GetObject) {
  EXPECT_CALL(*object_store_, GetObject(id1_)).Times(1).WillOnce(Return(&object2_));
  EXPECT_EQ(&object2_, manager_->GetObject(id1_));
}

TEST_F(ObjectLifecycleManagerTest, SealObject) {
  EXPECT_CALL(*object_store_, SealObject(id1_))
      .Times(1)
      .WillOnce(Return(&sealed_object_));
  EXPECT_EQ(&sealed_object_, manager_->SealObject(id1_));
}

TEST_F(ObjectLifecycleManagerTest, AbortFailure) {
  EXPECT_CALL(*object_store_, GetObject(id1_)).Times(1).WillOnce(Return(nullptr));
  EXPECT_CALL(*object_store_, GetObject(id2_)).Times(1).WillOnce(Return(&sealed_object_));
  EXPECT_EQ(manager_->AbortObject(id1_), flatbuf::PlasmaError::ObjectNonexistent);
  EXPECT_EQ(manager_->AbortObject(id2_), flatbuf::PlasmaError::ObjectSealed);
}

TEST_F(ObjectLifecycleManagerTest, AbortSuccess) {
  EXPECT_CALL(*object_store_, GetObject(id3_))
      .Times(2)
      .WillRepeatedly(Return(&not_sealed_object_));
  EXPECT_CALL(*object_store_, DeleteObject(id3_)).Times(1).WillOnce(Return(true));
  EXPECT_CALL(*eviction_policy_, RemoveObject(id3_)).Times(1).WillOnce(Return());
  EXPECT_EQ(manager_->AbortObject(id3_), flatbuf::PlasmaError::OK);
  // aborted object is not notified.
  EXPECT_TRUE(notify_deleted_ids_.empty());
}

TEST_F(ObjectLifecycleManagerTest, DeleteFailure) {
  EXPECT_CALL(*object_store_, GetObject(id1_)).Times(1).WillOnce(Return(nullptr));
  EXPECT_EQ(flatbuf::PlasmaError::ObjectNonexistent, manager_->DeleteObject(id1_));

  {
    EXPECT_CALL(*object_store_, GetObject(id2_))
        .Times(1)
        .WillOnce(Return(&not_sealed_object_));
    EXPECT_EQ(flatbuf::PlasmaError::ObjectNotSealed, manager_->DeleteObject(id2_));
    absl::flat_hash_set<ObjectID> expected_eagerly_deletion_objects{id2_};
    EXPECT_EQ(expected_eagerly_deletion_objects, manager_->earger_deletion_objects_);
  }

  {
    manager_->earger_deletion_objects_.clear();
    EXPECT_CALL(*object_store_, GetObject(id3_))
        .Times(1)
        .WillOnce(Return(&one_ref_object_));
    EXPECT_EQ(flatbuf::PlasmaError::ObjectInUse, manager_->DeleteObject(id3_));
    absl::flat_hash_set<ObjectID> expected_eagerly_deletion_objects{id3_};
    EXPECT_EQ(expected_eagerly_deletion_objects, manager_->earger_deletion_objects_);
  }
}

TEST_F(ObjectLifecycleManagerTest, DeleteSuccess) {
  EXPECT_CALL(*object_store_, GetObject(id1_))
      .Times(2)
      .WillRepeatedly(Return(&sealed_object_));
  EXPECT_CALL(*object_store_, DeleteObject(id1_)).Times(1).WillOnce(Return(true));
  EXPECT_CALL(*eviction_policy_, RemoveObject(id1_)).Times(1).WillOnce(Return());

  EXPECT_EQ(flatbuf::PlasmaError::OK, manager_->DeleteObject(id1_));
  std::vector<ObjectID> expect_notified_ids{id1_};
  EXPECT_EQ(expect_notified_ids, notify_deleted_ids_);
}

TEST_F(ObjectLifecycleManagerTest, AddReference) {
  {
    EXPECT_CALL(*object_store_, GetObject(id1_)).Times(1).WillOnce(Return(nullptr));
    EXPECT_FALSE(manager_->AddReference(id1_));
  }

  {
    EXPECT_CALL(*object_store_, GetObject(id2_)).Times(1).WillOnce(Return(&object1_));
    EXPECT_CALL(*eviction_policy_, BeginObjectAccess(id2_)).Times(1).WillOnce(Return());
    EXPECT_TRUE(manager_->AddReference(id2_));
    EXPECT_EQ(1, object1_.GetRefCount());
  }

  {
    EXPECT_CALL(*object_store_, GetObject(id3_))
        .Times(1)
        .WillOnce(Return(&one_ref_object_));
    EXPECT_TRUE(manager_->AddReference(id3_));
    EXPECT_EQ(2, one_ref_object_.GetRefCount());
  }
}

TEST_F(ObjectLifecycleManagerTest, RemoveReferenceFailure) {
  {
    EXPECT_CALL(*object_store_, GetObject(id1_)).Times(1).WillOnce(Return(nullptr));
    EXPECT_FALSE(manager_->RemoveReference(id1_));
  }

  {
    EXPECT_CALL(*object_store_, GetObject(id2_)).Times(1).WillOnce(Return(&object1_));
    EXPECT_FALSE(manager_->RemoveReference(id2_));
  }
}

TEST_F(ObjectLifecycleManagerTest, RemoveReferenceTwoRef) {
  EXPECT_CALL(*object_store_, GetObject(id1_))
      .Times(1)
      .WillOnce(Return(&two_ref_object_));
  EXPECT_TRUE(manager_->RemoveReference(id1_));
  EXPECT_EQ(1, two_ref_object_.GetRefCount());
}

TEST_F(ObjectLifecycleManagerTest, RemoveReferenceOneRefSealed) {
  EXPECT_TRUE(one_ref_object_.Sealed());
  EXPECT_CALL(*object_store_, GetObject(id1_))
      .Times(1)
      .WillOnce(Return(&one_ref_object_));
  EXPECT_CALL(*eviction_policy_, EndObjectAccess(id1_)).Times(1).WillOnce(Return());
  EXPECT_TRUE(manager_->RemoveReference(id1_));
  EXPECT_EQ(0, one_ref_object_.GetRefCount());
}

TEST_F(ObjectLifecycleManagerTest, RemoveReferenceOneRefEagerlyDeletion) {
  manager_->earger_deletion_objects_.emplace(id1_);

  EXPECT_CALL(*object_store_, GetObject(id1_))
      .Times(2)
      .WillRepeatedly(Return(&one_ref_object_));
  EXPECT_CALL(*eviction_policy_, EndObjectAccess(id1_)).Times(1).WillOnce(Return());
  EXPECT_CALL(*object_store_, DeleteObject(id1_)).Times(1).WillOnce(Return(true));
  EXPECT_CALL(*eviction_policy_, RemoveObject(id1_)).Times(1).WillOnce(Return());

  EXPECT_TRUE(manager_->RemoveReference(id1_));
  EXPECT_EQ(0, one_ref_object_.GetRefCount());

  std::vector<ObjectID> expect_notified_ids{id1_};
  EXPECT_EQ(expect_notified_ids, notify_deleted_ids_);
}
}  // namespace plasma

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
