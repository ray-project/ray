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

#include "ray/gcs/gcs_server/gcs_object_manager.h"

#include "gtest/gtest.h"
#include "ray/gcs/gcs_server/test/gcs_server_test_util.h"
#include "ray/gcs/test/gcs_test_util.h"

namespace ray {

class MockedGcsObjectManager : public gcs::GcsObjectManager {
 public:
  explicit MockedGcsObjectManager(std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
                                  std::shared_ptr<gcs::GcsPubSub> &gcs_pub_sub,
                                  gcs::GcsNodeManager &gcs_node_manager)
      : gcs::GcsObjectManager(gcs_table_storage, gcs_pub_sub, gcs_node_manager) {}

 public:
  void AddObjectsLocation(const ClientID &node_id,
                          const absl::flat_hash_set<ObjectID> &object_ids) {
    gcs::GcsObjectManager::AddObjectsLocation(node_id, object_ids);
  }

  void AddObjectLocationInCache(const ObjectID &object_id, const ClientID &node_id) {
    gcs::GcsObjectManager::AddObjectLocationInCache(object_id, node_id);
  }

  absl::flat_hash_set<ClientID> GetObjectLocations(const ObjectID &object_id) {
    return gcs::GcsObjectManager::GetObjectLocations(object_id);
  }

  void OnNodeRemoved(const ClientID &node_id) {
    gcs::GcsObjectManager::OnNodeRemoved(node_id);
  }

  void RemoveObjectLocationInCache(const ObjectID &object_id, const ClientID &node_id) {
    gcs::GcsObjectManager::RemoveObjectLocationInCache(object_id, node_id);
  }
};

class GcsObjectManagerTest : public ::testing::Test {
 public:
  void SetUp() override {
    gcs_table_storage_ = std::make_shared<gcs::InMemoryGcsTableStorage>(io_service_);
    gcs_node_manager_ = std::make_shared<gcs::GcsNodeManager>(
        io_service_, io_service_, error_info_accessor_, gcs_pub_sub_, gcs_table_storage_);
    gcs_object_manager_ = std::make_shared<MockedGcsObjectManager>(
        gcs_table_storage_, gcs_pub_sub_, *gcs_node_manager_);
    GenTestData();
  }

  void GenTestData() {
    for (size_t i = 0; i < object_count_; ++i) {
      ObjectID object_id = ObjectID::FromRandom();
      object_ids_.emplace(object_id);
    }
    for (size_t i = 0; i < node_count_; ++i) {
      ClientID node_id = ClientID::FromRandom();
      node_ids_.emplace(node_id);
    }
  }

  void CheckLocations(const absl::flat_hash_set<ClientID> &locations) {
    ASSERT_EQ(locations.size(), node_ids_.size());
    for (const auto &location : locations) {
      auto it = node_ids_.find(location);
      ASSERT_TRUE(it != node_ids_.end());
      ASSERT_TRUE(location == *it);
    }
  }

 protected:
  boost::asio::io_service io_service_;
  GcsServerMocker::MockedErrorInfoAccessor error_info_accessor_;
  std::shared_ptr<gcs::GcsNodeManager> gcs_node_manager_;
  std::shared_ptr<gcs::RedisGcsClient> gcs_client_;
  std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub_;
  std::shared_ptr<MockedGcsObjectManager> gcs_object_manager_;
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;

  size_t object_count_{5};
  size_t node_count_{10};
  absl::flat_hash_set<ObjectID> object_ids_;
  absl::flat_hash_set<ClientID> node_ids_;
};

TEST_F(GcsObjectManagerTest, AddObjectsLocationAndGetLocationTest) {
  for (const auto &node_id : node_ids_) {
    gcs_object_manager_->AddObjectsLocation(node_id, object_ids_);
  }
  for (const auto &object_id : object_ids_) {
    auto locations = gcs_object_manager_->GetObjectLocations(object_id);
    CheckLocations(locations);
  }
}

TEST_F(GcsObjectManagerTest, AddObjectLocationInCacheTest) {
  for (const auto &object_id : object_ids_) {
    for (const auto &node_id : node_ids_) {
      gcs_object_manager_->AddObjectLocationInCache(object_id, node_id);
    }
  }

  for (const auto &object_id : object_ids_) {
    auto locations = gcs_object_manager_->GetObjectLocations(object_id);
    CheckLocations(locations);
  }
}

TEST_F(GcsObjectManagerTest, RemoveNodeTest) {
  for (const auto &node_id : node_ids_) {
    gcs_object_manager_->AddObjectsLocation(node_id, object_ids_);
  }

  gcs_object_manager_->OnNodeRemoved(*node_ids_.begin());
  auto locations = gcs_object_manager_->GetObjectLocations(*object_ids_.begin());
  ASSERT_EQ(locations.size() + 1, node_ids_.size());

  locations.emplace(*node_ids_.begin());
  ASSERT_EQ(locations.size(), node_ids_.size());
}

TEST_F(GcsObjectManagerTest, RemoveObjectLocationTest) {
  for (const auto &node_id : node_ids_) {
    gcs_object_manager_->AddObjectsLocation(node_id, object_ids_);
  }

  gcs_object_manager_->RemoveObjectLocationInCache(*object_ids_.begin(),
                                                   *node_ids_.begin());
  auto locations = gcs_object_manager_->GetObjectLocations(*object_ids_.begin());
  ASSERT_EQ(locations.size() + 1, node_ids_.size());

  locations.emplace(*node_ids_.begin());
  ASSERT_EQ(locations.size(), node_ids_.size());
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
