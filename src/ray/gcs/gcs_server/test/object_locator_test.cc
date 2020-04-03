#include "ray/gcs/gcs_server/object_locator.h"
#include "gtest/gtest.h"

namespace ray {

namespace gcs {

class ObjectLocatorTest : public ::testing::Test {
 public:
  ObjectLocatorTest() {}

  void SetUp() override { GenTestData(); }

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

  void CheckLocations(const std::unordered_set<ClientID> &locations) {
    ASSERT_EQ(locations.size(), node_ids_.size());
    for (const auto &location : locations) {
      auto it = node_ids_.find(location);
      ASSERT_TRUE(it != node_ids_.end());
      ASSERT_TRUE(location == *it);
    }
  }

 protected:
  ObjectLocator object_locator_;

  size_t object_count_{5};
  size_t node_count_{10};
  std::unordered_set<ObjectID> object_ids_;
  std::unordered_set<ClientID> node_ids_;
};

TEST_F(ObjectLocatorTest, AddObjectsLocationAndGetLocationTest) {
  for (const auto &node_id : node_ids_) {
    object_locator_.AddObjectsLocation(node_id, object_ids_);
  }
  for (const auto &object_id : object_ids_) {
    auto locations = object_locator_.GetObjectLocations(object_id);
    CheckLocations(locations);
  }
}

TEST_F(ObjectLocatorTest, AddObjectLocationTest) {
  for (const auto &object_id : object_ids_) {
    for (const auto &node_id : node_ids_) {
      object_locator_.AddObjectLocation(object_id, node_id);
    }
  }

  for (const auto &object_id : object_ids_) {
    auto locations = object_locator_.GetObjectLocations(object_id);
    CheckLocations(locations);
  }
}

TEST_F(ObjectLocatorTest, RemoveNodeTest) {
  for (const auto &node_id : node_ids_) {
    object_locator_.AddObjectsLocation(node_id, object_ids_);
  }

  object_locator_.RemoveNode(*node_ids_.begin());
  auto locations = object_locator_.GetObjectLocations(*object_ids_.begin());
  ASSERT_EQ(locations.size() + 1, node_ids_.size());

  locations.emplace(*node_ids_.begin());
  ASSERT_EQ(locations.size(), node_ids_.size());
}

TEST_F(ObjectLocatorTest, RemoveObjectLocationTest) {
  for (const auto &node_id : node_ids_) {
    object_locator_.AddObjectsLocation(node_id, object_ids_);
  }

  object_locator_.RemoveObjectLocation(*object_ids_.begin(), *node_ids_.begin());
  auto locations = object_locator_.GetObjectLocations(*object_ids_.begin());
  ASSERT_EQ(locations.size() + 1, node_ids_.size());

  locations.emplace(*node_ids_.begin());
  ASSERT_EQ(locations.size(), node_ids_.size());
}

}  // namespace gcs

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
