
#include "ray/object_manager/pull_manager.h"

#include "gtest/gtest.h"
#include "ray/common/test_util.h"

namespace ray {

class PullManagerTest : public ::testing::Test {
 public:
  PullManagerTest()
      : self_node_id_(NodeID::FromRandom()),
        object_is_local_(false),
        num_send_pull_request_calls_(0),
        num_restore_spilled_object_calls_(0),
        fake_time_(0),
        pull_manager_(self_node_id_,
                      [this](const ObjectID &object_id) { return object_is_local_; },
                      [this](const ObjectID &object_id, const NodeID &node_id) {
                        num_send_pull_request_calls_++;
                      },
                      [this](const ObjectID &, const std::string &,
                             std::function<void(const ray::Status &)>) {
                        num_restore_spilled_object_calls_++;
                      },
                      [this]() { return fake_time_; }, 10000) {}

  NodeID self_node_id_;
  bool object_is_local_;
  int num_send_pull_request_calls_;
  int num_restore_spilled_object_calls_;
  double fake_time_;
  PullManager pull_manager_;
};

TEST_F(PullManagerTest, TestStaleSubscription) {
  ObjectID obj1 = ObjectID::FromRandom();
  rpc::Address addr1;
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 0);
  pull_manager_.Pull(obj1, addr1);
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 1);

  std::unordered_set<NodeID> client_ids;
  pull_manager_.OnLocationChange(obj1, client_ids, "");

  // There are no client ids to pull from.
  ASSERT_EQ(num_send_pull_request_calls_, 0);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);

  pull_manager_.CancelPull(obj1);

  ASSERT_EQ(num_send_pull_request_calls_, 0);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 0);

  client_ids.insert(NodeID::FromRandom());
  pull_manager_.OnLocationChange(obj1, client_ids, "");

  // Now we're getting a notification about an object that was already cancelled.
  ASSERT_EQ(num_send_pull_request_calls_, 0);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 0);
}

TEST_F(PullManagerTest, TestRestoreSpilledObject) {
  ObjectID obj1 = ObjectID::FromRandom();
  rpc::Address addr1;
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 0);
  pull_manager_.Pull(obj1, addr1);
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 1);

  std::unordered_set<NodeID> client_ids;
  pull_manager_.OnLocationChange(obj1, client_ids, "remote_url/foo/bar");

  // client_ids is empty here, so there's nowhere to pull from.
  ASSERT_EQ(num_send_pull_request_calls_, 0);
  ASSERT_EQ(num_restore_spilled_object_calls_, 1);

  client_ids.insert(NodeID::FromRandom());
  pull_manager_.OnLocationChange(obj1, client_ids, "remote_url/foo/bar");

  // The behavior is supposed to be to always restore the spilled object if possible (even
  // if it exists elsewhere in the cluster).
  ASSERT_EQ(num_send_pull_request_calls_, 0);
  ASSERT_EQ(num_restore_spilled_object_calls_, 2);

  pull_manager_.CancelPull(obj1);
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 0);
}

TEST_F(PullManagerTest, TestManyUpdates) {
  ObjectID obj1 = ObjectID::FromRandom();
  rpc::Address addr1;
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 0);
  pull_manager_.Pull(obj1, addr1);
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 1);

  std::unordered_set<NodeID> client_ids;
  client_ids.insert(NodeID::FromRandom());

  for (int i = 0; i < 100; i++) {
    pull_manager_.OnLocationChange(obj1, client_ids, "");
  }

  ASSERT_EQ(num_send_pull_request_calls_, 100);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);

  pull_manager_.CancelPull(obj1);
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 0);
}

TEST_F(PullManagerTest, TestRetryTimer) {
  ObjectID obj1 = ObjectID::FromRandom();
  rpc::Address addr1;
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 0);
  pull_manager_.Pull(obj1, addr1);
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 1);

  std::unordered_set<NodeID> client_ids;
  client_ids.insert(NodeID::FromRandom());

  // We need to call OnLocationChange at least once, to population the list of nodes with
  // the object.
  pull_manager_.OnLocationChange(obj1, client_ids, "");
  ASSERT_EQ(num_send_pull_request_calls_, 1);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);

  for (; fake_time_ <= 127 * 10; fake_time_ += 0.1) {
    pull_manager_.Tick();
  }

  // Rapid set of location changes.
  for (int i = 0; i < 127; i++) {
    fake_time_ += 0.1;
    pull_manager_.OnLocationChange(obj1, client_ids, "");
  }

  // We should make a pull request every tick (even if it's a duplicate to a node we're
  // already pulling from).
  // OnLocationChange also doesn't count towards the retry timer.
  // To the casual observer, this may seem off-by-one, but this is due to floating point
  // error (0.1 + 0.1 ... 10k times > 10 == True)
  ASSERT_EQ(num_send_pull_request_calls_, 127 * 2);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);

  pull_manager_.CancelPull(obj1);
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 0);
}

TEST_F(PullManagerTest, TestBasic) {
  ObjectID obj1 = ObjectID::FromRandom();
  rpc::Address addr1;
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 0);
  pull_manager_.Pull(obj1, addr1);
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 1);

  std::unordered_set<NodeID> client_ids;
  client_ids.insert(NodeID::FromRandom());
  pull_manager_.OnLocationChange(obj1, client_ids, "");

  ASSERT_EQ(num_send_pull_request_calls_, 1);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);

  pull_manager_.CancelPull(obj1);
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 0);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
