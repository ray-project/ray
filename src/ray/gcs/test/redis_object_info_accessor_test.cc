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

#include <unordered_map>
#include <vector>

#include "gtest/gtest.h"
#include "ray/common/test_util.h"
#include "ray/gcs/redis_accessor.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/gcs/test/accessor_test_base.h"

namespace ray {

namespace gcs {

class RedisObjectInfoAccessorTest : public AccessorTestBase<ObjectID, ObjectTableData> {
 protected:
  void GenTestData() {
    for (size_t i = 0; i < object_count_; ++i) {
      ObjectVector object_vec;
      for (size_t j = 0; j < copy_count_; ++j) {
        auto object = std::make_shared<ObjectTableData>();
        ClientID node_id = ClientID::FromRandom();
        object->set_manager(node_id.Binary());
        object_vec.emplace_back(std::move(object));
      }
      ObjectID id = ObjectID::FromRandom();
      object_id_to_data_[id] = object_vec;
    }
  }

  typedef std::vector<std::shared_ptr<ObjectTableData>> ObjectVector;
  std::unordered_map<ObjectID, ObjectVector> object_id_to_data_;

  size_t object_count_{100};
  size_t copy_count_{5};
};

TEST_F(RedisObjectInfoAccessorTest, TestGetAddRemove) {
  ObjectInfoAccessor &object_accessor = gcs_client_->Objects();
  // add && get
  // add
  for (const auto &elem : object_id_to_data_) {
    for (const auto &item : elem.second) {
      ++pending_count_;
      ClientID node_id = ClientID::FromBinary(item->manager());
      RAY_CHECK_OK(
          object_accessor.AsyncAddLocation(elem.first, node_id, [this](Status status) {
            RAY_CHECK_OK(status);
            --pending_count_;
          }));
    }
  }
  WaitPendingDone(wait_pending_timeout_);
  // get
  for (const auto &elem : object_id_to_data_) {
    ++pending_count_;
    size_t total_size = elem.second.size();
    RAY_CHECK_OK(object_accessor.AsyncGetLocations(
        elem.first,
        [this, total_size](Status status, const std::vector<ObjectTableData> &result) {
          RAY_CHECK_OK(status);
          RAY_CHECK(total_size == result.size());
          --pending_count_;
        }));
  }
  WaitPendingDone(wait_pending_timeout_);

  RAY_LOG(INFO) << "Case Add && Get done.";

  // subscribe && delete
  // subscribe
  std::atomic<int> sub_pending_count(0);
  auto subscribe = [this, &sub_pending_count](const ObjectID &object_id,
                                              const ObjectChangeNotification &result) {
    const auto it = object_id_to_data_.find(object_id);
    ASSERT_TRUE(it != object_id_to_data_.end());
    static size_t response_count = 1;
    size_t cur_count = response_count <= object_count_ ? copy_count_ : 1;
    ASSERT_EQ(result.GetData().size(), cur_count);
    rpc::GcsChangeMode change_mode = response_count <= object_count_
                                         ? rpc::GcsChangeMode::APPEND_OR_ADD
                                         : rpc::GcsChangeMode::REMOVE;
    ASSERT_EQ(change_mode, result.GetGcsChangeMode());
    ++response_count;
    --sub_pending_count;
  };
  for (const auto &elem : object_id_to_data_) {
    ++pending_count_;
    ++sub_pending_count;
    RAY_CHECK_OK(object_accessor.AsyncSubscribeToLocations(elem.first, subscribe,
                                                           [this](Status status) {
                                                             RAY_CHECK_OK(status);
                                                             --pending_count_;
                                                           }));
  }
  WaitPendingDone(wait_pending_timeout_);
  WaitPendingDone(sub_pending_count, wait_pending_timeout_);
  // delete
  for (const auto &elem : object_id_to_data_) {
    ++pending_count_;
    ++sub_pending_count;
    const ObjectVector &object_vec = elem.second;
    ClientID node_id = ClientID::FromBinary(object_vec[0]->manager());
    RAY_CHECK_OK(
        object_accessor.AsyncRemoveLocation(elem.first, node_id, [this](Status status) {
          RAY_CHECK_OK(status);
          --pending_count_;
        }));
  }
  WaitPendingDone(wait_pending_timeout_);
  WaitPendingDone(sub_pending_count, wait_pending_timeout_);
  // get
  for (const auto &elem : object_id_to_data_) {
    ++pending_count_;
    size_t total_size = elem.second.size();
    RAY_CHECK_OK(object_accessor.AsyncGetLocations(
        elem.first,
        [this, total_size](Status status, const std::vector<ObjectTableData> &result) {
          RAY_CHECK_OK(status);
          ASSERT_EQ(total_size - 1, result.size());
          --pending_count_;
        }));
  }
  WaitPendingDone(wait_pending_timeout_);

  RAY_LOG(INFO) << "Case Subscribe && Delete done.";
}

}  // namespace gcs

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 4);
  ray::REDIS_SERVER_EXEC_PATH = argv[1];
  ray::REDIS_CLIENT_EXEC_PATH = argv[2];
  ray::REDIS_MODULE_LIBRARY_PATH = argv[3];
  return RUN_ALL_TESTS();
}
