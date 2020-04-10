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

class RedisAccessorMultiJobTest : public AccessorTestBase<ObjectID, ObjectTableData> {
 protected:
  void GenTestData() {}
};

TEST_F(RedisAccessorMultiJobTest, TestJobTable) {
  ObjectInfoAccessor &object_accessor = gcs_client_->Objects();
  JobID job1_id = JobID::FromInt(1);
  JobID job2_id = JobID::FromInt(2);
  TaskID task1_id = TaskID::ForDriverTask(job1_id);
  TaskID task2_id = TaskID::ForDriverTask(job1_id);
  TaskID task3_id = TaskID::ForDriverTask(job2_id);
  ObjectID object1_id = ObjectID::ForPut(task1_id, 1, 1);
  ObjectID object2_id = ObjectID::ForPut(task2_id, 2, 1);
  ObjectID object3_id = ObjectID::ForPut(task3_id, 3, 1);

  ClientID node1_id = ClientID::FromRandom();
  ClientID node2_id = ClientID::FromRandom();
  auto on_done = [this](Status status) { --pending_count_; };
  pending_count_ += 3;
  RAY_CHECK_OK(object_accessor.AsyncAddLocation(object1_id, node1_id, on_done));
  RAY_CHECK_OK(object_accessor.AsyncAddLocation(object2_id, node1_id, on_done));
  RAY_CHECK_OK(object_accessor.AsyncAddLocation(object3_id, node2_id, on_done));
  WaitPendingDone(wait_pending_timeout_);

  // Get object id of node by job.
  auto callback = [this](Status status, const std::vector<ObjectID> &result) {
    RAY_LOG(INFO) << "Hello, get result size = " << result.size();
    for (auto &object_id : result) {
      RAY_LOG(INFO) << "Hello , get object id = " << object_id
                    << ", size = " << object_id.Binary().size();
    }
    --pending_count_;
  };
  ++pending_count_;
  RAY_CHECK_OK(object_accessor.AsyncGetObjectIdsOfNodeByJob(job2_id, node1_id, callback));
  WaitPendingDone(wait_pending_timeout_);
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
