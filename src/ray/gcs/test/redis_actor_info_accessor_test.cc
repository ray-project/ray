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

#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "ray/common/test_util.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/gcs/test/accessor_test_base.h"

namespace ray {

namespace gcs {

class ActorInfoAccessorTest : public AccessorTestBase<ActorID, ActorTableData> {
 protected:
  virtual void GenTestData() {
    for (size_t i = 0; i < 100; ++i) {
      std::shared_ptr<ActorTableData> actor = std::make_shared<ActorTableData>();
      actor->set_max_restarts(1);
      actor->set_num_restarts(0);
      JobID job_id = JobID::FromInt(i);
      actor->set_job_id(job_id.Binary());
      actor->set_state(ActorTableData::ALIVE);
      ActorID actor_id = ActorID::Of(job_id, RandomTaskId(), /*parent_task_counter=*/i);
      actor->set_actor_id(actor_id.Binary());
      id_to_data_[actor_id] = actor;
    }
  }

  size_t checkpoint_number_{2};
};

TEST_F(ActorInfoAccessorTest, Subscribe) {
  ActorInfoAccessor &actor_accessor = gcs_client_->Actors();
  // subscribe
  std::atomic<int> sub_pending_count(0);
  std::atomic<int> do_sub_pending_count(0);
  auto subscribe = [this, &sub_pending_count](const ActorID &actor_id,
                                              const ActorTableData &data) {
    const auto it = id_to_data_.find(actor_id);
    ASSERT_TRUE(it != id_to_data_.end());
    --sub_pending_count;
  };
  auto done = [&do_sub_pending_count](Status status) {
    RAY_CHECK_OK(status);
    --do_sub_pending_count;
  };

  ++do_sub_pending_count;
  RAY_CHECK_OK(actor_accessor.AsyncSubscribeAll(subscribe, done));
  // Wait until subscribe finishes.
  WaitPendingDone(do_sub_pending_count, wait_pending_timeout_);
}

}  // namespace gcs

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 4);
  ray::TEST_REDIS_SERVER_EXEC_PATH = argv[1];
  ray::TEST_REDIS_CLIENT_EXEC_PATH = argv[2];
  ray::TEST_REDIS_MODULE_LIBRARY_PATH = argv[3];
  return RUN_ALL_TESTS();
}
