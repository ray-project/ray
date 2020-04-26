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

#include <memory>

#include "gtest/gtest.h"
#include "ray/common/test_util.h"
#include "ray/gcs/pb_util.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/gcs/test/accessor_test_base.h"

namespace ray {

namespace gcs {

class RedisJobInfoAccessorTest : public AccessorTestBase<JobID, JobTableData> {
 protected:
  virtual void GenTestData() {
    for (size_t i = 0; i < total_job_number_; ++i) {
      JobID job_id = JobID::FromInt(i);
      std::shared_ptr<JobTableData> job_data_ptr =
          CreateJobTableData(job_id, /*is_dead*/ false, /*timestamp*/ 1,
                             /*driver_ip_address*/ "", /*driver_pid*/ i);
      id_to_data_[job_id] = job_data_ptr;
    }
  }
  std::atomic<int> subscribe_pending_count_{0};
  size_t total_job_number_{100};
};

TEST_F(RedisJobInfoAccessorTest, AddAndSubscribe) {
  JobInfoAccessor &job_accessor = gcs_client_->Jobs();
  // SubscribeAll
  auto on_subscribe = [this](const JobID &job_id, const JobTableData &data) {
    const auto it = id_to_data_.find(job_id);
    RAY_CHECK(it != id_to_data_.end());
    ASSERT_TRUE(data.is_dead());
    --subscribe_pending_count_;
  };

  auto on_done = [this](Status status) {
    RAY_CHECK_OK(status);
    --pending_count_;
  };

  ++pending_count_;
  RAY_CHECK_OK(job_accessor.AsyncSubscribeToFinishedJobs(on_subscribe, on_done));

  WaitPendingDone(wait_pending_timeout_);
  WaitPendingDone(subscribe_pending_count_, wait_pending_timeout_);

  // Register
  for (const auto &item : id_to_data_) {
    ++pending_count_;
    RAY_CHECK_OK(job_accessor.AsyncAdd(item.second, [this](Status status) {
      RAY_CHECK_OK(status);
      --pending_count_;
    }));
  }
  WaitPendingDone(wait_pending_timeout_);
  WaitPendingDone(subscribe_pending_count_, wait_pending_timeout_);

  // Update
  for (auto &item : id_to_data_) {
    ++pending_count_;
    ++subscribe_pending_count_;
    RAY_CHECK_OK(job_accessor.AsyncMarkFinished(item.first, [this](Status status) {
      RAY_CHECK_OK(status);
      --pending_count_;
    }));
  }
  WaitPendingDone(wait_pending_timeout_);
  WaitPendingDone(subscribe_pending_count_, wait_pending_timeout_);
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
