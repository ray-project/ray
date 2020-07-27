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
    GenCheckpointData();
  }

  void GenCheckpointData() {
    for (const auto item : id_to_data_) {
      const ActorID &id = item.first;
      ActorCheckpointList checkpoints;
      for (size_t i = 0; i < checkpoint_number_; ++i) {
        ActorCheckpointID checkpoint_id = ActorCheckpointID::FromRandom();
        auto checkpoint = std::make_shared<ActorCheckpointData>();
        checkpoint->set_actor_id(id.Binary());
        checkpoint->set_checkpoint_id(checkpoint_id.Binary());
        checkpoint->set_execution_dependency(checkpoint_id.Binary());
        checkpoints.emplace_back(checkpoint);
      }
      id_to_checkpoints_[id] = std::move(checkpoints);
    }
  }

  typedef std::vector<std::shared_ptr<ActorCheckpointData>> ActorCheckpointList;
  std::unordered_map<ActorID, ActorCheckpointList> id_to_checkpoints_;
  size_t checkpoint_number_{2};
};

TEST_F(ActorInfoAccessorTest, RegisterAndGet) {
  ActorInfoAccessor &actor_accessor = gcs_client_->Actors();
  // register
  for (const auto &elem : id_to_data_) {
    const auto &actor = elem.second;
    ++pending_count_;
    RAY_CHECK_OK(actor_accessor.AsyncRegister(actor, [this](Status status) {
      RAY_CHECK_OK(status);
      --pending_count_;
    }));
  }

  WaitPendingDone(wait_pending_timeout_);

  // async get
  for (const auto &elem : id_to_data_) {
    ++pending_count_;
    RAY_CHECK_OK(actor_accessor.AsyncGet(
        elem.first, [this](Status status, const boost::optional<ActorTableData> &data) {
          ASSERT_TRUE(data);
          ActorID actor_id = ActorID::FromBinary(data->actor_id());
          auto it = id_to_data_.find(actor_id);
          ASSERT_TRUE(it != id_to_data_.end());
          --pending_count_;
        }));
  }

  WaitPendingDone(wait_pending_timeout_);

  // sync get
  std::vector<ActorTableData> actor_table_data_list;
  RAY_CHECK_OK(actor_accessor.GetAll(&actor_table_data_list));
  ASSERT_EQ(id_to_data_.size(), actor_table_data_list.size());
  for (auto &data : actor_table_data_list) {
    ActorID actor_id = ActorID::FromBinary(data.actor_id());
    ASSERT_TRUE(id_to_data_.count(actor_id) != 0);
  }
}

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

  // register
  std::atomic<int> register_pending_count(0);
  for (const auto &elem : id_to_data_) {
    const auto &actor = elem.second;
    ++sub_pending_count;
    ++register_pending_count;
    RAY_CHECK_OK(
        actor_accessor.AsyncRegister(actor, [&register_pending_count](Status status) {
          RAY_CHECK_OK(status);
          --register_pending_count;
        }));
  }
  // Wait until register finishes.
  WaitPendingDone(register_pending_count, wait_pending_timeout_);

  // Wait for all subscribe notifications.
  WaitPendingDone(sub_pending_count, wait_pending_timeout_);
}

TEST_F(ActorInfoAccessorTest, GetActorCheckpointTest) {
  ActorInfoAccessor &actor_accessor = gcs_client_->Actors();
  auto on_add_done = [this](Status status) {
    RAY_CHECK_OK(status);
    --pending_count_;
  };
  for (size_t index = 0; index < checkpoint_number_; ++index) {
    for (const auto &actor_checkpoints : id_to_checkpoints_) {
      const ActorCheckpointList &checkpoints = actor_checkpoints.second;
      const auto &checkpoint = checkpoints[index];
      ++pending_count_;
      Status status = actor_accessor.AsyncAddCheckpoint(checkpoint, on_add_done);
      RAY_CHECK_OK(status);
    }
    WaitPendingDone(wait_pending_timeout_);
  }

  for (const auto &actor_checkpoints : id_to_checkpoints_) {
    const ActorCheckpointList &checkpoints = actor_checkpoints.second;
    for (const auto &checkpoint : checkpoints) {
      ActorCheckpointID checkpoint_id =
          ActorCheckpointID::FromBinary(checkpoint->checkpoint_id());
      auto on_get_done = [this, checkpoint_id](
                             Status status,
                             const boost::optional<ActorCheckpointData> &result) {
        RAY_CHECK(result);
        ActorCheckpointID result_checkpoint_id =
            ActorCheckpointID::FromBinary(result->checkpoint_id());
        ASSERT_EQ(checkpoint_id, result_checkpoint_id);
        --pending_count_;
      };
      ++pending_count_;
      Status status = actor_accessor.AsyncGetCheckpoint(
          checkpoint_id, ActorID::FromBinary(checkpoint->actor_id()), on_get_done);
      RAY_CHECK_OK(status);
    }
  }
  WaitPendingDone(wait_pending_timeout_);

  for (const auto &actor_checkpoints : id_to_checkpoints_) {
    const ActorID &actor_id = actor_checkpoints.first;
    const ActorCheckpointList &checkpoints = actor_checkpoints.second;
    auto on_get_done = [this, &checkpoints](
                           Status status,
                           const boost::optional<ActorCheckpointIdData> &result) {
      RAY_CHECK(result);
      ASSERT_EQ(checkpoints.size(), result->checkpoint_ids_size());
      for (size_t i = 0; i < checkpoints.size(); ++i) {
        const std::string checkpoint_id_str = checkpoints[i]->checkpoint_id();
        const std::string &result_checkpoint_id_str = result->checkpoint_ids(i);
        ASSERT_EQ(checkpoint_id_str, result_checkpoint_id_str);
      }
      --pending_count_;
    };
    ++pending_count_;
    Status status = actor_accessor.AsyncGetCheckpointID(actor_id, on_get_done);
    RAY_CHECK_OK(status);
  }
  WaitPendingDone(wait_pending_timeout_);
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
