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

#include "ray/gcs/subscription_executor.h"

#include "gtest/gtest.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/entry_change_notification.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/gcs/test/accessor_test_base.h"

namespace ray {

namespace gcs {

class SubscriptionExecutorTest : public AccessorTestBase<ActorID, ActorTableData> {
 public:
  typedef SubscriptionExecutor<ActorID, ActorTableData, LogBasedActorTable>
      ActorSubExecutor;

  virtual void SetUp() {
    AccessorTestBase<ActorID, ActorTableData>::SetUp();

    actor_sub_executor_.reset(new ActorSubExecutor(gcs_client_->log_based_actor_table()));

    subscribe_ = [this](const ActorID &id, const ActorTableData &data) {
      const auto it = id_to_data_.find(id);
      ASSERT_TRUE(it != id_to_data_.end());
      --sub_pending_count_;
    };

    sub_done_ = [this](Status status) {
      ASSERT_TRUE(status.ok()) << status;
      --do_sub_pending_count_;
    };

    unsub_done_ = [this](Status status) {
      ASSERT_TRUE(status.ok()) << status;
      --do_unsub_pending_count_;
    };
  }

  virtual void TearDown() {
    AccessorTestBase<ActorID, ActorTableData>::TearDown();
    ASSERT_EQ(sub_pending_count_, 0);
    ASSERT_EQ(do_sub_pending_count_, 0);
    ASSERT_EQ(do_unsub_pending_count_, 0);
  }

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

  size_t AsyncRegisterActorToGcs() {
    ActorInfoAccessor &actor_accessor = gcs_client_->Actors();
    for (const auto &elem : id_to_data_) {
      const auto &actor = elem.second;
      auto done = [this](Status status) {
        ASSERT_TRUE(status.ok());
        --pending_count_;
      };
      ++pending_count_;
      Status status = actor_accessor.AsyncRegister(actor, done);
      RAY_CHECK_OK(status);
    }
    return id_to_data_.size();
  }

 protected:
  std::unique_ptr<ActorSubExecutor> actor_sub_executor_;

  std::atomic<int> sub_pending_count_{0};
  std::atomic<int> do_sub_pending_count_{0};
  std::atomic<int> do_unsub_pending_count_{0};

  SubscribeCallback<ActorID, ActorTableData> subscribe_{nullptr};
  StatusCallback sub_done_{nullptr};
  StatusCallback unsub_done_{nullptr};
};

TEST_F(SubscriptionExecutorTest, SubscribeAllTest) {
  ++do_sub_pending_count_;
  Status status =
      actor_sub_executor_->AsyncSubscribeAll(ClientID::Nil(), subscribe_, sub_done_);
  WaitPendingDone(do_sub_pending_count_, wait_pending_timeout_);
  ASSERT_TRUE(status.ok());
  sub_pending_count_ = id_to_data_.size();
  AsyncRegisterActorToGcs();
  status = actor_sub_executor_->AsyncSubscribeAll(ClientID::Nil(), subscribe_, sub_done_);
  ASSERT_TRUE(status.IsInvalid());
  WaitPendingDone(sub_pending_count_, wait_pending_timeout_);
}

TEST_F(SubscriptionExecutorTest, SubscribeOneWithClientIDTest) {
  const auto &item = id_to_data_.begin();
  ++do_sub_pending_count_;
  ++sub_pending_count_;
  Status status = actor_sub_executor_->AsyncSubscribe(ClientID::FromRandom(), item->first,
                                                      subscribe_, sub_done_);
  WaitPendingDone(do_sub_pending_count_, wait_pending_timeout_);
  ASSERT_TRUE(status.ok());
  AsyncRegisterActorToGcs();
  WaitPendingDone(sub_pending_count_, wait_pending_timeout_);
  status = actor_sub_executor_->AsyncSubscribe(ClientID::FromRandom(), item->first,
                                               subscribe_, sub_done_);
  ASSERT_TRUE(status.IsInvalid());
}

TEST_F(SubscriptionExecutorTest, SubscribeOneAfterActorRegistrationWithClientIDTest) {
  const auto &item = id_to_data_.begin();
  ++do_sub_pending_count_;
  ++sub_pending_count_;
  AsyncRegisterActorToGcs();
  Status status = actor_sub_executor_->AsyncSubscribe(ClientID::FromRandom(), item->first,
                                                      subscribe_, sub_done_);
  WaitPendingDone(do_sub_pending_count_, wait_pending_timeout_);
  ASSERT_TRUE(status.ok());
  WaitPendingDone(sub_pending_count_, wait_pending_timeout_);
  status = actor_sub_executor_->AsyncSubscribe(ClientID::FromRandom(), item->first,
                                               subscribe_, sub_done_);
  ASSERT_TRUE(status.IsInvalid());
}

TEST_F(SubscriptionExecutorTest, SubscribeAllAndSubscribeOneTest) {
  ++do_sub_pending_count_;
  Status status =
      actor_sub_executor_->AsyncSubscribeAll(ClientID::Nil(), subscribe_, sub_done_);
  ASSERT_TRUE(status.ok());
  WaitPendingDone(do_sub_pending_count_, wait_pending_timeout_);
  for (const auto &item : id_to_data_) {
    status = actor_sub_executor_->AsyncSubscribe(ClientID::FromRandom(), item.first,
                                                 subscribe_, sub_done_);
    ASSERT_FALSE(status.ok());
  }
  sub_pending_count_ = id_to_data_.size();
  AsyncRegisterActorToGcs();
  WaitPendingDone(sub_pending_count_, wait_pending_timeout_);
}

TEST_F(SubscriptionExecutorTest, UnsubscribeTest) {
  ClientID client_id = ClientID::FromRandom();
  Status status;
  for (const auto &item : id_to_data_) {
    status = actor_sub_executor_->AsyncUnsubscribe(client_id, item.first, unsub_done_);
    ASSERT_TRUE(status.IsInvalid());
  }

  for (const auto &item : id_to_data_) {
    ++do_sub_pending_count_;
    status =
        actor_sub_executor_->AsyncSubscribe(client_id, item.first, subscribe_, sub_done_);
    ASSERT_TRUE(status.ok());
  }
  WaitPendingDone(do_sub_pending_count_, wait_pending_timeout_);
  for (const auto &item : id_to_data_) {
    ++do_unsub_pending_count_;
    status = actor_sub_executor_->AsyncUnsubscribe(client_id, item.first, unsub_done_);
    ASSERT_TRUE(status.ok());
  }
  WaitPendingDone(do_unsub_pending_count_, wait_pending_timeout_);
  for (const auto &item : id_to_data_) {
    status = actor_sub_executor_->AsyncUnsubscribe(client_id, item.first, unsub_done_);
    ASSERT_TRUE(!status.ok());
  }

  for (const auto &item : id_to_data_) {
    ++do_sub_pending_count_;
    status =
        actor_sub_executor_->AsyncSubscribe(client_id, item.first, subscribe_, sub_done_);
    ASSERT_TRUE(status.ok());
  }
  WaitPendingDone(do_sub_pending_count_, wait_pending_timeout_);
  for (const auto &item : id_to_data_) {
    ++do_unsub_pending_count_;
    status = actor_sub_executor_->AsyncUnsubscribe(client_id, item.first, unsub_done_);
    ASSERT_TRUE(status.ok());
  }
  WaitPendingDone(do_unsub_pending_count_, wait_pending_timeout_);
  for (const auto &item : id_to_data_) {
    ++do_sub_pending_count_;
    status =
        actor_sub_executor_->AsyncSubscribe(client_id, item.first, subscribe_, sub_done_);
    ASSERT_TRUE(status.ok());
  }
  WaitPendingDone(do_sub_pending_count_, wait_pending_timeout_);
  sub_pending_count_ = id_to_data_.size();
  AsyncRegisterActorToGcs();
  WaitPendingDone(pending_count_, wait_pending_timeout_);
  WaitPendingDone(sub_pending_count_, wait_pending_timeout_);
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
