#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>
#include "gtest/gtest.h"
#include "ray/gcs/accessor_test_base.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/util/test_util.h"

namespace ray {

namespace gcs {

class ActorStateAccessorTest : public AccessorTestBase<ActorID, ActorTableData> {
 protected:
  virtual void GenTestData() {
    for (size_t i = 0; i < 100; ++i) {
      std::shared_ptr<ActorTableData> actor = std::make_shared<ActorTableData>();
      actor->set_max_reconstructions(1);
      actor->set_remaining_reconstructions(1);
      JobID job_id = JobID::FromInt(i);
      actor->set_job_id(job_id.Binary());
      actor->set_state(ActorTableData::ALIVE);
      ActorID actor_id = ActorID::Of(job_id, RandomTaskId(), /*parent_task_counter=*/i);
      actor->set_actor_id(actor_id.Binary());
      id_to_data_[actor_id] = actor;
    }
  }
};

TEST_F(ActorStateAccessorTest, Subscribe) {
  ActorStateAccessor &actor_accessor = gcs_client_->Actors();
  // subscribe
  std::atomic<int> sub_pending_count(0);
  std::atomic<int> do_sub_pending_count(0);
  auto subscribe = [this, &sub_pending_count](const ActorID &actor_id,
                                              const ActorTableData &data) {
    const auto it = id_to_data_.find(actor_id);
    ASSERT_TRUE(it != id_to_data_.end());
    --sub_pending_count;
    RAY_LOG(INFO) << "Finished executing 'subscribe' callback";
  };
  auto done = [&do_sub_pending_count](Status status) {
    RAY_CHECK_OK(status);
    --do_sub_pending_count;
    RAY_LOG(INFO) << "Finished executing 'done' callback";
  };

  ++do_sub_pending_count;
  RAY_LOG(INFO) << "Registering 'subscribe' callback and 'done' callback";
  RAY_CHECK_OK(actor_accessor.AsyncSubscribe(subscribe, done));
  // Wait until subscribe finishes.
  WaitPendingDone(do_sub_pending_count, wait_pending_timeout_);

  // register
  std::atomic<int> register_pending_count(0);
  int idx = 0;
  for (const auto &elem : id_to_data_) {
    RAY_LOG(INFO) << "Loop index: " << idx++;
    const auto &actor = elem.second;
    ++sub_pending_count;
    ++register_pending_count;
    RAY_LOG(INFO) << "Registering inlined callback";
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

}  // namespace gcs

}  // namespace ray
