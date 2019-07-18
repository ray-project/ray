#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>
#include "gtest/gtest.h"
#include "ray/gcs/redis_gcs_client.h"

namespace ray {

namespace gcs {

class ActorStateAccessorTest : public ::testing::Test {
 public:
  ActorStateAccessorTest() : options_("127.0.0.1", 6379, "", true) {}

  virtual void SetUp() {
    GenTestData();

    gcs_client_.reset(new RedisGcsClient(options_));
    RAY_CHECK_OK(gcs_client_->Connect(io_service_));

    work_thread.reset(new std::thread([this] {
      std::auto_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(io_service_));
      io_service_.run();
    }));
  }

  virtual void TearDown() {
    gcs_client_->Disconnect();

    io_service_.stop();
    work_thread->join();
    work_thread.reset();

    gcs_client_.reset();

    ClearTestData();
  }

 protected:
  void GenTestData() { GenActorData(); }

  void GenActorData() {
    for (size_t i = 0; i < 2; ++i) {
      std::shared_ptr<ActorTableData> actor = std::make_shared<ActorTableData>();
      ActorID actor_id = ActorID::FromRandom();
      actor->set_actor_id(actor_id.Binary());
      actor->set_max_reconstructions(1);
      actor->set_remaining_reconstructions(1);
      JobID job_id = JobID::FromInt(i);
      actor->set_job_id(job_id.Binary());
      actor->set_state(ActorTableData::ALIVE);
      actor_datas_[actor_id] = actor;
    }
  }

  void ClearTestData() { actor_datas_.clear(); }

  void WaitPendingDone(std::chrono::milliseconds timeout) {
    WaitPendingDone(pending_count_, timeout);
  }

  void WaitPendingDone(std::atomic<int> &pending_count,
                       std::chrono::milliseconds timeout) {
    while (pending_count != 0 && timeout.count() > 0) {
      std::chrono::milliseconds interval(10);
      std::this_thread::sleep_for(interval);
      timeout -= interval;
    }
    EXPECT_EQ(pending_count, 0);
  }

 protected:
  GcsClientOptions options_;
  std::unique_ptr<RedisGcsClient> gcs_client_;

  boost::asio::io_service io_service_;
  std::unique_ptr<std::thread> work_thread;

  std::unordered_map<ActorID, std::shared_ptr<ActorTableData>> actor_datas_;

  std::atomic<int> pending_count_{0};
};

TEST_F(ActorStateAccessorTest, RegisterAndGet) {
  ActorStateAccessor &actor_accessor = gcs_client_->Actors();
  // register
  for (const auto &elem : actor_datas_) {
    const auto &actor = elem.second;
    ++pending_count_;
    actor_accessor.AsyncRegister(actor, [this](Status status) {
      RAY_CHECK_OK(status);
      --pending_count_;
    });
  }

  std::chrono::milliseconds timeout(10000);
  WaitPendingDone(timeout);

  // get
  for (const auto &elem : actor_datas_) {
    const auto &actor = elem.second;
    ++pending_count_;
    actor_accessor.AsyncGet(elem.first,
                            [this](Status status, std::vector<ActorTableData> datas) {
                              ASSERT_EQ(datas.size(), 1U);
                              ActorID actor_id = ActorID::FromBinary(datas[0].actor_id());
                              auto it = actor_datas_.find(actor_id);
                              ASSERT_TRUE(it != actor_datas_.end());
                              --pending_count_;
                            });
  }

  WaitPendingDone(timeout);
}

TEST_F(ActorStateAccessorTest, Subscribe) {
  ActorStateAccessor &actor_accessor = gcs_client_->Actors();
  std::chrono::milliseconds timeout(10000);
  // subscribe
  std::atomic<int> sub_pending_count(0);
  std::atomic<int> do_sub_pending_count(0);
  auto subscribe = [this, &sub_pending_count](const ActorID &actor_id,
                                              const ActorTableData &data) {
    const auto it = actor_datas_.find(actor_id);
    ASSERT_TRUE(it != actor_datas_.end());
    --sub_pending_count;
  };
  auto done = [&do_sub_pending_count](Status status) {
    RAY_CHECK_OK(status);
    --do_sub_pending_count;
  };

  ++do_sub_pending_count;
  actor_accessor.AsyncSubscribe(subscribe, done);
  // Wait until subscribe finishes.
  WaitPendingDone(do_sub_pending_count, timeout);

  // register
  std::atomic<int> register_pending_count(0);
  for (const auto &elem : actor_datas_) {
    const auto &actor = elem.second;
    ++sub_pending_count;
    ++register_pending_count;
    actor_accessor.AsyncRegister(actor, [&register_pending_count](Status status) {
      RAY_CHECK_OK(status);
      --register_pending_count;
    });
  }
  // Wait until register finishes.
  WaitPendingDone(register_pending_count, timeout);

  // Wait for all subscribe notifications.
  WaitPendingDone(sub_pending_count, timeout);
}

}  // namespace gcs

}  // namespace ray
