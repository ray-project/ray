#include <list>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <boost/asio.hpp>

#include "ray/raylet/format/node_manager_generated.h"
#include "ray/raylet/reconstruction_policy.h"

namespace ray {

namespace raylet {

class MockGcs : virtual public gcs::LogInterface<TaskID, TaskReconstructionData>,
                virtual public gcs::PubsubInterface<ObjectID> {
 public:
  MockGcs(){};
  Status AppendAt(
      const JobID &job_id, const TaskID &task_id,
      std::shared_ptr<TaskReconstructionDataT> task_reconstruction_data,
      const gcs::LogInterface<TaskID, TaskReconstructionData>::WriteCallback &done,
      const gcs::LogInterface<TaskID, TaskReconstructionData>::WriteCallback &failure,
      int index) {
    if (task_log_[task_id].size() == static_cast<size_t>(index)) {
      task_log_[task_id].push_back(task_reconstruction_data);
      done(NULL, task_id, task_reconstruction_data);
    } else {
      failure(NULL, task_id, task_reconstruction_data);
    }
    return ray::Status::OK();
  };

  Status RequestNotifications(const JobID &job_id, const ObjectID &object_id,
                              const ClientID &client_id) {
    return ray::Status::OK();
  }

  Status CancelNotifications(const JobID &job_id, const ObjectID &object_id,
                             const ClientID &client_id) {
    return ray::Status::OK();
  }

 private:
  std::unordered_map<TaskID, std::vector<std::shared_ptr<TaskReconstructionDataT>>,
                     UniqueIDHasher>
      task_log_;
};

class ReconstructionPolicyTest : public ::testing::Test {
 public:
  ReconstructionPolicyTest()
      : io_service_(),
        mock_gcs_(),
        reconstruction_timeout_ms_(100),
        reconstruction_policy_(std::make_shared<ReconstructionPolicy>(
            io_service_, ClientID::from_random(), mock_gcs_, mock_gcs_,
            [this](const TaskID &task_id) { TriggerReconstruction(task_id); },
            reconstruction_timeout_ms_)),
        timer_canceled_(false) {}

  uint64_t GetReconstructionTimeoutMs() const { return reconstruction_timeout_ms_; }
  std::shared_ptr<ReconstructionPolicy> GetReconstructionPolicy() {
    return reconstruction_policy_;
  }

  const std::unordered_map<TaskID, int, UniqueIDHasher> &ReconstructionsTriggered()
      const {
    return reconstructions_triggered_;
  }

  void TriggerReconstruction(const TaskID &task_id) {
    reconstructions_triggered_[task_id]++;
  }

  void Tick(const std::function<void(void)> &handler,
            std::shared_ptr<boost::asio::deadline_timer> timer,
            boost::posix_time::milliseconds timer_period,
            const boost::system::error_code &error) {
    if (timer_canceled_) {
      return;
    }
    ASSERT_FALSE(error);
    handler();
    // Fire the timer again after another period.
    timer->expires_from_now(timer_period);
    timer->async_wait(
        [this, handler, timer, timer_period](const boost::system::error_code &error) {
          Tick(handler, timer, timer_period, error);
        });
  }

  void SetPeriodicTimer(uint64_t period_ms, const std::function<void(void)> &handler) {
    timer_canceled_ = false;
    auto timer_period = boost::posix_time::milliseconds(period_ms);
    auto timer = std::make_shared<boost::asio::deadline_timer>(io_service_, timer_period);
    timer->async_wait(
        [this, handler, timer, timer_period](const boost::system::error_code &error) {
          Tick(handler, timer, timer_period, error);
        });
  }

  void CancelPeriodicTimer() { timer_canceled_ = true; }

  void Run(uint64_t reconstruction_timeout_ms) {
    auto timer_period = boost::posix_time::milliseconds(reconstruction_timeout_ms);
    auto timer = std::make_shared<boost::asio::deadline_timer>(io_service_, timer_period);
    timer->async_wait([this](const boost::system::error_code &error) {
      ASSERT_FALSE(error);
      io_service_.stop();
    });
    io_service_.run();
    io_service_.reset();
  }

 protected:
  boost::asio::io_service io_service_;
  MockGcs mock_gcs_;
  uint64_t reconstruction_timeout_ms_;
  std::shared_ptr<ReconstructionPolicy> reconstruction_policy_;
  bool timer_canceled_;
  std::unordered_map<TaskID, int, UniqueIDHasher> reconstructions_triggered_;
};

TEST_F(ReconstructionPolicyTest, TestReconstruction) {
  auto reconstruction_policy = GetReconstructionPolicy();
  auto timeout_ms = GetReconstructionTimeoutMs();
  TaskID task_id = TaskID::from_random();
  task_id = FinishTaskId(task_id);
  ObjectID object_id = ComputeReturnId(task_id, 1);

  // Listen for an object.
  reconstruction_policy->Listen(object_id);
  reconstruction_policy->Tick();
  // Run the test for longer than the reconstruction timeout.
  Run(timeout_ms * 1.1);
  // Check that reconstruction was triggered for the task that created the
  // object.
  auto reconstructions = ReconstructionsTriggered();
  ASSERT_EQ(reconstructions.size(), 1);
  ASSERT_EQ(reconstructions[task_id], 1);

  // Run the test again.
  reconstruction_policy->Tick();
  Run(timeout_ms * 1.1);
  // Check that reconstruction was triggered again.
  reconstructions = ReconstructionsTriggered();
  ASSERT_EQ(reconstructions.size(), 1);
  ASSERT_EQ(reconstructions[task_id], 2);
}

TEST_F(ReconstructionPolicyTest, TestDuplicateReconstruction) {
  auto reconstruction_policy = GetReconstructionPolicy();
  auto timeout_ms = GetReconstructionTimeoutMs();
  // Create two object IDs produced by the same task.
  TaskID task_id = TaskID::from_random();
  task_id = FinishTaskId(task_id);
  ObjectID object_id1 = ComputeReturnId(task_id, 1);
  ObjectID object_id2 = ComputeReturnId(task_id, 2);

  // Listen for both objects.
  reconstruction_policy->Listen(object_id1);
  reconstruction_policy->Listen(object_id2);
  reconstruction_policy->Tick();
  // Run the test for longer than the reconstruction timeout.
  Run(timeout_ms * 1.1);
  // Check that reconstruction is only triggered once for the task that created
  // both objects.
  auto reconstructions = ReconstructionsTriggered();
  ASSERT_EQ(reconstructions.size(), 1);
  ASSERT_EQ(reconstructions[task_id], 1);

  // Run the test again.
  reconstruction_policy->Tick();
  Run(timeout_ms * 1.1);
  // Check that reconstruction is again only triggered once.
  reconstructions = ReconstructionsTriggered();
  ASSERT_EQ(reconstructions.size(), 1);
  ASSERT_EQ(reconstructions[task_id], 2);
}

TEST_F(ReconstructionPolicyTest, TestReconstructionEviction) {
  auto reconstruction_policy = GetReconstructionPolicy();
  auto timeout_ms = GetReconstructionTimeoutMs();
  TaskID task_id = TaskID::from_random();
  task_id = FinishTaskId(task_id);
  ObjectID object_id = ComputeReturnId(task_id, 1);

  // Create one addition entry and one deletion entry for a different object
  // manager.
  ObjectTableDataT addition_entry;
  addition_entry.manager = ClientID::from_random().binary();
  addition_entry.is_eviction = false;
  ObjectTableDataT deletion_entry;
  deletion_entry.manager = ClientID::from_random().binary();
  deletion_entry.is_eviction = true;

  // Listen for an object.
  reconstruction_policy->Listen(object_id);
  reconstruction_policy->Tick();
  // Send the reconstruction manager information about the current locations of
  // the object. The reconstruction manager should now record the object as
  // being available at the node listed in the addition entry.
  reconstruction_policy->HandleNotification(object_id, {addition_entry, deletion_entry});
  // Run the test for longer than the reconstruction timeout.
  Run(timeout_ms * 1.1);
  // Check that reconstruction is suppressed because the object was at a known
  // location.
  auto reconstructions = ReconstructionsTriggered();
  ASSERT_EQ(reconstructions.size(), 0);

  // Send the reconstruction manager information about the object being evicted
  // from the known object manager location. The reconstruction manager should
  // now record the object as being evicted from all locations.
  deletion_entry.manager = addition_entry.manager;
  reconstruction_policy->HandleNotification(object_id, {deletion_entry});
  // Check that reconstruction is triggered immediately.
  reconstructions = ReconstructionsTriggered();
  ASSERT_EQ(reconstructions.size(), 1);
  ASSERT_EQ(reconstructions[task_id], 1);
}

TEST_F(ReconstructionPolicyTest, TestReconstructionSuppressed) {
  auto reconstruction_policy = GetReconstructionPolicy();
  auto timeout_ms = GetReconstructionTimeoutMs();
  TaskID task_id = TaskID::from_random();
  task_id = FinishTaskId(task_id);
  ObjectID object_id = ComputeReturnId(task_id, 1);

  // Listen for an object.
  reconstruction_policy->Listen(object_id);
  reconstruction_policy->Tick();
  // Send the reconstruction manager heartbeats about the object.
  SetPeriodicTimer(timeout_ms / 2, [reconstruction_policy, object_id]() {
    reconstruction_policy->HandleNotification(object_id, {});
  });
  // Run the test for much longer than the reconstruction timeout.
  Run(timeout_ms * 2);
  // Check that reconstruction is suppressed.
  auto reconstructions = ReconstructionsTriggered();
  ASSERT_EQ(reconstructions.size(), 0);

  // Cancel the heartbeats to the reconstruction manager.
  CancelPeriodicTimer();
  // Run the test again.
  reconstruction_policy->Tick();
  Run(timeout_ms * 1.1);
  // Check that this time, reconstruction is triggered.
  reconstructions = ReconstructionsTriggered();
  ASSERT_EQ(reconstructions.size(), 1);
  ASSERT_EQ(reconstructions[task_id], 1);
}

TEST_F(ReconstructionPolicyTest, TestReconstructionCanceled) {
  auto reconstruction_policy = GetReconstructionPolicy();
  auto timeout_ms = GetReconstructionTimeoutMs();
  TaskID task_id = TaskID::from_random();
  task_id = FinishTaskId(task_id);
  ObjectID object_id = ComputeReturnId(task_id, 1);

  // Listen for an object.
  reconstruction_policy->Listen(object_id);
  reconstruction_policy->Tick();
  // Halfway through the reconstruction timeout, cancel the object
  // reconstruction.
  reconstruction_policy->Cancel(object_id);
  Run(timeout_ms * 2);
  // Check that reconstruction is suppressed.
  auto reconstructions = ReconstructionsTriggered();
  ASSERT_EQ(reconstructions.size(), 0);

  // Listen for the object again.
  reconstruction_policy->Listen(object_id);
  // Run the test again.
  reconstruction_policy->Tick();
  Run(timeout_ms * 1.1);
  // Check that this time, reconstruction is triggered.
  reconstructions = ReconstructionsTriggered();
  ASSERT_EQ(reconstructions.size(), 1);
  ASSERT_EQ(reconstructions[task_id], 1);
}

}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
