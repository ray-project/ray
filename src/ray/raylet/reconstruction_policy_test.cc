#include <list>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <boost/asio.hpp>

#include "ray/raylet/format/node_manager_generated.h"
#include "ray/raylet/reconstruction_policy.h"

namespace ray {

namespace raylet {

class MockGcs : virtual public gcs::PubsubInterface<TaskID> {
 public:
  MockGcs() : notification_callback_(nullptr), failure_callback_(nullptr){};

  void Subscribe(const gcs::TaskLeaseTable::WriteCallback &notification_callback,
                 const gcs::TaskLeaseTable::FailureCallback &failure_callback) {
    notification_callback_ = notification_callback;
    failure_callback_ = failure_callback;
  }

  void Add(const JobID &job_id, const TaskID &task_id,
           std::shared_ptr<TaskLeaseDataT> &task_lease_data) {
    task_lease_table_[task_id] = task_lease_data;
    if (subscribed_tasks_.count(task_id) == 1) {
      notification_callback_(nullptr, task_id, *task_lease_data);
    }
  }

  Status RequestNotifications(const JobID &job_id, const TaskID &task_id,
                              const ClientID &client_id) {
    subscribed_tasks_.insert(task_id);
    auto entry = task_lease_table_.find(task_id);
    if (entry == task_lease_table_.end()) {
      failure_callback_(nullptr, task_id);
    } else {
      notification_callback_(nullptr, task_id, *entry->second);
    }
    return ray::Status::OK();
  }

  Status CancelNotifications(const JobID &job_id, const TaskID &task_id,
                             const ClientID &client_id) {
    subscribed_tasks_.erase(task_id);
    return ray::Status::OK();
  }

 private:
  gcs::TaskLeaseTable::WriteCallback notification_callback_;
  gcs::TaskLeaseTable::FailureCallback failure_callback_;
  std::unordered_map<TaskID, std::shared_ptr<TaskLeaseDataT>> task_lease_table_;
  std::unordered_set<TaskID> subscribed_tasks_;
};

class ReconstructionPolicyTest : public ::testing::Test {
 public:
  ReconstructionPolicyTest()
      : io_service_(),
        mock_gcs_(),
        reconstruction_timeout_ms_(100),
        reconstruction_policy_(std::make_shared<ReconstructionPolicy>(
            io_service_,
            [this](const TaskID &task_id) { TriggerReconstruction(task_id); },
            reconstruction_timeout_ms_, ClientID::from_random(), mock_gcs_)),
        timer_canceled_(false) {
    mock_gcs_.Subscribe(
        [this](gcs::AsyncGcsClient *client, const TaskID &task_id,
               const TaskLeaseDataT &task_lease) {
          reconstruction_policy_->HandleTaskLeaseNotification(task_id,
                                                              task_lease.expires_at);
        },
        [this](gcs::AsyncGcsClient *client, const TaskID &task_id) {
          reconstruction_policy_->HandleTaskLeaseNotification(task_id, 0);
        });
  }

  void TriggerReconstruction(const TaskID &task_id) { reconstructed_tasks_[task_id]++; }

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
    timer->async_wait([this, timer](const boost::system::error_code &error) {
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
  std::unordered_map<TaskID, int> reconstructed_tasks_;
};

TEST_F(ReconstructionPolicyTest, TestReconstruction) {
  TaskID task_id = TaskID::from_random();
  task_id = FinishTaskId(task_id);
  ObjectID object_id = ComputeReturnId(task_id, 1);

  // Listen for an object.
  reconstruction_policy_->Listen(object_id);
  // Run the test for longer than the reconstruction timeout.
  Run(reconstruction_timeout_ms_ * 1.1);
  // Check that reconstruction was triggered for the task that created the
  // object.
  ASSERT_EQ(reconstructed_tasks_[task_id], 1);

  // Run the test again.
  Run(reconstruction_timeout_ms_ * 1.1);
  // Check that reconstruction was triggered again.
  ASSERT_EQ(reconstructed_tasks_[task_id], 2);
}

TEST_F(ReconstructionPolicyTest, TestDuplicateReconstruction) {
  // Create two object IDs produced by the same task.
  TaskID task_id = TaskID::from_random();
  task_id = FinishTaskId(task_id);
  ObjectID object_id1 = ComputeReturnId(task_id, 1);
  ObjectID object_id2 = ComputeReturnId(task_id, 2);

  // Listen for both objects.
  reconstruction_policy_->Listen(object_id1);
  reconstruction_policy_->Listen(object_id2);
  // Run the test for longer than the reconstruction timeout.
  Run(reconstruction_timeout_ms_ * 1.1);
  // Check that reconstruction is only triggered once for the task that created
  // both objects.
  ASSERT_EQ(reconstructed_tasks_[task_id], 1);

  // Run the test again.
  Run(reconstruction_timeout_ms_ * 1.1);
  // Check that reconstruction is again only triggered once.
  ASSERT_EQ(reconstructed_tasks_[task_id], 2);
}

TEST_F(ReconstructionPolicyTest, TestReconstructionSuppressed) {
  TaskID task_id = TaskID::from_random();
  task_id = FinishTaskId(task_id);
  ObjectID object_id = ComputeReturnId(task_id, 1);

  // Listen for an object.
  reconstruction_policy_->Listen(object_id);
  // Send the reconstruction manager heartbeats about the object.
  SetPeriodicTimer(reconstruction_timeout_ms_ / 2, [this, object_id]() {
    reconstruction_policy_->HandleTaskLeaseNotification(
        object_id, current_time_ms() + reconstruction_timeout_ms_);
  });
  // Run the test for much longer than the reconstruction timeout.
  Run(reconstruction_timeout_ms_ * 2);
  // Check that reconstruction is suppressed.
  ASSERT_TRUE(reconstructed_tasks_.empty());

  // Cancel the heartbeats to the reconstruction manager.
  CancelPeriodicTimer();
  // Run the test again.
  Run(reconstruction_timeout_ms_ * 1.1);
  // Check that this time, reconstruction is triggered.
  ASSERT_EQ(reconstructed_tasks_[task_id], 1);
}

TEST_F(ReconstructionPolicyTest, TestReconstructionCanceled) {
  TaskID task_id = TaskID::from_random();
  task_id = FinishTaskId(task_id);
  ObjectID object_id = ComputeReturnId(task_id, 1);

  // Listen for an object.
  reconstruction_policy_->Listen(object_id);
  // Halfway through the reconstruction timeout, cancel the object
  // reconstruction.
  reconstruction_policy_->Cancel(object_id);
  Run(reconstruction_timeout_ms_ * 2);
  // Check that reconstruction is suppressed.
  ASSERT_TRUE(reconstructed_tasks_.empty());

  // Listen for the object again.
  reconstruction_policy_->Listen(object_id);
  // Run the test again.
  Run(reconstruction_timeout_ms_ * 1.1);
  // Check that this time, reconstruction is triggered.
  ASSERT_EQ(reconstructed_tasks_[task_id], 1);
}

}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
