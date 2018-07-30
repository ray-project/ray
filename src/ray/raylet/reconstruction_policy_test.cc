#include <list>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <boost/asio.hpp>

#include "ray/raylet/format/node_manager_generated.h"
#include "ray/raylet/reconstruction_policy.h"

#include "ray/object_manager/object_directory.h"

namespace ray {

namespace raylet {

class MockObjectDirectory : public ObjectDirectoryInterface {
 public:
  MockObjectDirectory() {}

  ray::Status LookupLocations(const ObjectID &object_id,
                              const OnLocationsFound &callback) {
    callbacks_.push_back({object_id, callback});
    return ray::Status::OK();
  }

  void FlushCallbacks() {
    for (const auto &callback : callbacks_) {
      const ObjectID object_id = callback.first;
      callback.second(locations_[object_id], object_id);
    }
    callbacks_.clear();
  }

  void SetObjectLocations(const ObjectID &object_id, std::vector<ClientID> locations) {
    locations_[object_id] = locations;
  }

  MOCK_METHOD0(RegisterBackend, void(void));
  MOCK_METHOD3(GetInformation, ray::Status(const ClientID &, const InfoSuccessCallback &,
                                           const InfoFailureCallback &));
  MOCK_METHOD3(SubscribeObjectLocations,
               ray::Status(const ray::UniqueID &, const ObjectID &,
                           const OnLocationsFound &));
  MOCK_METHOD2(UnsubscribeObjectLocations,
               ray::Status(const ray::UniqueID &, const ObjectID &));
  MOCK_METHOD3(ReportObjectAdded,
               ray::Status(const ObjectID &, const ClientID &, const ObjectInfoT &));
  MOCK_METHOD2(ReportObjectRemoved, ray::Status(const ObjectID &, const ClientID &));

 private:
  std::vector<std::pair<ObjectID, OnLocationsFound>> callbacks_;
  std::unordered_map<ObjectID, std::vector<ClientID>> locations_;
};

class MockGcs : public gcs::PubsubInterface<TaskID> {
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
        mock_object_directory_(std::make_shared<MockObjectDirectory>()),
        reconstruction_timeout_ms_(50),
        reconstruction_policy_(std::make_shared<ReconstructionPolicy>(
            io_service_,
            [this](const TaskID &task_id) { TriggerReconstruction(task_id); },
            reconstruction_timeout_ms_, ClientID::from_random(), mock_gcs_,
            mock_object_directory_)),
        timer_canceled_(false) {
    mock_gcs_.Subscribe(
        [this](gcs::AsyncGcsClient *client, const TaskID &task_id,
               const TaskLeaseDataT &task_lease) {
          reconstruction_policy_->HandleTaskLeaseNotification(task_id,
                                                              task_lease.timeout);
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

    mock_object_directory_->FlushCallbacks();
  }

 protected:
  boost::asio::io_service io_service_;
  MockGcs mock_gcs_;
  std::shared_ptr<MockObjectDirectory> mock_object_directory_;
  uint64_t reconstruction_timeout_ms_;
  std::shared_ptr<ReconstructionPolicy> reconstruction_policy_;
  bool timer_canceled_;
  std::unordered_map<TaskID, int> reconstructed_tasks_;
};

TEST_F(ReconstructionPolicyTest, TestReconstructionSimple) {
  TaskID task_id = TaskID::from_random();
  task_id = FinishTaskId(task_id);
  ObjectID object_id = ComputeReturnId(task_id, 1);

  // Listen for an object.
  reconstruction_policy_->ListenAndMaybeReconstruct(object_id);
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

TEST_F(ReconstructionPolicyTest, TestReconstructionEvicted) {
  TaskID task_id = TaskID::from_random();
  task_id = FinishTaskId(task_id);
  ObjectID object_id = ComputeReturnId(task_id, 1);
  mock_object_directory_->SetObjectLocations(object_id, {ClientID::from_random()});

  // Listen for both objects.
  reconstruction_policy_->ListenAndMaybeReconstruct(object_id);
  // Run the test for longer than the reconstruction timeout.
  Run(reconstruction_timeout_ms_ * 1.1);
  // Check that reconstruction was not triggered, since the objects still
  // exist on a live node.
  ASSERT_EQ(reconstructed_tasks_[task_id], 0);

  // Simulate evicting one of the objects.
  mock_object_directory_->SetObjectLocations(object_id, {});
  // Run the test again.
  Run(reconstruction_timeout_ms_ * 1.1);
  // Check that reconstruction was triggered, since one of the objects was
  // evicted.
  ASSERT_EQ(reconstructed_tasks_[task_id], 1);
}

TEST_F(ReconstructionPolicyTest, TestDuplicateReconstruction) {
  // Create two object IDs produced by the same task.
  TaskID task_id = TaskID::from_random();
  task_id = FinishTaskId(task_id);
  ObjectID object_id1 = ComputeReturnId(task_id, 1);
  ObjectID object_id2 = ComputeReturnId(task_id, 2);

  // Listen for both objects.
  reconstruction_policy_->ListenAndMaybeReconstruct(object_id1);
  reconstruction_policy_->ListenAndMaybeReconstruct(object_id2);
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
  // Run the test for much longer than the reconstruction timeout.
  int64_t test_period = 2 * reconstruction_timeout_ms_;

  // Acquire the task lease for a period longer than the test period.
  auto task_lease_data = std::make_shared<TaskLeaseDataT>();
  task_lease_data->node_manager_id = ClientID::from_random().hex();
  task_lease_data->acquired_at = current_sys_time_ms();
  task_lease_data->timeout = 2 * test_period;
  mock_gcs_.Add(DriverID::nil(), task_id, task_lease_data);

  // Listen for an object.
  reconstruction_policy_->ListenAndMaybeReconstruct(object_id);
  // Run the test.
  Run(test_period);
  // Check that reconstruction is suppressed by the active task lease.
  ASSERT_TRUE(reconstructed_tasks_.empty());

  // Run the test again past the expiration time of the lease.
  Run(task_lease_data->timeout * 1.1);
  // Check that this time, reconstruction is triggered.
  ASSERT_EQ(reconstructed_tasks_[task_id], 1);
}

TEST_F(ReconstructionPolicyTest, TestReconstructionContinuallySuppressed) {
  TaskID task_id = TaskID::from_random();
  task_id = FinishTaskId(task_id);
  ObjectID object_id = ComputeReturnId(task_id, 1);

  // Listen for an object.
  reconstruction_policy_->ListenAndMaybeReconstruct(object_id);
  // Send the reconstruction manager heartbeats about the object.
  SetPeriodicTimer(reconstruction_timeout_ms_ / 2, [this, task_id]() {
    auto task_lease_data = std::make_shared<TaskLeaseDataT>();
    task_lease_data->node_manager_id = ClientID::from_random().hex();
    task_lease_data->acquired_at = current_sys_time_ms();
    task_lease_data->timeout = reconstruction_timeout_ms_;
    mock_gcs_.Add(DriverID::nil(), task_id, task_lease_data);
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
  reconstruction_policy_->ListenAndMaybeReconstruct(object_id);
  // Halfway through the reconstruction timeout, cancel the object
  // reconstruction.
  auto timer_period = boost::posix_time::milliseconds(reconstruction_timeout_ms_);
  auto timer = std::make_shared<boost::asio::deadline_timer>(io_service_, timer_period);
  timer->async_wait([this, timer, object_id](const boost::system::error_code &error) {
    ASSERT_FALSE(error);
    reconstruction_policy_->Cancel(object_id);
  });
  Run(reconstruction_timeout_ms_ * 2);
  // Check that reconstruction is suppressed.
  ASSERT_TRUE(reconstructed_tasks_.empty());

  // Listen for the object again.
  reconstruction_policy_->ListenAndMaybeReconstruct(object_id);
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
