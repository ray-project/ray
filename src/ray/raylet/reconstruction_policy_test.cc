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

#include "ray/raylet/reconstruction_policy.h"

#include <boost/asio.hpp>
#include <list>

#include "absl/time/clock.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/gcs_client/service_based_accessor.h"
#include "ray/gcs/gcs_client/service_based_gcs_client.h"
#include "ray/object_manager/object_directory.h"
#include "ray/raylet/format/node_manager_generated.h"
#include "ray/raylet/reconstruction_policy.h"

namespace ray {

namespace raylet {

using rpc::TaskLeaseData;

// A helper function to get a normal task id.
inline TaskID ForNormalTask() {
  const static JobID job_id = JobID::FromInt(1);
  const static TaskID driver_task_id = TaskID::ForDriverTask(job_id);
  static TaskID task_id =
      TaskID::ForNormalTask(job_id, driver_task_id, /*parent_task_counter=*/1);
  return task_id;
}

class MockObjectDirectory : public ObjectDirectoryInterface {
 public:
  MockObjectDirectory() {}

  ray::Status LookupLocations(const ObjectID &object_id,
                              const rpc::Address &owner_address,
                              const OnLocationsFound &callback) override {
    callbacks_.push_back({object_id, callback});
    return ray::Status::OK();
  }

  void FlushCallbacks() {
    for (const auto &callback : callbacks_) {
      const ObjectID object_id = callback.first;
      auto it = locations_.find(object_id);
      if (it == locations_.end()) {
        callback.second(object_id, std::unordered_set<ray::NodeID>(), "", 0);
      } else {
        callback.second(object_id, it->second, "", 0);
      }
    }
    callbacks_.clear();
  }

  void SetObjectLocations(const ObjectID &object_id,
                          const std::unordered_set<NodeID> &locations) {
    locations_[object_id] = locations;
  }

  void HandleNodeRemoved(const NodeID &node_id) override {
    for (auto &locations : locations_) {
      locations.second.erase(node_id);
    }
  }

  std::string DebugString() const override { return ""; }

  MOCK_METHOD0(GetLocalNodeID, ray::NodeID());
  MOCK_CONST_METHOD1(LookupRemoteConnectionInfo, void(RemoteConnectionInfo &));
  MOCK_CONST_METHOD0(LookupAllRemoteConnections, std::vector<RemoteConnectionInfo>());
  MOCK_METHOD4(SubscribeObjectLocations,
               ray::Status(const ray::UniqueID &, const ObjectID &,
                           const rpc::Address &owner_address, const OnLocationsFound &));
  MOCK_METHOD2(UnsubscribeObjectLocations,
               ray::Status(const ray::UniqueID &, const ObjectID &));
  MOCK_METHOD3(ReportObjectAdded,
               ray::Status(const ObjectID &, const NodeID &,
                           const object_manager::protocol::ObjectInfoT &));
  MOCK_METHOD3(ReportObjectRemoved,
               ray::Status(const ObjectID &, const NodeID &,
                           const object_manager::protocol::ObjectInfoT &));

 private:
  std::vector<std::pair<ObjectID, OnLocationsFound>> callbacks_;
  std::unordered_map<ObjectID, std::unordered_set<NodeID>> locations_;
};

class MockNodeInfoAccessor : public gcs::ServiceBasedNodeInfoAccessor {
 public:
  MockNodeInfoAccessor(gcs::ServiceBasedGcsClient *client)
      : gcs::ServiceBasedNodeInfoAccessor(client) {}

  bool IsRemoved(const NodeID &node_id) const override { return false; }
};

class MockTaskInfoAccessor : public gcs::ServiceBasedTaskInfoAccessor {
 public:
  MockTaskInfoAccessor(gcs::ServiceBasedGcsClient *client)
      : ServiceBasedTaskInfoAccessor(client) {}

  Status AsyncSubscribeTaskLease(
      const TaskID &task_id,
      const gcs::SubscribeCallback<TaskID, boost::optional<TaskLeaseData>> &subscribe,
      const gcs::StatusCallback &done) override {
    subscribe_callback_ = subscribe;
    subscribed_tasks_.insert(task_id);
    auto entry = task_lease_table_.find(task_id);
    if (entry == task_lease_table_.end()) {
      boost::optional<TaskLeaseData> result;
      subscribe(task_id, result);
    } else {
      boost::optional<TaskLeaseData> result(*entry->second);
      subscribe(task_id, result);
    }
    return ray::Status::OK();
  }

  Status AsyncUnsubscribeTaskLease(const TaskID &task_id) override {
    subscribed_tasks_.erase(task_id);
    return ray::Status::OK();
  }

  Status AsyncAddTaskLease(const std::shared_ptr<TaskLeaseData> &task_lease_data,
                           const gcs::StatusCallback &done) override {
    TaskID task_id = TaskID::FromBinary(task_lease_data->task_id());
    task_lease_table_[task_id] = task_lease_data;
    if (subscribed_tasks_.count(task_id) == 1) {
      boost::optional<TaskLeaseData> result(*task_lease_data);
      subscribe_callback_(task_id, result);
    }
    return Status::OK();
  }

  Status AsyncGetTaskLease(
      const TaskID &task_id,
      const gcs::OptionalItemCallback<rpc::TaskLeaseData> &callback) override {
    auto iter = task_lease_table_.find(task_id);
    if (iter != task_lease_table_.end()) {
      callback(Status::OK(), *iter->second);
    } else {
      callback(Status::OK(), boost::none);
    }
    return Status::OK();
  }

  Status AttemptTaskReconstruction(
      const std::shared_ptr<TaskReconstructionData> &task_data,
      const gcs::StatusCallback &done) override {
    int log_index = task_data->num_reconstructions();
    TaskID task_id = TaskID::FromBinary(task_data->task_id());
    if (task_reconstruction_log_[task_id].size() == static_cast<size_t>(log_index)) {
      task_reconstruction_log_[task_id].push_back(*task_data);
      if (done != nullptr) {
        done(Status::OK());
      }
    } else {
      if (done != nullptr) {
        done(Status::Invalid("Updating task reconstruction failed."));
      }
    }
    return Status::OK();
  }

 private:
  gcs::SubscribeCallback<TaskID, boost::optional<TaskLeaseData>> subscribe_callback_;
  std::unordered_map<TaskID, std::shared_ptr<TaskLeaseData>> task_lease_table_;
  std::unordered_set<TaskID> subscribed_tasks_;
  std::unordered_map<TaskID, std::vector<TaskReconstructionData>>
      task_reconstruction_log_;
};

class MockGcs : public gcs::ServiceBasedGcsClient {
 public:
  MockGcs() : gcs::ServiceBasedGcsClient(gcs::GcsClientOptions("", 0, "")){};

  void Init(gcs::TaskInfoAccessor *task_accessor, gcs::NodeInfoAccessor *node_accessor) {
    task_accessor_.reset(task_accessor);
    node_accessor_.reset(node_accessor);
  }
};

class ReconstructionPolicyTest : public ::testing::Test {
 public:
  ReconstructionPolicyTest()
      : io_service_(),
        mock_gcs_(new MockGcs()),
        task_accessor_(new MockTaskInfoAccessor(mock_gcs_.get())),
        node_accessor_(new MockNodeInfoAccessor(mock_gcs_.get())),
        mock_object_directory_(std::make_shared<MockObjectDirectory>()),
        reconstruction_timeout_ms_(50),
        reconstruction_policy_(std::make_shared<ReconstructionPolicy>(
            io_service_,
            [this](const TaskID &task_id, const ObjectID &obj) {
              TriggerReconstruction(task_id);
            },
            reconstruction_timeout_ms_, NodeID::FromRandom(), mock_gcs_,
            mock_object_directory_)),
        timer_canceled_(false) {
    subscribe_callback_ = [this](const TaskID &task_id,
                                 const boost::optional<TaskLeaseData> &task_lease) {
      if (task_lease) {
        reconstruction_policy_->HandleTaskLeaseNotification(task_id,
                                                            task_lease->timeout());
      } else {
        reconstruction_policy_->HandleTaskLeaseNotification(task_id, 0);
      }
    };

    mock_gcs_->Init(task_accessor_, node_accessor_);
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
  std::shared_ptr<MockGcs> mock_gcs_;
  MockTaskInfoAccessor *task_accessor_;
  MockNodeInfoAccessor *node_accessor_;
  gcs::SubscribeCallback<TaskID, boost::optional<TaskLeaseData>> subscribe_callback_;
  std::shared_ptr<MockObjectDirectory> mock_object_directory_;
  uint64_t reconstruction_timeout_ms_;
  std::shared_ptr<ReconstructionPolicy> reconstruction_policy_;
  bool timer_canceled_;
  std::unordered_map<TaskID, int> reconstructed_tasks_;
};

TEST_F(ReconstructionPolicyTest, TestReconstructionSimple) {
  TaskID task_id = ForNormalTask();
  ObjectID object_id = ObjectID::FromIndex(task_id, /*index=*/1);

  // Listen for an object.
  reconstruction_policy_->ListenAndMaybeReconstruct(object_id, rpc::Address());
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
  TaskID task_id = ForNormalTask();
  ObjectID object_id = ObjectID::FromIndex(task_id, /*index=*/1);
  mock_object_directory_->SetObjectLocations(object_id, {NodeID::FromRandom()});

  // Listen for both objects.
  reconstruction_policy_->ListenAndMaybeReconstruct(object_id, rpc::Address());
  // Run the test for longer than the reconstruction timeout.
  Run(reconstruction_timeout_ms_ * 1.1);
  // Check that reconstruction was not triggered, since the objects still
  // exist on a live node.
  ASSERT_EQ(reconstructed_tasks_[task_id], 0);

  // Simulate evicting one of the objects.
  mock_object_directory_->SetObjectLocations(object_id,
                                             std::unordered_set<ray::NodeID>());
  // Run the test again.
  Run(reconstruction_timeout_ms_ * 1.1);
  // Check that reconstruction was triggered, since one of the objects was
  // evicted.
  ASSERT_EQ(reconstructed_tasks_[task_id], 1);
}

TEST_F(ReconstructionPolicyTest, TestReconstructionObjectLost) {
  TaskID task_id = ForNormalTask();
  ObjectID object_id = ObjectID::FromIndex(task_id, /*index=*/1);
  NodeID node_id = NodeID::FromRandom();
  mock_object_directory_->SetObjectLocations(object_id, {node_id});

  // Listen for both objects.
  reconstruction_policy_->ListenAndMaybeReconstruct(object_id, rpc::Address());
  // Run the test for longer than the reconstruction timeout.
  Run(reconstruction_timeout_ms_ * 1.1);
  // Check that reconstruction was not triggered, since the objects still
  // exist on a live node.
  ASSERT_EQ(reconstructed_tasks_[task_id], 0);

  // Simulate evicting one of the objects.
  mock_object_directory_->HandleNodeRemoved(node_id);
  // Run the test again.
  Run(reconstruction_timeout_ms_ * 1.1);
  // Check that reconstruction was triggered, since one of the objects was
  // evicted.
  ASSERT_EQ(reconstructed_tasks_[task_id], 1);
}

TEST_F(ReconstructionPolicyTest, TestDuplicateReconstruction) {
  // Create two object IDs produced by the same task.
  TaskID task_id = ForNormalTask();
  ObjectID object_id1 = ObjectID::FromIndex(task_id, /*index=*/1);
  ObjectID object_id2 = ObjectID::FromIndex(task_id, /*index=*/2);

  // Listen for both objects.
  reconstruction_policy_->ListenAndMaybeReconstruct(object_id1, rpc::Address());
  reconstruction_policy_->ListenAndMaybeReconstruct(object_id2, rpc::Address());
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
  TaskID task_id = ForNormalTask();
  ObjectID object_id = ObjectID::FromIndex(task_id, /*index=*/1);
  // Run the test for much longer than the reconstruction timeout.
  int64_t test_period = 2 * reconstruction_timeout_ms_;

  // Acquire the task lease for a period longer than the test period.
  auto task_lease_data = std::make_shared<TaskLeaseData>();
  task_lease_data->set_node_manager_id(NodeID::FromRandom().Binary());
  task_lease_data->set_acquired_at(absl::GetCurrentTimeNanos() / 1000000);
  task_lease_data->set_timeout(2 * test_period);
  task_lease_data->set_task_id(task_id.Binary());
  RAY_CHECK_OK(mock_gcs_->Tasks().AsyncAddTaskLease(task_lease_data, nullptr));

  // Listen for an object.
  reconstruction_policy_->ListenAndMaybeReconstruct(object_id, rpc::Address());
  // Run the test.
  Run(test_period);
  // Check that reconstruction is suppressed by the active task lease.
  ASSERT_TRUE(reconstructed_tasks_.empty());

  // Run the test again past the expiration time of the lease.
  Run(task_lease_data->timeout() * 1.1);
  // Check that this time, reconstruction is triggered.
  ASSERT_EQ(reconstructed_tasks_[task_id], 1);
}

TEST_F(ReconstructionPolicyTest, TestReconstructionContinuallySuppressed) {
  TaskID task_id = ForNormalTask();
  ObjectID object_id = ObjectID::FromIndex(task_id, /*index=*/1);

  // Listen for an object.
  reconstruction_policy_->ListenAndMaybeReconstruct(object_id, rpc::Address());
  // Send the reconstruction manager heartbeats about the object.
  SetPeriodicTimer(reconstruction_timeout_ms_ / 2, [this, task_id]() {
    auto task_lease_data = std::make_shared<TaskLeaseData>();
    task_lease_data->set_node_manager_id(NodeID::FromRandom().Binary());
    task_lease_data->set_acquired_at(absl::GetCurrentTimeNanos() / 1000000);
    task_lease_data->set_timeout(reconstruction_timeout_ms_);
    task_lease_data->set_task_id(task_id.Binary());
    RAY_CHECK_OK(mock_gcs_->Tasks().AsyncAddTaskLease(task_lease_data, nullptr));
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
  TaskID task_id = ForNormalTask();
  ObjectID object_id = ObjectID::FromIndex(task_id, /*index=*/1);

  // Listen for an object.
  reconstruction_policy_->ListenAndMaybeReconstruct(object_id, rpc::Address());
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
  reconstruction_policy_->ListenAndMaybeReconstruct(object_id, rpc::Address());
  // Run the test again.
  Run(reconstruction_timeout_ms_ * 1.1);
  // Check that this time, reconstruction is triggered.
  ASSERT_EQ(reconstructed_tasks_[task_id], 1);
}

TEST_F(ReconstructionPolicyTest, TestSimultaneousReconstructionSuppressed) {
  TaskID task_id = ForNormalTask();
  ObjectID object_id = ObjectID::FromIndex(task_id, /*index=*/1);

  // Log a reconstruction attempt to simulate a different node attempting the
  // reconstruction first. This should suppress this node's first attempt at
  // reconstruction.
  auto task_reconstruction_data = std::make_shared<TaskReconstructionData>();
  task_reconstruction_data->set_task_id(task_id.Binary());
  task_reconstruction_data->set_node_manager_id(NodeID::FromRandom().Binary());
  task_reconstruction_data->set_num_reconstructions(0);
  RAY_CHECK_OK(mock_gcs_->Tasks().AttemptTaskReconstruction(
      task_reconstruction_data,
      /*done=*/
      [](Status status) { ASSERT_TRUE(status.ok()); }));

  // Listen for an object.
  reconstruction_policy_->ListenAndMaybeReconstruct(object_id, rpc::Address());
  // Run the test for longer than the reconstruction timeout.
  Run(reconstruction_timeout_ms_ * 1.1);
  // Check that reconstruction is suppressed by the reconstruction attempt
  // logged by the other node.
  ASSERT_TRUE(reconstructed_tasks_.empty());

  // Run the test for longer than the reconstruction timeout again.
  Run(reconstruction_timeout_ms_ * 1.1);
  // Check that this time, reconstruction is triggered, since we did not
  // receive a task lease notification from the other node yet and our next
  // attempt to reconstruct adds an entry at the next index in the
  // TaskReconstructionLog.
  ASSERT_EQ(reconstructed_tasks_[task_id], 1);
}

}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
