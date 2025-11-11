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

#include "ray/core_worker/task_submission/normal_task_submitter.h"

#include <list>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "mock/ray/core_worker/memory_store.h"
#include "mock/ray/core_worker/task_manager_interface.h"
#include "ray/common/task/task_spec.h"
#include "ray/common/task/task_util.h"
#include "ray/common/test_utils.h"
#include "ray/core_worker/fake_actor_creator.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker_rpc_client/core_worker_client_pool.h"
#include "ray/core_worker_rpc_client/fake_core_worker_client.h"
#include "ray/observability/fake_metric.h"
#include "ray/raylet_rpc_client/fake_raylet_client.h"
#include "ray/raylet_rpc_client/raylet_client_interface.h"

namespace ray {
namespace core {
namespace {

class DynamicRateLimiter : public LeaseRequestRateLimiter {
 public:
  explicit DynamicRateLimiter(size_t limit) : limit_(limit) {}
  size_t GetMaxPendingLeaseRequestsPerSchedulingCategory() override { return limit_; }

  size_t limit_;
};

// Wait (and halt the thread) until object_id appears in memory_store.
void WaitForObjectIdInMemoryStore(CoreWorkerMemoryStore &memory_store,
                                  const ObjectID &object_id) {
  std::promise<bool> p;
  memory_store.GetAsync(object_id, [&p](auto) { p.set_value(true); });
  ASSERT_TRUE(p.get_future().get());
}
}  // namespace

// Used to prevent leases from timing out when not testing that logic. It would
// be better to use a mock clock or lease manager interface, but that's high
// overhead for the very simple timeout logic we currently have.
int64_t kLongTimeout = 1024 * 1024 * 1024;

TaskSpecification BuildTaskSpec(const std::unordered_map<std::string, double> &resources,
                                const FunctionDescriptor &function_descriptor,
                                int64_t depth = 0,
                                std::string serialized_runtime_env = "") {
  TaskSpecBuilder builder;
  rpc::Address empty_address;
  rpc::JobConfig config;
  builder.SetCommonTaskSpec(TaskID::FromRandom(JobID::Nil()),
                            "dummy_task",
                            Language::PYTHON,
                            function_descriptor,
                            JobID::Nil(),
                            config,
                            TaskID::Nil(),
                            0,
                            TaskID::Nil(),
                            empty_address,
                            1,
                            false,
                            false,
                            -1,
                            resources,
                            resources,
                            serialized_runtime_env,
                            depth,
                            TaskID::Nil(),
                            "");
  return std::move(builder).ConsumeAndBuild();
}
// Calls BuildTaskSpec with empty resources map and empty function descriptor
TaskSpecification BuildEmptyTaskSpec();

class MockWorkerClient : public rpc::FakeCoreWorkerClient {
 public:
  void PushNormalTask(std::unique_ptr<rpc::PushTaskRequest> request,
                      const rpc::ClientCallback<rpc::PushTaskReply> &callback) override {
    callbacks.push_back(callback);
  }

  bool ReplyPushTask(Status status = Status::OK(),
                     bool exit = false,
                     bool is_retryable_error = false,
                     bool was_cancelled_before_running = false) {
    if (callbacks.size() == 0) {
      return false;
    }
    const auto &callback = callbacks.front();
    auto reply = rpc::PushTaskReply();
    if (exit) {
      reply.set_worker_exiting(true);
    }
    if (is_retryable_error) {
      reply.set_is_retryable_error(true);
    }
    if (was_cancelled_before_running) {
      reply.set_was_cancelled_before_running(true);
    }
    callback(status, std::move(reply));
    callbacks.pop_front();
    return true;
  }

  void CancelTask(const rpc::CancelTaskRequest &request,
                  const rpc::ClientCallback<rpc::CancelTaskReply> &callback) override {
    kill_requests.push_front(request);
    cancel_callbacks.push_back(callback);
  }

  void ReplyCancelTask(Status status = Status::OK(),
                       bool attempt_succeeded = true,
                       bool requested_task_running = false) {
    auto &callback = cancel_callbacks.front();
    rpc::CancelTaskReply reply;
    reply.set_attempt_succeeded(attempt_succeeded);
    reply.set_requested_task_running(requested_task_running);
    callback(status, std::move(reply));
    cancel_callbacks.pop_front();
  }

  std::list<rpc::ClientCallback<rpc::PushTaskReply>> callbacks;
  std::list<rpc::CancelTaskRequest> kill_requests;
  std::list<rpc::ClientCallback<rpc::CancelTaskReply>> cancel_callbacks;
};

class MockTaskManager : public MockTaskManagerInterface {
  // TODO(ray-core): Consider adding an integration test between TaskManager and
  // NormalTaskSubmitter, due to the complexity of the interaction between the two.
  // https://github.com/ray-project/ray/issues/54922
 public:
  MockTaskManager() {}

  void CompletePendingTask(const TaskID &task_id,
                           const rpc::PushTaskReply &,
                           const rpc::Address &actor_addr,
                           bool is_application_error) override {
    num_tasks_complete++;
  }

  bool RetryTaskIfPossible(const TaskID &task_id,
                           const rpc::RayErrorInfo &error_info) override {
    num_task_retries_attempted++;
    return false;
  }

  void FailPendingTask(const TaskID &task_id,
                       rpc::ErrorType error_type,
                       const Status *status,
                       const rpc::RayErrorInfo *ray_error_info = nullptr) override {
    num_fail_pending_task_calls++;
    num_tasks_failed++;
  }

  bool FailOrRetryPendingTask(const TaskID &task_id,
                              rpc::ErrorType error_type,
                              const Status *status,
                              const rpc::RayErrorInfo *ray_error_info = nullptr,
                              bool mark_task_object_failed = true,
                              bool fail_immediately = false) override {
    num_tasks_failed++;
    if (!fail_immediately) {
      RetryTaskIfPossible(task_id,
                          ray_error_info ? *ray_error_info : rpc::RayErrorInfo());
    }
    return true;
  }

  void OnTaskDependenciesInlined(const std::vector<ObjectID> &inlined_dependency_ids,
                                 const std::vector<ObjectID> &contained_ids) override {
    num_inlined_dependencies += inlined_dependency_ids.size();
    num_contained_ids += contained_ids.size();
  }

  void MarkTaskCanceled(const TaskID &task_id) override {}

  void MarkTaskNoRetry(const TaskID &task_id) override {}

  std::optional<TaskSpecification> GetTaskSpec(const TaskID &task_id) const override {
    TaskSpecification task = BuildEmptyTaskSpec();
    return task;
  }

  void MarkDependenciesResolved(const TaskID &task_id) override {}

  void MarkTaskWaitingForExecution(const TaskID &task_id,
                                   const NodeID &node_id,
                                   const WorkerID &worker_id) override {}

  bool IsTaskPending(const TaskID &task_id) const override { return true; }

  void MarkGeneratorFailedAndResubmit(const TaskID &task_id) override {
    num_generator_failed_and_resubmitted++;
  }

  int num_tasks_complete = 0;
  int num_tasks_failed = 0;
  int num_inlined_dependencies = 0;
  int num_contained_ids = 0;
  int num_task_retries_attempted = 0;
  int num_fail_pending_task_calls = 0;
  int num_generator_failed_and_resubmitted = 0;
};

class MockRayletClient : public rpc::FakeRayletClient {
 public:
  void ReturnWorkerLease(int worker_port,
                         const LeaseID &lease_id,
                         bool disconnect_worker,
                         const std::string &disconnect_worker_error_detail,
                         bool worker_exiting) override {
    std::lock_guard<std::mutex> lock(mu_);
    if (disconnect_worker) {
      num_workers_disconnected++;
    } else {
      num_workers_returned++;
      if (worker_exiting) {
        num_workers_returned_exiting++;
      }
    }
  }

  void GetWorkerFailureCause(
      const LeaseID &lease_id,
      const ray::rpc::ClientCallback<ray::rpc::GetWorkerFailureCauseReply> &callback)
      override {
    std::lock_guard<std::mutex> lock(mu_);
    get_task_failure_cause_callbacks.push_back(callback);
    num_get_task_failure_causes += 1;
  }

  bool ReplyGetWorkerFailureCause() {
    if (get_task_failure_cause_callbacks.size() == 0) {
      return false;
    }
    auto callback = std::move(get_task_failure_cause_callbacks.front());
    get_task_failure_cause_callbacks.pop_front();
    rpc::GetWorkerFailureCauseReply reply;
    callback(Status::OK(), std::move(reply));
    return true;
  }

  void ReportWorkerBacklog(
      const WorkerID &worker_id,
      const std::vector<rpc::WorkerBacklogReport> &backlog_reports) override {
    std::lock_guard<std::mutex> lock(mu_);
    reported_backlog_size = 0;
    reported_backlogs.clear();
    for (const auto &backlog_report : backlog_reports) {
      reported_backlog_size += backlog_report.backlog_size();
      const LeaseSpecification lease_spec(backlog_report.lease_spec());
      const SchedulingClass scheduling_class = lease_spec.GetSchedulingClass();
      reported_backlogs[scheduling_class] = backlog_report.backlog_size();
    }
  }

  void RequestWorkerLease(
      const rpc::LeaseSpec &lease_spec,
      bool grant_or_reject,
      const ray::rpc::ClientCallback<ray::rpc::RequestWorkerLeaseReply> &callback,
      const int64_t backlog_size,
      const bool is_selected_based_on_locality) override {
    std::lock_guard<std::mutex> lock(mu_);
    num_workers_requested += 1;
    if (grant_or_reject) {
      num_grant_or_reject_leases_requested += 1;
    }
    if (is_selected_based_on_locality) {
      num_is_selected_based_on_locality_leases_requested += 1;
    }
    callbacks.push_back(callback);
  }

  void PrestartWorkers(
      const rpc::PrestartWorkersRequest &request,
      const rpc::ClientCallback<ray::rpc::PrestartWorkersReply> &callback) override {}

  void ReleaseUnusedActorWorkers(
      const std::vector<WorkerID> &workers_in_use,
      const rpc::ClientCallback<rpc::ReleaseUnusedActorWorkersReply> &callback) override {
  }

  void CancelWorkerLease(
      const LeaseID &lease_id,
      const rpc::ClientCallback<rpc::CancelWorkerLeaseReply> &callback) override {
    std::lock_guard<std::mutex> lock(mu_);
    num_leases_canceled += 1;
    cancel_callbacks.push_back(callback);
  }

  // Trigger reply to RequestWorkerLease.
  bool GrantWorkerLease(
      const std::string &address,
      int port,
      const NodeID &granted_node_id,
      const NodeID &retry_at_node_id = NodeID::Nil(),
      bool cancel = false,
      std::string worker_id = WorkerID::FromRandom().Binary(),
      bool reject = false,
      const rpc::RequestWorkerLeaseReply::SchedulingFailureType &failure_type =
          rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_INTENDED) {
    rpc::RequestWorkerLeaseReply reply;
    if (cancel) {
      reply.set_canceled(true);
      reply.set_failure_type(failure_type);
    } else if (reject) {
      reply.set_rejected(true);
    } else if (!retry_at_node_id.IsNil()) {
      reply.mutable_retry_at_raylet_address()->set_ip_address(address);
      reply.mutable_retry_at_raylet_address()->set_port(port);
      reply.mutable_retry_at_raylet_address()->set_node_id(retry_at_node_id.Binary());
    } else {
      reply.mutable_worker_address()->set_ip_address(address);
      reply.mutable_worker_address()->set_port(port);
      reply.mutable_worker_address()->set_node_id(granted_node_id.Binary());
      reply.mutable_worker_address()->set_worker_id(worker_id);
    }
    rpc::ClientCallback<rpc::RequestWorkerLeaseReply> callback = PopCallbackInLock();
    if (!callback) {
      return false;
    }
    callback(Status::OK(), std::move(reply));
    return true;
  }

  bool FailWorkerLeaseDueToGrpcUnavailable() {
    rpc::ClientCallback<rpc::RequestWorkerLeaseReply> callback = PopCallbackInLock();
    if (!callback) {
      return false;
    }
    rpc::RequestWorkerLeaseReply reply;
    callback(Status::RpcError("unavailable", grpc::StatusCode::UNAVAILABLE),
             std::move(reply));
    return true;
  }

  bool ReplyCancelWorkerLease(bool success = true) {
    rpc::ClientCallback<rpc::CancelWorkerLeaseReply> callback = PopCancelCallbackInLock();
    if (!callback) {
      return false;
    }
    rpc::CancelWorkerLeaseReply reply;
    reply.set_success(success);
    callback(Status::OK(), std::move(reply));
    return true;
  }

  template <typename Callback>
  Callback GenericPopCallbackInLock(std::list<Callback> &lst) {
    std::lock_guard<std::mutex> lock(mu_);
    if (lst.size() == 0) {
      return nullptr;
    }
    auto callback = std::move(lst.front());
    lst.pop_front();
    return callback;
  }

  // Pop a callback from the list and return it. If there's no callbacks, returns nullptr.
  rpc::ClientCallback<rpc::RequestWorkerLeaseReply> PopCallbackInLock() {
    return GenericPopCallbackInLock(callbacks);
  }

  rpc::ClientCallback<rpc::CancelWorkerLeaseReply> PopCancelCallbackInLock() {
    return GenericPopCallbackInLock(cancel_callbacks);
  }

  ~MockRayletClient() = default;

  // Protects all internal fields.
  std::mutex mu_;
  int num_grant_or_reject_leases_requested = 0;
  int num_is_selected_based_on_locality_leases_requested = 0;
  int num_workers_requested = 0;
  int num_workers_returned = 0;
  int num_workers_returned_exiting = 0;
  int num_workers_disconnected = 0;
  int num_leases_canceled = 0;
  int num_get_task_failure_causes = 0;
  int reported_backlog_size = 0;
  std::map<SchedulingClass, int64_t> reported_backlogs;
  std::list<rpc::ClientCallback<rpc::RequestWorkerLeaseReply>> callbacks = {};
  std::list<rpc::ClientCallback<rpc::CancelWorkerLeaseReply>> cancel_callbacks = {};
  std::list<rpc::ClientCallback<rpc::GetWorkerFailureCauseReply>>
      get_task_failure_cause_callbacks = {};
};

class MockLeasePolicy : public LeasePolicyInterface {
 public:
  void SetNodeID(NodeID node_id) { fallback_rpc_address_.set_node_id(node_id.Binary()); }

  std::pair<rpc::Address, bool> GetBestNodeForLease(const LeaseSpecification &spec) {
    num_lease_policy_consults++;
    return std::make_pair(fallback_rpc_address_, is_locality_aware);
  };

  rpc::Address fallback_rpc_address_;

  int num_lease_policy_consults = 0;

  bool is_locality_aware = false;
};

TaskSpecification BuildEmptyTaskSpec() {
  std::unordered_map<std::string, double> empty_resources;
  FunctionDescriptor empty_descriptor =
      FunctionDescriptorBuilder::BuildPython("", "", "", "");
  return BuildTaskSpec(empty_resources, empty_descriptor);
}

TaskSpecification WithRandomTaskId(const TaskSpecification &task_spec) {
  auto copied_proto = task_spec.GetMessage();
  *copied_proto.mutable_task_id() = TaskID::FromRandom(JobID::Nil()).Binary();
  return TaskSpecification(std::move(copied_proto));
}

class NormalTaskSubmitterTest : public testing::Test {
 public:
  NormalTaskSubmitterTest()
      : local_node_id(NodeID::FromRandom()),
        raylet_client_pool(std::make_shared<rpc::RayletClientPool>(
            [](const rpc::Address &) { return std::make_shared<MockRayletClient>(); })),
        raylet_client(std::make_shared<MockRayletClient>()),
        worker_client(std::make_shared<MockWorkerClient>()),
        store(DefaultCoreWorkerMemoryStoreWithThread::CreateShared()),
        client_pool(std::make_shared<rpc::CoreWorkerClientPool>(
            [&](const rpc::Address &) { return worker_client; })),
        task_manager(std::make_unique<MockTaskManager>()),
        actor_creator(std::make_shared<FakeActorCreator>()),
        lease_policy(std::make_unique<MockLeasePolicy>()),
        lease_policy_ptr(lease_policy.get()) {
    address.set_node_id(local_node_id.Binary());
    lease_policy_ptr->SetNodeID(local_node_id);
  }

  NormalTaskSubmitter CreateNormalTaskSubmitter(
      std::shared_ptr<LeaseRequestRateLimiter> rate_limiter,
      WorkerType worker_type = WorkerType::WORKER,
      std::function<std::shared_ptr<RayletClientInterface>(const rpc::Address &)>
          raylet_client_factory = nullptr,
      std::shared_ptr<CoreWorkerMemoryStore> custom_memory_store = nullptr,
      int64_t lease_timeout_ms = kLongTimeout) {
    if (custom_memory_store != nullptr) {
      store = custom_memory_store;
    }
    if (raylet_client_factory == nullptr) {
      raylet_client_pool = std::make_shared<rpc::RayletClientPool>(
          [this](const rpc::Address &) { return this->raylet_client; });
    } else {
      raylet_client_pool = std::make_shared<rpc::RayletClientPool>(
          [this, raylet_client_factory](
              const rpc::Address &addr) -> std::shared_ptr<RayletClientInterface> {
            NodeID addr_node_id = NodeID::FromBinary(addr.node_id());
            if (addr_node_id == local_node_id) {
              return this->raylet_client;
            } else {
              return raylet_client_factory(addr);
            }
          });
    }
    return NormalTaskSubmitter(
        address,
        raylet_client,
        client_pool,
        raylet_client_pool,
        std::move(lease_policy),
        store,
        *task_manager,
        local_node_id,
        worker_type,
        lease_timeout_ms,
        actor_creator,
        JobID::Nil(),
        rate_limiter,
        [](const ObjectID &object_id) { return rpc::TensorTransport::OBJECT_STORE; },
        boost::asio::steady_timer(io_context),
        fake_scheduler_placement_time_ms_histogram_);
  }

  NodeID local_node_id;
  rpc::Address address;
  std::shared_ptr<rpc::RayletClientPool> raylet_client_pool;
  std::shared_ptr<MockRayletClient> raylet_client;
  std::shared_ptr<MockWorkerClient> worker_client;
  std::shared_ptr<CoreWorkerMemoryStore> store;
  std::shared_ptr<rpc::CoreWorkerClientPool> client_pool;
  std::unique_ptr<MockTaskManager> task_manager;
  std::shared_ptr<FakeActorCreator> actor_creator;
  // Note: Use lease_policy_ptr in tests, not lease_policy since it has to be moved into
  // the submitter.
  std::unique_ptr<MockLeasePolicy> lease_policy;
  MockLeasePolicy *lease_policy_ptr = nullptr;
  instrumented_io_context io_context;
  ray::observability::FakeHistogram fake_scheduler_placement_time_ms_histogram_;
};

TEST_F(NormalTaskSubmitterTest, TestLocalityAwareSubmitOneTask) {
  auto submitter =
      CreateNormalTaskSubmitter(std::make_shared<StaticLeaseRequestRateLimiter>(1));
  lease_policy_ptr->is_locality_aware = true;

  TaskSpecification task = BuildEmptyTaskSpec();

  submitter.SubmitTask(task);
  ASSERT_EQ(lease_policy_ptr->num_lease_policy_consults, 1);
  ASSERT_EQ(raylet_client->num_is_selected_based_on_locality_leases_requested, 1);
  ASSERT_EQ(raylet_client->num_workers_requested, 1);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(worker_client->callbacks.size(), 0);

  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1234, local_node_id));
  ASSERT_EQ(worker_client->callbacks.size(), 1);
  ASSERT_EQ(task_manager->num_tasks_complete, 0);
  ASSERT_EQ(task_manager->num_tasks_failed, 0);

  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(raylet_client->num_workers_returned, 1);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_manager->num_tasks_complete, 1);
  ASSERT_EQ(task_manager->num_tasks_failed, 0);
  ASSERT_EQ(task_manager->num_task_retries_attempted, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);
  ASSERT_FALSE(raylet_client->ReplyCancelWorkerLease());

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST_F(NormalTaskSubmitterTest, TestSubmitOneTask) {
  auto submitter =
      CreateNormalTaskSubmitter(std::make_shared<StaticLeaseRequestRateLimiter>(1));
  TaskSpecification task = BuildEmptyTaskSpec();

  submitter.SubmitTask(task);
  ASSERT_EQ(lease_policy_ptr->num_lease_policy_consults, 1);
  ASSERT_EQ(raylet_client->num_is_selected_based_on_locality_leases_requested, 0);
  ASSERT_EQ(raylet_client->num_workers_requested, 1);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(worker_client->callbacks.size(), 0);

  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1234, local_node_id));
  ASSERT_EQ(worker_client->callbacks.size(), 1);
  ASSERT_EQ(task_manager->num_tasks_complete, 0);
  ASSERT_EQ(task_manager->num_tasks_failed, 0);

  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(raylet_client->num_workers_returned, 1);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_manager->num_tasks_complete, 1);
  ASSERT_EQ(task_manager->num_tasks_failed, 0);
  ASSERT_EQ(task_manager->num_task_retries_attempted, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);
  ASSERT_FALSE(raylet_client->ReplyCancelWorkerLease());

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST_F(NormalTaskSubmitterTest, TestRetryTaskApplicationLevelError) {
  auto submitter =
      CreateNormalTaskSubmitter(std::make_shared<StaticLeaseRequestRateLimiter>(1));
  TaskSpecification task = BuildEmptyTaskSpec();
  task.GetMutableMessage().set_retry_exceptions(true);

  submitter.SubmitTask(task);
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1234, local_node_id));
  // Simulate an application-level error.
  ASSERT_TRUE(worker_client->ReplyPushTask(Status::OK(), false, true));
  ASSERT_EQ(raylet_client->num_workers_returned, 1);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_manager->num_tasks_complete, 1);
  ASSERT_EQ(task_manager->num_task_retries_attempted, 1);
  ASSERT_EQ(task_manager->num_tasks_failed, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);
  ASSERT_FALSE(raylet_client->ReplyCancelWorkerLease());

  task.GetMutableMessage().set_retry_exceptions(false);

  submitter.SubmitTask(task);
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1234, local_node_id));
  // Simulate an application-level error.
  ASSERT_TRUE(worker_client->ReplyPushTask(Status::OK(), false, true));
  ASSERT_EQ(raylet_client->num_workers_returned, 2);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_manager->num_tasks_complete, 2);
  ASSERT_EQ(task_manager->num_task_retries_attempted, 1);
  ASSERT_EQ(task_manager->num_tasks_failed, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);
  ASSERT_FALSE(raylet_client->ReplyCancelWorkerLease());

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST_F(NormalTaskSubmitterTest, TestHandleTaskFailure) {
  auto submitter =
      CreateNormalTaskSubmitter(std::make_shared<StaticLeaseRequestRateLimiter>(1));
  TaskSpecification task = BuildEmptyTaskSpec();

  submitter.SubmitTask(task);
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1234, local_node_id));
  // Simulate a system failure, i.e., worker died unexpectedly.
  ASSERT_TRUE(worker_client->ReplyPushTask(Status::IOError("oops")));
  ASSERT_TRUE(raylet_client->ReplyGetWorkerFailureCause());
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 1);
  ASSERT_EQ(raylet_client->num_get_task_failure_causes, 1);
  ASSERT_EQ(task_manager->num_tasks_complete, 0);
  ASSERT_EQ(task_manager->num_tasks_failed, 1);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);
  ASSERT_FALSE(raylet_client->ReplyCancelWorkerLease());

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST_F(NormalTaskSubmitterTest, TestCancellationWhileHandlingTaskFailure) {
  // This test is a regression test for a bug where a crash happens when
  // the task cancellation races between ReplyPushTask and ReplyGetWorkerFailureCause.
  // For an example of a python integration test, see
  // https://github.com/ray-project/ray/blob/2b6807f4d9c4572e6309f57bc404aa641bc4b185/python/ray/tests/test_cancel.py#L35
  auto submitter =
      CreateNormalTaskSubmitter(std::make_shared<StaticLeaseRequestRateLimiter>(1));

  TaskSpecification task = BuildEmptyTaskSpec();
  submitter.SubmitTask(task);
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1234, local_node_id));
  // Simulate a system failure, i.e., worker died unexpectedly so that
  // GetWorkerFailureCause is called.
  ASSERT_TRUE(worker_client->ReplyPushTask(Status::IOError("oops")));
  // Cancel the task while GetWorkerFailureCause has not been completed.
  submitter.CancelTask(task, true, false);
  // Completing the GetWorkerFailureCause call. Check that the reply runs without error
  // and FailPendingTask is not called.
  ASSERT_TRUE(raylet_client->ReplyGetWorkerFailureCause());
  ASSERT_EQ(task_manager->num_fail_pending_task_calls, 0);
}

TEST_F(NormalTaskSubmitterTest, TestHandleUnschedulableTask) {
  auto submitter =
      CreateNormalTaskSubmitter(std::make_shared<StaticLeaseRequestRateLimiter>(2));
  TaskSpecification task1 = BuildEmptyTaskSpec();
  TaskSpecification task2 = BuildEmptyTaskSpec();
  TaskSpecification task3 = BuildEmptyTaskSpec();

  submitter.SubmitTask(task1);
  submitter.SubmitTask(task2);
  submitter.SubmitTask(task3);
  ASSERT_EQ(lease_policy_ptr->num_lease_policy_consults, 2);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->reported_backlog_size, 0);
  ASSERT_EQ(worker_client->callbacks.size(), 0);

  // Fail task1 which will fail all the tasks
  ASSERT_TRUE(raylet_client->GrantWorkerLease(
      "",
      0,
      local_node_id,
      NodeID::Nil(),
      true,
      "",
      false,
      /*failure_type=*/
      rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_UNSCHEDULABLE));
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(task_manager->num_fail_pending_task_calls, 3);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);

  // Fail task2
  ASSERT_TRUE(raylet_client->GrantWorkerLease(
      "",
      0,
      local_node_id,
      NodeID::Nil(),
      true,
      "",
      false,
      /*failure_type=*/
      rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_UNSCHEDULABLE));
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(task_manager->num_fail_pending_task_calls, 3);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST_F(NormalTaskSubmitterTest, TestHandleRuntimeEnvSetupFailed) {
  auto submitter =
      CreateNormalTaskSubmitter(std::make_shared<StaticLeaseRequestRateLimiter>(2));

  TaskSpecification task1 = BuildEmptyTaskSpec();
  TaskSpecification task2 = BuildEmptyTaskSpec();
  TaskSpecification task3 = BuildEmptyTaskSpec();

  submitter.SubmitTask(task1);
  submitter.SubmitTask(task2);
  submitter.SubmitTask(task3);
  ASSERT_EQ(lease_policy_ptr->num_lease_policy_consults, 2);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->reported_backlog_size, 0);
  ASSERT_EQ(worker_client->callbacks.size(), 0);

  // Fail task1 which will fail all the tasks
  ASSERT_TRUE(raylet_client->GrantWorkerLease(
      "",
      0,
      local_node_id,
      NodeID::Nil(),
      true,
      "",
      false,
      /*failure_type=*/
      rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_RUNTIME_ENV_SETUP_FAILED));
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(task_manager->num_fail_pending_task_calls, 3);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);

  // Fail task2
  ASSERT_TRUE(raylet_client->GrantWorkerLease(
      "",
      0,
      local_node_id,
      NodeID::Nil(),
      true,
      "",
      false,
      /*failure_type=*/
      rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_RUNTIME_ENV_SETUP_FAILED));
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(task_manager->num_fail_pending_task_calls, 3);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST_F(NormalTaskSubmitterTest, TestWorkerHandleLocalRayletDied) {
  auto submitter =
      CreateNormalTaskSubmitter(std::make_shared<StaticLeaseRequestRateLimiter>(2));

  TaskSpecification task1 = BuildEmptyTaskSpec();
  submitter.SubmitTask(task1);
  ASSERT_DEATH(raylet_client->FailWorkerLeaseDueToGrpcUnavailable(), "");
}

TEST_F(NormalTaskSubmitterTest, TestDriverHandleLocalRayletDied) {
  auto submitter = CreateNormalTaskSubmitter(
      std::make_shared<StaticLeaseRequestRateLimiter>(2), WorkerType::DRIVER);

  TaskSpecification task1 = BuildEmptyTaskSpec();
  TaskSpecification task2 = BuildEmptyTaskSpec();
  TaskSpecification task3 = BuildEmptyTaskSpec();

  submitter.SubmitTask(task1);
  submitter.SubmitTask(task2);
  submitter.SubmitTask(task3);
  ASSERT_EQ(lease_policy_ptr->num_lease_policy_consults, 2);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->reported_backlog_size, 0);
  ASSERT_EQ(worker_client->callbacks.size(), 0);

  // Fail task1 which will fail all the tasks
  ASSERT_TRUE(raylet_client->FailWorkerLeaseDueToGrpcUnavailable());
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(task_manager->num_fail_pending_task_calls, 3);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);

  // Fail task2
  ASSERT_TRUE(raylet_client->FailWorkerLeaseDueToGrpcUnavailable());
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(task_manager->num_fail_pending_task_calls, 3);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST_F(NormalTaskSubmitterTest, TestConcurrentWorkerLeases) {
  int64_t concurrency = 10;
  auto rateLimiter = std::make_shared<StaticLeaseRequestRateLimiter>(concurrency);
  auto submitter = CreateNormalTaskSubmitter(rateLimiter);

  std::vector<TaskSpecification> tasks;
  for (int i = 0; i < 2 * concurrency; i++) {
    auto task = BuildEmptyTaskSpec();
    tasks.push_back(task);
    submitter.SubmitTask(task);
  }

  ASSERT_EQ(lease_policy_ptr->num_lease_policy_consults, concurrency);
  ASSERT_EQ(raylet_client->num_workers_requested, concurrency);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->reported_backlog_size, 0);
  ASSERT_EQ(worker_client->callbacks.size(), 0);

  // Trigger the periodic backlog report
  submitter.ReportWorkerBacklog();
  ASSERT_EQ(raylet_client->reported_backlog_size, concurrency);

  // Grant the first round of leases.
  for (int i = 0; i < concurrency; i++) {
    ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", i, local_node_id));
    ASSERT_EQ(worker_client->callbacks.size(), i + 1);
    ASSERT_EQ(lease_policy_ptr->num_lease_policy_consults, concurrency + i + 1);
    ASSERT_EQ(raylet_client->num_workers_requested, concurrency + i + 1);
    ASSERT_EQ(raylet_client->reported_backlog_size, concurrency - i - 1);
  }
  for (int i = 0; i < concurrency; i++) {
    ASSERT_TRUE(
        raylet_client->GrantWorkerLease("localhost", concurrency + i, local_node_id));
    ASSERT_EQ(worker_client->callbacks.size(), concurrency + i + 1);
    ASSERT_EQ(lease_policy_ptr->num_lease_policy_consults, tasks.size());
    ASSERT_EQ(raylet_client->num_workers_requested, tasks.size());
    ASSERT_EQ(raylet_client->reported_backlog_size, 0);
  }

  // All workers returned.
  while (!worker_client->callbacks.empty()) {
    ASSERT_TRUE(worker_client->ReplyPushTask());
  }
  ASSERT_EQ(raylet_client->num_workers_returned, tasks.size());
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_manager->num_tasks_complete, tasks.size());
  ASSERT_EQ(task_manager->num_tasks_failed, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);
  ASSERT_EQ(raylet_client->reported_backlog_size, 0);
  ASSERT_FALSE(raylet_client->ReplyCancelWorkerLease());

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST_F(NormalTaskSubmitterTest, TestConcurrentWorkerLeasesDynamic) {
  int64_t concurrency = 10;
  auto rateLimiter = std::make_shared<DynamicRateLimiter>(1);
  auto submitter = CreateNormalTaskSubmitter(rateLimiter);

  std::vector<TaskSpecification> tasks;
  for (int i = 0; i < 2 * concurrency; i++) {
    auto task = BuildEmptyTaskSpec();
    tasks.push_back(task);
    submitter.SubmitTask(task);
  }

  ASSERT_EQ(lease_policy_ptr->num_lease_policy_consults, 1);
  ASSERT_EQ(raylet_client->num_workers_requested, 1);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->reported_backlog_size, 0);
  ASSERT_EQ(worker_client->callbacks.size(), 0);

  // Trigger the periodic backlog report
  submitter.ReportWorkerBacklog();
  ASSERT_EQ(raylet_client->reported_backlog_size, tasks.size() - 1);

  // Max concurrency is still 1.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1000, local_node_id));
  ASSERT_EQ(lease_policy_ptr->num_lease_policy_consults, 2);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);
  ASSERT_EQ(raylet_client->reported_backlog_size, tasks.size() - 2);

  // Increase max concurrency. Should request leases up to the max concurrency.
  rateLimiter->limit_ = concurrency;
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1001, local_node_id));
  ASSERT_EQ(lease_policy_ptr->num_lease_policy_consults, 2 + concurrency);
  ASSERT_EQ(raylet_client->num_workers_requested, 2 + concurrency);
  ASSERT_EQ(raylet_client->reported_backlog_size,
            tasks.size() - raylet_client->num_workers_requested);

  // Decrease max concurrency again. Should not request any more leases even as
  // previous requests are granted, since we are still over the current
  // concurrency.
  rateLimiter->limit_ = 1;
  for (int i = 0; i < concurrency - 1; i++) {
    ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", i, local_node_id));
    ASSERT_EQ(lease_policy_ptr->num_lease_policy_consults, 2 + concurrency);
    ASSERT_EQ(raylet_client->num_workers_requested, 2 + concurrency);
    ASSERT_EQ(raylet_client->reported_backlog_size,
              tasks.size() - raylet_client->num_workers_requested);
  }

  // Grant remaining leases with max lease concurrency of 1.
  int num_tasks_remaining = tasks.size() - raylet_client->num_workers_requested;
  lease_policy_ptr->num_lease_policy_consults = 0;
  raylet_client->num_workers_requested = 0;
  for (int i = 0; i < num_tasks_remaining; i++) {
    ASSERT_TRUE(
        raylet_client->GrantWorkerLease("localhost", concurrency + i, local_node_id));
    ASSERT_EQ(lease_policy_ptr->num_lease_policy_consults, i + 1);
    ASSERT_EQ(raylet_client->num_workers_requested, i + 1);
  }

  lease_policy_ptr->num_lease_policy_consults = 0;
  raylet_client->num_workers_requested = 0;
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 2000, local_node_id));
  ASSERT_EQ(lease_policy_ptr->num_lease_policy_consults, 0);
  ASSERT_EQ(raylet_client->num_workers_requested, 0);

  // All workers returned.
  while (!worker_client->callbacks.empty()) {
    ASSERT_TRUE(worker_client->ReplyPushTask());
  }
  ASSERT_EQ(raylet_client->num_workers_returned, tasks.size());
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_manager->num_tasks_complete, tasks.size());
  ASSERT_EQ(task_manager->num_tasks_failed, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);
  ASSERT_EQ(raylet_client->reported_backlog_size, 0);
  ASSERT_FALSE(raylet_client->ReplyCancelWorkerLease());

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST_F(NormalTaskSubmitterTest, TestConcurrentWorkerLeasesDynamicWithSpillback) {
  int64_t concurrency = 10;
  auto rateLimiter = std::make_shared<DynamicRateLimiter>(1);
  auto submitter = CreateNormalTaskSubmitter(
      rateLimiter,
      WorkerType::WORKER,
      /*raylet_client_factory*/ [&](const rpc::Address &addr) { return raylet_client; });

  std::vector<TaskSpecification> tasks;
  for (int i = 0; i < 2 * concurrency; i++) {
    auto task = BuildEmptyTaskSpec();
    tasks.push_back(task);
    submitter.SubmitTask(task);
  }

  ASSERT_EQ(lease_policy_ptr->num_lease_policy_consults, 1);
  ASSERT_EQ(raylet_client->num_workers_requested, 1);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->reported_backlog_size, 0);
  ASSERT_EQ(worker_client->callbacks.size(), 0);

  // Trigger the periodic backlog report
  submitter.ReportWorkerBacklog();
  ASSERT_EQ(raylet_client->reported_backlog_size, tasks.size() - 1);

  // Max concurrency is still 1.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1000, local_node_id));
  ASSERT_EQ(lease_policy_ptr->num_lease_policy_consults, 2);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);
  ASSERT_EQ(raylet_client->reported_backlog_size, tasks.size() - 2);

  // Increase max concurrency.
  rateLimiter->limit_ = concurrency;
  // The outstanding lease request is spilled back to a remote raylet.
  auto remote_node_id = NodeID::FromRandom();
  ASSERT_TRUE(
      raylet_client->GrantWorkerLease("localhost", 1001, NodeID::Nil(), remote_node_id));
  // We should request one lease request from the spillback raylet and then the
  // rest from the raylet returned by the lease policy.
  ASSERT_EQ(lease_policy_ptr->num_lease_policy_consults, concurrency + 1);
  ASSERT_EQ(raylet_client->num_workers_requested, 2 + concurrency);
  ASSERT_EQ(raylet_client->reported_backlog_size,
            tasks.size() - raylet_client->num_workers_requested + 1);

  // Decrease max concurrency again. Should not request any more leases even as
  // previous requests are granted, since we are still over the current
  // concurrency.
  rateLimiter->limit_ = 1;
  for (int i = 0; i < concurrency - 1; i++) {
    ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", i, local_node_id));
    ASSERT_EQ(lease_policy_ptr->num_lease_policy_consults, concurrency + 1);
    ASSERT_EQ(raylet_client->num_workers_requested, 2 + concurrency);
    ASSERT_EQ(raylet_client->reported_backlog_size,
              tasks.size() - raylet_client->num_workers_requested + 1);
  }

  // Grant remaining leases with max lease concurrency of 1.
  int num_tasks_remaining = tasks.size() - raylet_client->num_workers_requested + 1;
  lease_policy_ptr->num_lease_policy_consults = 0;
  raylet_client->num_workers_requested = 0;
  for (int i = 0; i < num_tasks_remaining; i++) {
    ASSERT_TRUE(
        raylet_client->GrantWorkerLease("localhost", concurrency + i, local_node_id));
    ASSERT_EQ(lease_policy_ptr->num_lease_policy_consults, i + 1);
    ASSERT_EQ(raylet_client->num_workers_requested, i + 1);
  }

  lease_policy_ptr->num_lease_policy_consults = 0;
  raylet_client->num_workers_requested = 0;
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 2000, local_node_id));
  ASSERT_EQ(lease_policy_ptr->num_lease_policy_consults, 0);
  ASSERT_EQ(raylet_client->num_workers_requested, 0);

  // All workers returned.
  while (!worker_client->callbacks.empty()) {
    ASSERT_TRUE(worker_client->ReplyPushTask());
  }
  ASSERT_EQ(raylet_client->num_workers_returned, tasks.size());
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_manager->num_tasks_complete, tasks.size());
  ASSERT_EQ(task_manager->num_tasks_failed, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);
  ASSERT_EQ(raylet_client->reported_backlog_size, 0);
  ASSERT_FALSE(raylet_client->ReplyCancelWorkerLease());

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST_F(NormalTaskSubmitterTest, TestSubmitMultipleTasks) {
  auto submitter =
      CreateNormalTaskSubmitter(std::make_shared<StaticLeaseRequestRateLimiter>(1));
  TaskSpecification task1 = BuildEmptyTaskSpec();
  TaskSpecification task2 = BuildEmptyTaskSpec();
  TaskSpecification task3 = BuildEmptyTaskSpec();

  submitter.SubmitTask(task1);
  submitter.SubmitTask(task2);
  submitter.SubmitTask(task3);
  ASSERT_EQ(lease_policy_ptr->num_lease_policy_consults, 1);
  ASSERT_EQ(raylet_client->num_workers_requested, 1);
  ASSERT_EQ(raylet_client->reported_backlog_size, 0);

  // Task 1 is pushed; worker 2 is requested.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1000, local_node_id));
  ASSERT_EQ(worker_client->callbacks.size(), 1);
  ASSERT_EQ(lease_policy_ptr->num_lease_policy_consults, 2);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);
  ASSERT_EQ(raylet_client->reported_backlog_size, 1);

  // Task 2 is pushed; worker 3 is requested.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1001, local_node_id));
  ASSERT_EQ(worker_client->callbacks.size(), 2);
  ASSERT_EQ(lease_policy_ptr->num_lease_policy_consults, 3);
  ASSERT_EQ(raylet_client->num_workers_requested, 3);
  ASSERT_EQ(raylet_client->reported_backlog_size, 0);

  // Task 3 is pushed; no more workers requested.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1002, local_node_id));
  ASSERT_EQ(worker_client->callbacks.size(), 3);
  ASSERT_EQ(lease_policy_ptr->num_lease_policy_consults, 3);
  ASSERT_EQ(raylet_client->num_workers_requested, 3);

  // All workers returned.
  while (!worker_client->callbacks.empty()) {
    ASSERT_TRUE(worker_client->ReplyPushTask());
  }
  ASSERT_EQ(raylet_client->num_workers_returned, 3);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_manager->num_tasks_complete, 3);
  ASSERT_EQ(task_manager->num_tasks_failed, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);
  ASSERT_EQ(raylet_client->reported_backlog_size, 0);
  ASSERT_FALSE(raylet_client->ReplyCancelWorkerLease());

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST_F(NormalTaskSubmitterTest, TestReuseWorkerLease) {
  auto submitter =
      CreateNormalTaskSubmitter(std::make_shared<StaticLeaseRequestRateLimiter>(1));
  TaskSpecification task1 = BuildEmptyTaskSpec();
  TaskSpecification task2 = BuildEmptyTaskSpec();
  TaskSpecification task3 = BuildEmptyTaskSpec();

  submitter.SubmitTask(task1);
  submitter.SubmitTask(task2);
  submitter.SubmitTask(task3);
  ASSERT_EQ(lease_policy_ptr->num_lease_policy_consults, 1);
  ASSERT_EQ(raylet_client->num_workers_requested, 1);

  // Task 1 is pushed.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1000, local_node_id));
  ASSERT_EQ(worker_client->callbacks.size(), 1);
  ASSERT_EQ(lease_policy_ptr->num_lease_policy_consults, 2);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);

  // Task 1 finishes, Task 2 is scheduled on the same worker.
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(worker_client->callbacks.size(), 1);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);

  // Task 2 finishes, Task 3 is scheduled on the same worker.
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(worker_client->callbacks.size(), 1);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 1);
  ASSERT_TRUE(raylet_client->ReplyCancelWorkerLease());
  // Task 3 finishes, the worker is returned.
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(raylet_client->num_workers_returned, 1);

  // The second lease request is returned immediately.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1001, local_node_id));
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(lease_policy_ptr->num_lease_policy_consults, 2);
  ASSERT_EQ(raylet_client->num_workers_returned, 2);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_manager->num_tasks_complete, 3);
  ASSERT_EQ(task_manager->num_tasks_failed, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 1);
  ASSERT_FALSE(raylet_client->ReplyCancelWorkerLease());

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST_F(NormalTaskSubmitterTest, TestRetryLeaseCancellation) {
  auto submitter =
      CreateNormalTaskSubmitter(std::make_shared<StaticLeaseRequestRateLimiter>(1));
  TaskSpecification task1 = BuildEmptyTaskSpec();
  TaskSpecification task2 = BuildEmptyTaskSpec();
  TaskSpecification task3 = BuildEmptyTaskSpec();

  submitter.SubmitTask(task1);
  submitter.SubmitTask(task2);
  submitter.SubmitTask(task3);
  ASSERT_EQ(raylet_client->num_workers_requested, 1);

  // Task 1 is pushed.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1000, local_node_id));
  // Task 1 finishes, Task 2 is scheduled on the same worker.
  ASSERT_TRUE(worker_client->ReplyPushTask());
  // Task 2 finishes, Task 3 is scheduled on the same worker.
  ASSERT_TRUE(worker_client->ReplyPushTask());
  // Task 3 finishes, the worker is returned.
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(raylet_client->num_workers_returned, 1);

  // Simulate the lease cancellation request failing because it arrives at the
  // raylet before the last worker lease request has been received.
  int i = 1;
  for (; i <= 3; i++) {
    ASSERT_EQ(raylet_client->num_leases_canceled, i);
    ASSERT_TRUE(raylet_client->ReplyCancelWorkerLease(false));
  }

  // Simulate the lease cancellation request succeeding.
  ASSERT_TRUE(raylet_client->ReplyCancelWorkerLease());
  ASSERT_EQ(raylet_client->num_leases_canceled, i);
  ASSERT_FALSE(raylet_client->ReplyCancelWorkerLease());
  ASSERT_EQ(raylet_client->num_leases_canceled, i);
  ASSERT_TRUE(raylet_client->GrantWorkerLease(
      "", 0, local_node_id, NodeID::Nil(), /*cancel=*/true));
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  // The canceled lease is not returned.
  ASSERT_EQ(raylet_client->num_workers_returned, 1);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_manager->num_tasks_complete, 3);
  ASSERT_EQ(task_manager->num_tasks_failed, 0);

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST_F(NormalTaskSubmitterTest, TestConcurrentCancellationAndSubmission) {
  auto submitter =
      CreateNormalTaskSubmitter(std::make_shared<StaticLeaseRequestRateLimiter>(1));
  TaskSpecification task1 = BuildEmptyTaskSpec();
  TaskSpecification task2 = BuildEmptyTaskSpec();
  TaskSpecification task3 = BuildEmptyTaskSpec();

  submitter.SubmitTask(task1);
  submitter.SubmitTask(task2);

  // Task 1 is pushed.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1000, local_node_id));
  ASSERT_EQ(raylet_client->num_workers_requested, 2);
  // Task 1 finishes, Task 2 is scheduled on the same worker.
  ASSERT_TRUE(worker_client->ReplyPushTask());

  // Task 2's lease request gets canceled.
  ASSERT_EQ(raylet_client->num_leases_canceled, 1);

  // Task 2 finishes, the worker is returned.
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(raylet_client->num_workers_returned, 1);

  // Another task is submitted while task 2's lease request is being canceled.
  submitter.SubmitTask(task3);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);

  // Task 2's lease request is canceled, a new worker is requested for task 3.
  ASSERT_TRUE(raylet_client->ReplyCancelWorkerLease());
  ASSERT_EQ(raylet_client->num_workers_requested, 2);
  ASSERT_TRUE(raylet_client->GrantWorkerLease(
      "", 0, local_node_id, NodeID::Nil(), /*cancel=*/true));
  ASSERT_EQ(raylet_client->num_workers_requested, 3);

  // Task 3 finishes, all workers returned.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1000, local_node_id));
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(raylet_client->num_workers_returned, 2);
  ASSERT_FALSE(raylet_client->ReplyCancelWorkerLease());
  ASSERT_EQ(raylet_client->num_leases_canceled, 1);

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST_F(NormalTaskSubmitterTest, TestWorkerNotReusedOnError) {
  auto submitter =
      CreateNormalTaskSubmitter(std::make_shared<StaticLeaseRequestRateLimiter>(1));
  TaskSpecification task1 = BuildEmptyTaskSpec();
  TaskSpecification task2 = BuildEmptyTaskSpec();

  submitter.SubmitTask(task1);
  submitter.SubmitTask(task2);
  ASSERT_EQ(raylet_client->num_workers_requested, 1);

  // Task 1 is pushed.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1000, local_node_id));
  ASSERT_EQ(worker_client->callbacks.size(), 1);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);

  // Task 1 finishes with failure; the worker is returned.
  ASSERT_TRUE(worker_client->ReplyPushTask(Status::IOError("worker dead")));
  ASSERT_TRUE(raylet_client->ReplyGetWorkerFailureCause());
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 1);

  // Task 2 runs successfully on the second worker.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1001, local_node_id));
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(raylet_client->num_workers_returned, 1);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 1);
  ASSERT_EQ(task_manager->num_tasks_complete, 1);
  ASSERT_EQ(task_manager->num_tasks_failed, 1);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);
  ASSERT_FALSE(raylet_client->ReplyCancelWorkerLease());

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST_F(NormalTaskSubmitterTest, TestWorkerNotReturnedOnExit) {
  auto submitter =
      CreateNormalTaskSubmitter(std::make_shared<StaticLeaseRequestRateLimiter>(1));
  TaskSpecification task1 = BuildEmptyTaskSpec();

  submitter.SubmitTask(task1);
  ASSERT_EQ(raylet_client->num_workers_requested, 1);

  // Task 1 is pushed.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1000, local_node_id));
  ASSERT_EQ(worker_client->callbacks.size(), 1);

  // Task 1 finishes with exit status; the worker is not returned.
  ASSERT_TRUE(worker_client->ReplyPushTask(Status::OK(), /*exit=*/true));
  ASSERT_EQ(raylet_client->num_workers_returned, 1);
  ASSERT_EQ(raylet_client->num_workers_returned_exiting, 1);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_manager->num_tasks_complete, 1);
  ASSERT_EQ(task_manager->num_tasks_failed, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);
  ASSERT_FALSE(raylet_client->ReplyCancelWorkerLease());

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST_F(NormalTaskSubmitterTest, TestSpillback) {
  absl::flat_hash_map<int, std::shared_ptr<MockRayletClient>> remote_raylet_clients;
  auto raylet_client_factory = [&remote_raylet_clients](const rpc::Address &addr) {
    RAY_CHECK(remote_raylet_clients.count(addr.port()) == 0);
    auto client = std::make_shared<MockRayletClient>();
    remote_raylet_clients[addr.port()] = client;
    return client;
  };
  auto submitter =
      CreateNormalTaskSubmitter(std::make_shared<StaticLeaseRequestRateLimiter>(1),
                                WorkerType::WORKER,
                                raylet_client_factory);
  TaskSpecification task = BuildEmptyTaskSpec();

  submitter.SubmitTask(task);
  ASSERT_EQ(lease_policy_ptr->num_lease_policy_consults, 1);
  ASSERT_EQ(raylet_client->num_workers_requested, 1);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(remote_raylet_clients.size(), 0);

  // Spillback to a remote node.
  auto remote_node_id = NodeID::FromRandom();
  ASSERT_TRUE(
      raylet_client->GrantWorkerLease("localhost", 7777, NodeID::Nil(), remote_node_id));
  ASSERT_EQ(remote_raylet_clients.count(7777), 1);
  // Confirm that lease policy is not consulted on spillback.
  ASSERT_EQ(lease_policy_ptr->num_lease_policy_consults, 1);
  // There should be no more callbacks on the local client.
  ASSERT_FALSE(raylet_client->GrantWorkerLease("remote", 1234, local_node_id));
  // Trigger retry at the remote node.
  ASSERT_TRUE(
      remote_raylet_clients[7777]->GrantWorkerLease("remote", 1234, remote_node_id));

  // The worker is returned to the remote node, not the local one.
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(remote_raylet_clients[7777]->num_workers_returned, 1);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(remote_raylet_clients[7777]->num_workers_disconnected, 0);
  ASSERT_EQ(task_manager->num_tasks_complete, 1);
  ASSERT_EQ(task_manager->num_tasks_failed, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);
  ASSERT_FALSE(raylet_client->ReplyCancelWorkerLease());
  for (const auto &remote_client : remote_raylet_clients) {
    ASSERT_EQ(remote_client.second->num_leases_canceled, 0);
    ASSERT_FALSE(remote_client.second->ReplyCancelWorkerLease());
  }

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST_F(NormalTaskSubmitterTest, TestSpillbackRoundTrip) {
  absl::flat_hash_map<int, std::shared_ptr<MockRayletClient>> remote_raylet_clients;
  auto raylet_client_factory = [&](const rpc::Address &addr) {
    // We should not create a connection to the same raylet more than once.
    RAY_CHECK(remote_raylet_clients.count(addr.port()) == 0);
    auto client = std::make_shared<MockRayletClient>();
    remote_raylet_clients[addr.port()] = client;
    return client;
  };
  auto memory_store = DefaultCoreWorkerMemoryStoreWithThread::CreateShared();
  auto submitter =
      CreateNormalTaskSubmitter(std::make_shared<StaticLeaseRequestRateLimiter>(1),
                                WorkerType::WORKER,
                                raylet_client_factory,
                                memory_store,
                                kLongTimeout);
  TaskSpecification task = BuildEmptyTaskSpec();

  submitter.SubmitTask(task);
  ASSERT_EQ(raylet_client->num_grant_or_reject_leases_requested, 0);
  ASSERT_EQ(raylet_client->num_workers_requested, 1);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(remote_raylet_clients.size(), 0);

  // Spillback to a remote node.
  auto remote_node_id = NodeID::FromRandom();
  rpc::Address remote_address;
  remote_address.set_node_id(remote_node_id.Binary());
  remote_address.set_ip_address("localhost");
  remote_address.set_port(7777);
  raylet_client_pool->GetOrConnectByAddress(remote_address);
  ASSERT_TRUE(
      raylet_client->GrantWorkerLease("localhost", 7777, NodeID::Nil(), remote_node_id));
  ASSERT_EQ(remote_raylet_clients.count(7777), 1);
  ASSERT_EQ(remote_raylet_clients[7777]->num_workers_requested, 1);
  // Confirm that the spillback lease request has grant_or_reject set to true.
  ASSERT_EQ(remote_raylet_clients[7777]->num_grant_or_reject_leases_requested, 1);
  // Confirm that lease policy is not consulted on spillback.
  ASSERT_EQ(lease_policy_ptr->num_lease_policy_consults, 1);
  ASSERT_FALSE(raylet_client->GrantWorkerLease("remote", 1234, local_node_id));
  // Trigger a rejection back to the local node.
  ASSERT_TRUE(remote_raylet_clients[7777]->GrantWorkerLease(
      "local", 1234, remote_node_id, NodeID::Nil(), false, "", /*reject=*/true));
  // We should not have created another lease client to the local raylet.
  ASSERT_EQ(remote_raylet_clients.size(), 1);
  // There should be no more callbacks on the remote node.
  ASSERT_FALSE(
      remote_raylet_clients[7777]->GrantWorkerLease("remote", 1234, remote_node_id));

  // The worker is returned to the local node.
  ASSERT_EQ(raylet_client->num_grant_or_reject_leases_requested, 0);
  ASSERT_EQ(raylet_client->num_workers_requested, 2);
  ASSERT_TRUE(raylet_client->GrantWorkerLease("local", 1234, local_node_id));
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(raylet_client->num_workers_returned, 1);
  ASSERT_EQ(remote_raylet_clients[7777]->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(remote_raylet_clients[7777]->num_workers_disconnected, 0);
  ASSERT_EQ(task_manager->num_tasks_complete, 1);
  ASSERT_EQ(task_manager->num_tasks_failed, 0);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);
  ASSERT_FALSE(raylet_client->ReplyCancelWorkerLease());
  for (const auto &remote_client : remote_raylet_clients) {
    ASSERT_EQ(remote_client.second->num_leases_canceled, 0);
    ASSERT_FALSE(remote_client.second->ReplyCancelWorkerLease());
  }

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

// Helper to run a test that checks that 'same1' and 'same2' are treated as the same
// resource shape, while 'different' is treated as a separate shape.
void TestSchedulingKey(const std::shared_ptr<CoreWorkerMemoryStore> store,
                       const TaskSpecification &same1,
                       const TaskSpecification &same2,
                       const TaskSpecification &different) {
  rpc::Address address;
  ray::observability::FakeHistogram fake_scheduler_placement_time_ms_histogram_;
  auto local_node_id = NodeID::FromRandom();
  auto raylet_client = std::make_shared<MockRayletClient>();
  auto raylet_client_pool = std::make_shared<rpc::RayletClientPool>(
      [&](const rpc::Address &addr) { return raylet_client; });
  auto worker_client = std::make_shared<MockWorkerClient>();
  auto client_pool = std::make_shared<rpc::CoreWorkerClientPool>(
      [&](const rpc::Address &addr) { return worker_client; });
  auto task_manager = std::make_unique<MockTaskManager>();
  auto actor_creator = std::make_shared<FakeActorCreator>();
  auto lease_policy = std::make_unique<MockLeasePolicy>();
  lease_policy->SetNodeID(local_node_id);
  instrumented_io_context io_context;
  NormalTaskSubmitter submitter(
      address,
      raylet_client,
      client_pool,
      raylet_client_pool,
      std::move(lease_policy),
      store,
      *task_manager,
      local_node_id,
      WorkerType::WORKER,
      kLongTimeout,
      actor_creator,
      JobID::Nil(),
      std::make_shared<StaticLeaseRequestRateLimiter>(1),
      [](const ObjectID &object_id) { return rpc::TensorTransport::OBJECT_STORE; },
      boost::asio::steady_timer(io_context),
      fake_scheduler_placement_time_ms_histogram_);

  submitter.SubmitTask(same1);
  submitter.SubmitTask(same2);
  submitter.SubmitTask(different);

  WaitForCondition(
      [&raylet_client]() { return raylet_client->num_workers_returned == 2; },
      /*timeout_ms=*/1000);

  // same1 is pushed.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1000, local_node_id));
  ASSERT_EQ(worker_client->callbacks.size(), 1);
  // Another worker is requested because same2 is pending.
  ASSERT_EQ(raylet_client->num_workers_requested, 3);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);

  // same1 runs successfully. Worker isn't returned.
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  // same2 is pushed.
  ASSERT_EQ(worker_client->callbacks.size(), 1);
  ASSERT_EQ(raylet_client->num_leases_canceled, 1);
  ASSERT_TRUE(raylet_client->ReplyCancelWorkerLease());

  // different is pushed.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1001, local_node_id));
  ASSERT_EQ(worker_client->callbacks.size(), 2);
  ASSERT_EQ(raylet_client->num_workers_requested, 3);

  // same2 runs successfully. Worker is returned.
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(raylet_client->num_workers_returned, 1);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);

  // different runs successfully. Worker is returned.
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(raylet_client->num_workers_returned, 2);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);

  ASSERT_EQ(raylet_client->num_leases_canceled, 1);

  // Trigger reply to RequestWorkerLease to remove the canceled pending lease request
  ASSERT_TRUE(raylet_client->GrantWorkerLease(
      "localhost", 1002, local_node_id, NodeID::Nil(), true));
  ASSERT_EQ(raylet_client->num_workers_returned, 2);

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST(NormalTaskSubmitterSchedulingKeyTest, TestSchedulingKeys) {
  InstrumentedIOContextWithThread io_context("TestSchedulingKeys");
  auto memory_store = std::make_shared<CoreWorkerMemoryStore>(io_context.GetIoService());

  std::unordered_map<std::string, double> resources1({{"a", 1.0}});
  std::unordered_map<std::string, double> resources2({{"b", 2.0}});
  FunctionDescriptor descriptor1 =
      FunctionDescriptorBuilder::BuildPython("a", "", "", "");
  FunctionDescriptor descriptor2 =
      FunctionDescriptorBuilder::BuildPython("b", "", "", "");

  // Tasks with different resources should request different worker leases.
  RAY_LOG(INFO) << "Test different resources";
  TestSchedulingKey(memory_store,
                    BuildTaskSpec(resources1, descriptor1),
                    BuildTaskSpec(resources1, descriptor1),
                    BuildTaskSpec(resources2, descriptor1));

  // Tasks with different functions should request different worker leases.
  RAY_LOG(INFO) << "Test different functions";
  TestSchedulingKey(memory_store,
                    BuildTaskSpec(resources1, descriptor1),
                    BuildTaskSpec(resources1, descriptor1),
                    BuildTaskSpec(resources1, descriptor2));

  // Tasks with different depths should request different worker leases.
  RAY_LOG(INFO) << "Test different depths";
  TestSchedulingKey(memory_store,
                    BuildTaskSpec(resources1, descriptor1, 0),
                    BuildTaskSpec(resources1, descriptor1, 0),
                    BuildTaskSpec(resources1, descriptor1, 1));

  // Tasks with different runtime envs do not request different workers.
  RAY_LOG(INFO) << "Test different runtimes";
  TestSchedulingKey(memory_store,
                    BuildTaskSpec(resources1, descriptor1, 0, "a"),
                    BuildTaskSpec(resources1, descriptor1, 0, "b"),
                    BuildTaskSpec(resources1, descriptor1, 1, "a"));

  ObjectID direct1 = ObjectID::FromRandom();
  ObjectID direct2 = ObjectID::FromRandom();
  ObjectID plasma1 = ObjectID::FromRandom();
  ObjectID plasma2 = ObjectID::FromRandom();
  // Ensure the data is already present in the local store for direct call objects.
  auto data = GenerateRandomObject();
  memory_store->Put(*data, direct1);
  memory_store->Put(*data, direct2);

  // Force plasma objects to be promoted.
  std::string meta = std::to_string(static_cast<int>(rpc::ErrorType::OBJECT_IN_PLASMA));
  auto metadata = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(meta.data()));
  auto meta_buffer = std::make_shared<LocalMemoryBuffer>(metadata, meta.size());
  auto plasma_data = RayObject(nullptr, meta_buffer, std::vector<rpc::ObjectReference>());
  memory_store->Put(plasma_data, plasma1);
  memory_store->Put(plasma_data, plasma2);

  TaskSpecification same_deps_1 = BuildTaskSpec(resources1, descriptor1);
  same_deps_1.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(
      direct1.Binary());
  same_deps_1.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(
      plasma1.Binary());
  TaskSpecification same_deps_2 = BuildTaskSpec(resources1, descriptor1);
  same_deps_2.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(
      direct1.Binary());
  same_deps_2.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(
      direct2.Binary());
  same_deps_2.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(
      plasma1.Binary());

  TaskSpecification different_deps = BuildTaskSpec(resources1, descriptor1);
  different_deps.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(
      direct1.Binary());
  different_deps.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(
      direct2.Binary());
  different_deps.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(
      plasma2.Binary());

  // Tasks with different plasma dependencies should request different worker leases,
  // but direct call dependencies shouldn't be considered.
  RAY_LOG(INFO) << "Test different dependencies";
  TestSchedulingKey(memory_store, same_deps_1, same_deps_2, different_deps);
}

TEST_F(NormalTaskSubmitterTest, TestBacklogReport) {
  InstrumentedIOContextWithThread store_io_context("TestBacklogReport");
  auto memory_store =
      std::make_shared<CoreWorkerMemoryStore>(store_io_context.GetIoService());
  auto submitter =
      CreateNormalTaskSubmitter(std::make_shared<StaticLeaseRequestRateLimiter>(1),
                                WorkerType::WORKER,
                                /*raylet_client_factory=*/nullptr,
                                memory_store);

  TaskSpecification task1 = BuildEmptyTaskSpec();

  std::unordered_map<std::string, double> resources1({{"a", 1.0}});
  std::unordered_map<std::string, double> resources2({{"b", 2.0}});
  FunctionDescriptor descriptor1 =
      FunctionDescriptorBuilder::BuildPython("a", "", "", "");
  FunctionDescriptor descriptor2 =
      FunctionDescriptorBuilder::BuildPython("b", "", "", "");
  ObjectID plasma1 = ObjectID::FromRandom();
  ObjectID plasma2 = ObjectID::FromRandom();
  // Force plasma objects to be promoted.
  std::string meta = std::to_string(static_cast<int>(rpc::ErrorType::OBJECT_IN_PLASMA));
  auto metadata = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(meta.data()));
  auto meta_buffer = std::make_shared<LocalMemoryBuffer>(metadata, meta.size());
  auto plasma_data = RayObject(nullptr, meta_buffer, std::vector<rpc::ObjectReference>());
  memory_store->Put(plasma_data, plasma1);
  memory_store->Put(plasma_data, plasma2);

  // Same SchedulingClass, different SchedulingKey
  TaskSpecification task2 = BuildTaskSpec(resources1, descriptor1);
  task2.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(
      plasma1.Binary());
  TaskSpecification task3 = BuildTaskSpec(resources1, descriptor1);
  task3.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(
      plasma2.Binary());
  TestSchedulingKey(
      memory_store, WithRandomTaskId(task2), WithRandomTaskId(task2), task3);

  TaskSpecification task4 = BuildTaskSpec(resources2, descriptor2);

  submitter.SubmitTask(task1);
  // One is requested and one is in the backlog for each SchedulingKey
  submitter.SubmitTask(WithRandomTaskId(task2));
  submitter.SubmitTask(WithRandomTaskId(task2));
  submitter.SubmitTask(WithRandomTaskId(task3));
  submitter.SubmitTask(WithRandomTaskId(task3));
  submitter.SubmitTask(WithRandomTaskId(task4));
  submitter.SubmitTask(WithRandomTaskId(task4));

  // Waits for the async callbacks in submitter.SubmitTask to finish, before we call
  // ReportWorkerBacklog.
  std::promise<bool> wait_for_io_ctx_empty;
  store_io_context.GetIoService().post(
      [&wait_for_io_ctx_empty]() { wait_for_io_ctx_empty.set_value(true); },
      "wait_for_io_ctx_empty");
  wait_for_io_ctx_empty.get_future().get();

  submitter.ReportWorkerBacklog();
  ASSERT_EQ(raylet_client->reported_backlogs.size(), 3);
  ASSERT_EQ(raylet_client->reported_backlogs[task1.GetSchedulingClass()], 0);
  ASSERT_EQ(raylet_client->reported_backlogs[task2.GetSchedulingClass()], 2);
  ASSERT_EQ(raylet_client->reported_backlogs[task4.GetSchedulingClass()], 1);
}

TEST_F(NormalTaskSubmitterTest, TestWorkerLeaseTimeout) {
  auto memory_store = DefaultCoreWorkerMemoryStoreWithThread::CreateShared();
  auto submitter =
      CreateNormalTaskSubmitter(std::make_shared<StaticLeaseRequestRateLimiter>(1),
                                WorkerType::WORKER,
                                /*raylet_client_factory=*/nullptr,
                                memory_store,
                                /*lease_timeout_ms=*/5);
  TaskSpecification task1 = BuildEmptyTaskSpec();
  TaskSpecification task2 = BuildEmptyTaskSpec();
  TaskSpecification task3 = BuildEmptyTaskSpec();

  submitter.SubmitTask(task1);
  submitter.SubmitTask(task2);
  submitter.SubmitTask(task3);
  ASSERT_EQ(raylet_client->num_workers_requested, 1);

  // Task 1 is pushed.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1000, local_node_id));
  ASSERT_EQ(raylet_client->num_workers_requested, 2);

  // Task 1 finishes with failure; the worker is returned due to the error even though
  // it hasn't timed out.
  ASSERT_TRUE(worker_client->ReplyPushTask(Status::IOError("worker dead")));
  ASSERT_TRUE(raylet_client->ReplyGetWorkerFailureCause());
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 1);

  // Task 2 runs successfully on the second worker; the worker is returned due to the
  // timeout.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1001, local_node_id));
  std::this_thread::sleep_for(
      std::chrono::milliseconds(10));  // Sleep for 10ms, causing the lease to time out.
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(raylet_client->num_workers_returned, 1);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 1);

  // Task 3 runs successfully on the third worker; the worker is returned even though it
  // hasn't timed out.
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1002, local_node_id));
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(raylet_client->num_workers_returned, 2);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 1);
  ASSERT_EQ(raylet_client->num_leases_canceled, 0);
  ASSERT_FALSE(raylet_client->ReplyCancelWorkerLease());

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST_F(NormalTaskSubmitterTest, TestKillExecutingTask) {
  auto submitter =
      CreateNormalTaskSubmitter(std::make_shared<StaticLeaseRequestRateLimiter>(1));
  TaskSpecification task = BuildEmptyTaskSpec();

  submitter.SubmitTask(task);
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1234, local_node_id));

  // Try force kill, exiting the worker
  submitter.CancelTask(task, true, false);
  ASSERT_EQ(worker_client->kill_requests.front().intended_task_id(), task.TaskIdBinary());
  ASSERT_TRUE(worker_client->ReplyPushTask(Status::IOError("workerdying"), true));
  ASSERT_TRUE(raylet_client->ReplyGetWorkerFailureCause());
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_workers_returned_exiting, 0);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 1);
  ASSERT_EQ(task_manager->num_tasks_complete, 0);
  ASSERT_EQ(task_manager->num_tasks_failed, 1);

  task.GetMutableMessage().set_task_id(
      TaskID::ForNormalTask(JobID::Nil(), TaskID::Nil(), 1).Binary());
  submitter.SubmitTask(task);
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1234, local_node_id));

  // Try non-force kill, worker returns normally
  submitter.CancelTask(task, false, false);
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(worker_client->kill_requests.front().intended_task_id(), task.TaskIdBinary());
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(raylet_client->num_workers_returned, 1);
  ASSERT_EQ(raylet_client->num_workers_returned_exiting, 0);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 1);
  ASSERT_EQ(task_manager->num_tasks_complete, 1);
  ASSERT_EQ(task_manager->num_tasks_failed, 1);

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST_F(NormalTaskSubmitterTest, TestKillPendingTask) {
  auto submitter =
      CreateNormalTaskSubmitter(std::make_shared<StaticLeaseRequestRateLimiter>(1));
  TaskSpecification task = BuildEmptyTaskSpec();

  submitter.SubmitTask(task);
  submitter.CancelTask(task, true, false);
  ASSERT_EQ(worker_client->kill_requests.size(), 0);
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_manager->num_tasks_complete, 0);
  ASSERT_EQ(task_manager->num_tasks_failed, 1);
  ASSERT_EQ(task_manager->num_fail_pending_task_calls, 1);
  ASSERT_EQ(raylet_client->num_leases_canceled, 1);
  ASSERT_TRUE(raylet_client->ReplyCancelWorkerLease());

  // Trigger reply to RequestWorkerLease to remove the canceled pending lease request
  ASSERT_TRUE(raylet_client->GrantWorkerLease(
      "localhost", 1000, local_node_id, NodeID::Nil(), true));

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST_F(NormalTaskSubmitterTest, TestKillResolvingTask) {
  auto submitter =
      CreateNormalTaskSubmitter(std::make_shared<StaticLeaseRequestRateLimiter>(1));
  TaskSpecification task = BuildEmptyTaskSpec();
  ObjectID obj1 = ObjectID::FromRandom();
  task.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(obj1.Binary());
  submitter.SubmitTask(task);
  ASSERT_EQ(task_manager->num_inlined_dependencies, 0);
  submitter.CancelTask(task, true, false);
  auto data = GenerateRandomObject();
  store->Put(*data, obj1);
  WaitForObjectIdInMemoryStore(*store, obj1);
  ASSERT_EQ(worker_client->kill_requests.size(), 0);
  ASSERT_EQ(worker_client->callbacks.size(), 0);
  ASSERT_EQ(raylet_client->num_workers_returned, 0);
  ASSERT_EQ(raylet_client->num_workers_disconnected, 0);
  ASSERT_EQ(task_manager->num_tasks_complete, 0);
  ASSERT_EQ(task_manager->num_tasks_failed, 1);

  // Check that there are no entries left in the scheduling_key_entries_ hashmap. These
  // would otherwise cause a memory leak.
  ASSERT_TRUE(submitter.CheckNoSchedulingKeyEntriesPublic());
}

TEST_F(NormalTaskSubmitterTest, TestQueueGeneratorForResubmit) {
  // Executing generator -> Resubmit queued -> execution finishes -> resubmit happens.
  auto submitter =
      CreateNormalTaskSubmitter(std::make_shared<StaticLeaseRequestRateLimiter>(1));
  TaskSpecification task = BuildEmptyTaskSpec();
  submitter.SubmitTask(task);
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1234, local_node_id));
  ASSERT_TRUE(submitter.QueueGeneratorForResubmit(task));
  ASSERT_TRUE(worker_client->ReplyPushTask());
  ASSERT_EQ(task_manager->num_tasks_complete, 0);
  ASSERT_EQ(task_manager->num_tasks_failed, 0);
  ASSERT_EQ(task_manager->num_generator_failed_and_resubmitted, 1);
}

TEST_F(NormalTaskSubmitterTest, TestCancelBeforeAfterQueueGeneratorForResubmit) {
  // Cancel -> failed queue generator for resubmit -> cancel reply -> successful queue for
  // resubmit -> push task reply -> honor the cancel not the queued resubmit.
  auto submitter =
      CreateNormalTaskSubmitter(std::make_shared<StaticLeaseRequestRateLimiter>(1));
  TaskSpecification task = BuildEmptyTaskSpec();
  submitter.SubmitTask(task);
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1234, local_node_id));
  submitter.CancelTask(task, /*force_kill=*/false, /*recursive=*/true);
  ASSERT_FALSE(submitter.QueueGeneratorForResubmit(task));
  worker_client->ReplyCancelTask();
  ASSERT_TRUE(submitter.QueueGeneratorForResubmit(task));
  ASSERT_TRUE(worker_client->ReplyPushTask(Status::OK(),
                                           /*exit=*/false,
                                           /*is_retryable_error=*/false,
                                           /*was_cancelled_before_running=*/true));
  ASSERT_EQ(task_manager->num_tasks_complete, 0);
  ASSERT_EQ(task_manager->num_tasks_failed, 1);
  ASSERT_EQ(task_manager->num_generator_failed_and_resubmitted, 0);

  // Succesful queue generator for resubmit -> cancel -> successful execution -> no
  // resubmit.
  TaskSpecification task2 = BuildEmptyTaskSpec();
  submitter.SubmitTask(task2);
  ASSERT_TRUE(raylet_client->GrantWorkerLease("localhost", 1234, local_node_id));
  ASSERT_TRUE(submitter.QueueGeneratorForResubmit(task2));
  submitter.CancelTask(task2, /*force_kill=*/false, /*recursive=*/true);
  ASSERT_TRUE(worker_client->ReplyPushTask());
  worker_client->ReplyCancelTask(Status::OK(),
                                 /*attempt_succeeded=*/true,
                                 /*requested_task_running=*/false);
  ASSERT_EQ(task_manager->num_tasks_complete, 1);
  ASSERT_EQ(task_manager->num_tasks_failed, 1);
  ASSERT_EQ(task_manager->num_generator_failed_and_resubmitted, 0);
}

TEST(LeaseRequestRateLimiterTest, StaticLeaseRequestRateLimiter) {
  StaticLeaseRequestRateLimiter limiter(10);
  ASSERT_EQ(limiter.GetMaxPendingLeaseRequestsPerSchedulingCategory(), 10);
}

TEST(LeaseRequestRateLimiterTest, ClusterSizeBasedLeaseRequestRateLimiter) {
  rpc::GcsNodeAddressAndLiveness dead_node;
  dead_node.set_state(rpc::GcsNodeInfo::DEAD);
  rpc::GcsNodeAddressAndLiveness alive_node;
  alive_node.set_state(rpc::GcsNodeInfo::ALIVE);
  {
    ClusterSizeBasedLeaseRequestRateLimiter limiter(1);
    ASSERT_EQ(limiter.GetMaxPendingLeaseRequestsPerSchedulingCategory(), 1);
    limiter.OnNodeChanges(alive_node);
    ASSERT_EQ(limiter.GetMaxPendingLeaseRequestsPerSchedulingCategory(), 1);
    limiter.OnNodeChanges(alive_node);
    ASSERT_EQ(limiter.GetMaxPendingLeaseRequestsPerSchedulingCategory(), 2);
    limiter.OnNodeChanges(dead_node);
    ASSERT_EQ(limiter.GetMaxPendingLeaseRequestsPerSchedulingCategory(), 1);
    limiter.OnNodeChanges(dead_node);
    ASSERT_EQ(limiter.GetMaxPendingLeaseRequestsPerSchedulingCategory(), 1);
  }

  {
    ClusterSizeBasedLeaseRequestRateLimiter limiter(0);
    ASSERT_EQ(limiter.GetMaxPendingLeaseRequestsPerSchedulingCategory(), 0);
    limiter.OnNodeChanges(alive_node);
    ASSERT_EQ(limiter.GetMaxPendingLeaseRequestsPerSchedulingCategory(), 1);
    limiter.OnNodeChanges(dead_node);
    ASSERT_EQ(limiter.GetMaxPendingLeaseRequestsPerSchedulingCategory(), 0);
    limiter.OnNodeChanges(dead_node);
    ASSERT_EQ(limiter.GetMaxPendingLeaseRequestsPerSchedulingCategory(), 0);
    limiter.OnNodeChanges(alive_node);
    ASSERT_EQ(limiter.GetMaxPendingLeaseRequestsPerSchedulingCategory(), 1);
  }
}

}  // namespace core
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
