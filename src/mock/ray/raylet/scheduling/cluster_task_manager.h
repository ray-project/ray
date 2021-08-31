namespace ray {
namespace raylet {

class MockWork : public Work {
 public:
};

}  // namespace raylet
}  // namespace ray

namespace ray {
namespace raylet {

class MockClusterTaskManager : public ClusterTaskManager {
 public:
  MOCK_METHOD(void, QueueAndScheduleTask, (const RayTask &task, rpc::RequestWorkerLeaseReply *reply, rpc::SendReplyCallback send_reply_callback), (override));
  MOCK_METHOD(void, TasksUnblocked, (const std::vector<TaskID> &ready_ids), (override));
  MOCK_METHOD(void, TaskFinished, (std::shared_ptr<WorkerInterface> worker, RayTask *task), (override));
  MOCK_METHOD(void, ReturnWorkerResources, (std::shared_ptr<WorkerInterface> worker), (override));
  MOCK_METHOD(bool, CancelTask, (const TaskID &task_id, bool runtime_env_setup_failed), (override));
  MOCK_METHOD(void, FillPendingActorInfo, (rpc::GetNodeStatsReply *reply), (const, override));
  MOCK_METHOD(void, FillResourceUsage, (rpc::ResourcesData &data, const std::shared_ptr<SchedulingResources> &last_reported_resources), (override));
  MOCK_METHOD(bool, AnyPendingTasks, (RayTask *exemplar, bool *any_pending, int *num_pending_actor_creation, int *num_pending_tasks), (const, override));
  MOCK_METHOD(void, ReleaseWorkerResources, (std::shared_ptr<WorkerInterface> worker), (override));
  MOCK_METHOD(bool, ReleaseCpuResourcesFromUnblockedWorker, (std::shared_ptr<WorkerInterface> worker), (override));
  MOCK_METHOD(bool, ReturnCpuResourcesToBlockedWorker, (std::shared_ptr<WorkerInterface> worker), (override));
  MOCK_METHOD(void, ScheduleAndDispatchTasks, (), (override));
  MOCK_METHOD(void, RecordMetrics, (), (override));
  MOCK_METHOD(std::string, DebugStr, (), (const, override));
  MOCK_METHOD(ResourceSet, CalcNormalTaskResources, (), (const, override));
};

}  // namespace raylet
}  // namespace ray
