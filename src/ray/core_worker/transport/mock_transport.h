#ifndef RAY_CORE_WORKER_MOCK_TRANSPORT_H
#define RAY_CORE_WORKER_MOCK_TRANSPORT_H

#include <list>
#include <unordered_set>

#include "ray/core_worker/context.h"
#include "ray/core_worker/store_provider/mock_store_provider.h"
#include "ray/core_worker/transport/transport.h"

namespace ray {

class CoreWorkerMockStoreProvider;

class CoreWorkerMockTaskPool {
 public:
  Status SubmitTask(const TaskSpec &task);

  Status GetTasks(std::shared_ptr<WorkerContext> worker_context,
                  std::vector<TaskSpec> *tasks);

  void OnObjectPut(const ObjectID &object_id);

  void SetMockStoreProvider(
      std::shared_ptr<CoreWorkerMockStoreProvider> mock_store_provider);

 private:
  std::unordered_set<ObjectID> GetUnreadyObjects(const TaskSpec &task);

  void PutReadyTask(std::shared_ptr<TaskSpec> task);

  std::unordered_map<ActorID, std::list<std::shared_ptr<TaskSpec>>> actor_ready_tasks_;

  std::list<std::shared_ptr<TaskSpec>> other_ready_tasks_;

  std::unordered_map<
      ObjectID,
      std::unordered_set<std::shared_ptr<std::pair<std::shared_ptr<TaskSpec>, size_t>>>>
      waiting_tasks_;

  std::mutex mutex_;

  std::shared_ptr<CoreWorkerMockStoreProvider> mock_store_provider_;
};

class CoreWorkerMockTaskSubmitter : public CoreWorkerTaskSubmitter {
 public:
  CoreWorkerMockTaskSubmitter(std::shared_ptr<CoreWorkerMockTaskPool> mock_task_pool);

  virtual Status SubmitTask(const TaskSpec &task) override;

 private:
  std::shared_ptr<CoreWorkerMockTaskPool> mock_task_pool_;
};

class CoreWorkerMockTaskReceiver : public CoreWorkerTaskReceiver {
 public:
  CoreWorkerMockTaskReceiver(std::shared_ptr<WorkerContext> worker_context,
                             std::shared_ptr<CoreWorkerMockTaskPool> mock_task_pool);

  virtual Status GetTasks(std::vector<TaskSpec> *tasks) override;

 private:
  std::shared_ptr<WorkerContext> worker_context_;
  std::shared_ptr<CoreWorkerMockTaskPool> mock_task_pool_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_MOCK_TRANSPORT_H
