#ifndef RAY_CORE_WORKER_MOCK_TRANSPORT_H
#define RAY_CORE_WORKER_MOCK_TRANSPORT_H

#include <list>
#include <unordered_set>

#include "ray/core_worker/store_provider/mock_store_provider.h"
#include "ray/core_worker/transport/transport.h"

namespace ray {

class CoreWorkerMockStoreProvider;

class CoreWorkerMockTaskSubmitterReceiver : public CoreWorkerTaskSubmitter,
                                            public CoreWorkerTaskReceiver {
 public:
  /// Submit a task for execution to raylet.
  ///
  /// \param[in] task The task spec to submit.
  /// \return Status.
  virtual Status SubmitTask(const TaskSpec &task) override;

  // Get tasks for execution from raylet.
  virtual Status GetTasks(std::vector<TaskSpec> *tasks) override;

  void OnObjectPut(const ObjectID &object_id);

  void SetMockStoreProvider(
      std::shared_ptr<CoreWorkerMockStoreProvider> mock_store_provider);

 private:
  std::unordered_set<ObjectID> GetUnreadyObjects(const TaskSpec &task);

  std::list<std::shared_ptr<TaskSpec>> ready_tasks_;

  std::unordered_map<
      ObjectID,
      std::unordered_set<std::shared_ptr<std::pair<std::shared_ptr<TaskSpec>, size_t>>>>
      waiting_tasks_;

  std::mutex mutex_;

  std::shared_ptr<CoreWorkerMockStoreProvider> mock_store_provider_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_MOCK_TRANSPORT_H
