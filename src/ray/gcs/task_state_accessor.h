#ifndef RAY_GCS_TASK_STATE_ACCESSOR_H
#define RAY_GCS_TASK_STATE_ACCESSOR_H

#include "ray/common/id.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/subscription_executor.h"
#include "ray/gcs/tables.h"

namespace ray {

namespace gcs {

class RedisGcsClient;

/// \class TaskStateAccessor
/// TaskStateAccessor class encapsulates the implementation details of
/// read or write or subscribe of task's information (immutable fields which
/// determined at submission time, and mutable fields which determined at runtime).
class TaskStateAccessor {
 public:
  explicit TaskStateAccessor(RedisGcsClient &client_impl);

  ~TaskStateAccessor() {}

  /// Register a task to GCS asynchronously.
  ///
  /// \param data_ptr The task that will be registered to GCS.
  /// \param callback Callback that will be called after task has been registered
  /// to GCS.
  /// \return Status
  Status AsyncRegister(const std::shared_ptr<TaskTableData> &data_ptr,
                       const StatusCallback &callback);

  /// Get task information from GCS asynchronously.
  ///
  /// \param task_id The ID of the task to look up in GCS.
  /// \param callback Callback that is called after lookup finishes.
  /// \return Status
  Status AsyncGet(const TaskID &task_id,
                  const OptionalItemCallback<TaskTableData> &callback);

  /// Delete tasks from GCS asynchronously.
  ///
  /// \param task_ids The vector of IDs to delete from GCS.
  /// \param callback Callback that is called after delete finishes.
  /// \return Status
  // TODO(micafan) Will support callback of batch deletion in the future.
  // Currently this callback will never be called.
  Status AsyncDelete(const std::vector<TaskID> &task_ids, const StatusCallback &callback);

  /// Subscribe to any update operations of a task from GCS asynchronously.
  /// This method is for node only (core worker shouldn't use this method).
  ///
  /// \param task_id The ID of the task to be subscribed to.
  /// \param subscribe Callback that will be called each time when the task is updated.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  Status AsyncSubscribe(const TaskID &task_id,
                        const SubscribePairCallback<TaskID, TaskTableData> &subscribe,
                        const StatusCallback &done);

  /// Cancel subscribe to a task asynchronously.
  /// This method is for node only (core worker shouldn't use this method).
  ///
  /// \param task_id The ID of the task to be unsubscribed to.
  /// \param done Callback that will be called when unsubscribe is complete.
  /// \return Status
  Status AsyncUnsubscribe(const TaskID &task_id, const StatusCallback &done);

 private:
  RedisGcsClient &client_impl_;

  typedef SubscriptionExecutor<TaskID, TaskTableData, raylet::TaskTable>
      TaskSubscriptionExecutor;
  TaskSubscriptionExecutor task_sub_executor_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_TASK_STATE_ACCESSOR_H
