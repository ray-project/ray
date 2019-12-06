#ifndef RAY_GCS_TASK_INFO_ACCESSOR_H
#define RAY_GCS_TASK_INFO_ACCESSOR_H

#include "ray/common/id.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/subscription_executor.h"
#include "ray/gcs/tables.h"

namespace ray {

namespace gcs {

class RedisGcsClient;

/// \class TaskInfoAccessor
/// `TaskInfoAccessor` is a sub-interface of `GcsClient`.
/// This class includes all the methods that are related to accessing
/// actor information in the GCS.
class TaskInfoAccessor {
 public:
  virtual ~TaskInfoAccessor() {}

  /// Add a task to GCS asynchronously.
  ///
  /// \param data_ptr The task that will be added to GCS.
  /// \param callback Callback that will be called after task has been added
  /// to GCS.
  /// \return Status
  virtual Status AsyncAdd(const std::shared_ptr<TaskTableData> &data_ptr,
                          const StatusCallback &callback) = 0;

  /// Get task information from GCS asynchronously.
  ///
  /// \param task_id The ID of the task to look up in GCS.
  /// \param callback Callback that is called after lookup finishes.
  /// \return Status
  virtual Status AsyncGet(const TaskID &task_id,
                          const OptionalItemCallback<TaskTableData> &callback) = 0;

  /// Delete tasks from GCS asynchronously.
  ///
  /// \param task_ids The vector of IDs to delete from GCS.
  /// \param callback Callback that is called after delete finishes.
  /// \return Status
  // TODO(micafan) Will support callback of batch deletion in the future.
  // Currently this callback will never be called.
  virtual Status AsyncDelete(const std::vector<TaskID> &task_ids,
                             const StatusCallback &callback) = 0;

  /// Subscribe to any update operations of a task from GCS asynchronously.
  /// This method is for node only (core worker shouldn't use this method).
  ///
  /// \param task_id The ID of the task to be subscribed to.
  /// \param subscribe Callback that will be called each time when the task is updated.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  virtual Status AsyncSubscribe(
      const TaskID &task_id,
      const SubscribePairCallback<TaskID, TaskTableData> &subscribe,
      const StatusCallback &done) = 0;

  /// Cancel subscribe to a task asynchronously.
  /// This method is for node only (core worker shouldn't use this method).
  ///
  /// \param task_id The ID of the task to be unsubscribed to.
  /// \param done Callback that will be called when unsubscribe is complete.
  /// \return Status
  virtual Status AsyncUnsubscribe(const TaskID &task_id, const StatusCallback &done) = 0;

 protected:
  TaskInfoAccessor() = default;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_TASK_INFO_ACCESSOR_H
