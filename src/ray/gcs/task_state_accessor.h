#ifndef RAY_GCS_TASK_STATE_ACCESSOR_H
#define RAY_GCS_TASK_STATE_ACCESSOR_H

#include "ray/common/id.h"
#include "ray/gcs/call_back.h"
#include "ray/gcs/tables.h"

namespace ray {

namespace gcs {

class GcsClientImpl;

/// \class TaskStateAccessor
/// TaskStateAccessor class encapsulates the implementation details of
/// read or write or subscribe of task's specification(immutable fields which
/// determined at submission time, and mutable fields which determined at runtime).
class TaskStateAccessor {
 public:
  TaskStateAccessor(GcsClientImpl &client_impl);

  ~TaskStateAccessor() {}

  /// Add a task to GCS asynchronously.
  ///
  /// \param driver_id The ID of the job (= driver).
  /// \param task_id The ID of the task that is added to GCS.
  /// \param data Task data that is added to GCS.
  /// \param callback Callback that is called once the data has been written to the GCS.
  /// \return Status
  Status AsyncAdd(const DriverID &driver_id, const TaskID &task_id,
                  std::shared_ptr<TaskTableData> data,
                  const StatusCallback &callback);

  /// Read task from GCS asynchronously.
  ///
  /// \param driver_id The ID of the job (= driver).
  /// \param task_id The ID of the task that is read from GCS.
  /// \param callback Callback that is called after read done.
  /// \return Status
  Status AsyncGet(const DriverID &driver_id, const TaskID &task_id,
                  const OptionalItemCallback<TaskTableData> &callback);

  /// Subscribe to any Add operations to GCS. The caller may choose to
  /// subscribe to all Adds, or to subscribe only to tasks that it requests
  /// notifications for. This may only be called once per update.
  ///
  /// \param driver_id The ID of the job (= driver).
  /// \param client_id The type of update to listen to. If this is nil, then a
  /// message for each Add to the table will be received. Else, only
  /// messages for the given client will be received. In the latter
  /// case, the client may request notifications on specific tasks in GCS
  /// via `RequestNotifications`.
  /// \param subscribe Callback that is called on each received message. If the
  /// callback is called with an empty vector, then there was no data of tasks.
  /// \param done Callback that is called when subscription is complete.
  /// \return Status
  Status AsyncSubscribe(const DriverID &driver_id, const ClientID &client_id,
                        const SubscribeCallback<TaskID, TaskTableData> &subscribe,
                        const StatusCallback &done);

  /// Request notifications about a task in GCS.
  ///
  /// The notifications will be returned via the subscribe callback that was
  /// registered by `AsyncSubscribe`.  An initial notification will be returned for
  /// the current values of the task, if any, and a subsequent notification will
  /// be published for every following add to the task. Before
  /// notifications can be requested, the caller must first call `AsyncSubscribe`
  /// with the same `client_id`.
  ///
  /// \param driver_id The ID of the job (= driver).
  /// \param client_id The client who is requesting notifications. Before
  /// notifications can be requested, a call to `AsyncSubscribe` to GCS
  /// with the same `client_id` must complete successfully.
  /// \param task_id The ID of the task to request notifications for.
  /// notifications can be requested, a call to `AsyncSubscribe`
  /// must complete successfully.
  /// \return Status
  Status RequestNotifications(const DriverID &driver_id, const TaskID &task_id,
                              const ClientID &client_id);

  /// Cancel notifications about a task in GCS.
  ///
  /// \param driver_id The ID of the job (= driver).
  /// \param task_id The ID of the task to request notifications for.
  /// \param client_id The client who originally requested notifications.
  /// \return Status
  Status CancelNotifications(const DriverID &driver_id, const TaskID &task_id,
                             const ClientID &client_id);

  /// Delete tasks from GCS.
  ///
  /// \param driver_id The ID of the job (= driver).
  /// \param task_ids The ids of tasks to delete from GCS.
  /// \return Status
  Status Delete(const DriverID &driver_id, const std::vector<TaskID> &task_ids);

 private:
  GcsClientImpl &client_impl_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_TASK_STATE_ACCESSOR_H
