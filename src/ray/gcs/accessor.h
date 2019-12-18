#ifndef RAY_GCS_ACCESSOR_H
#define RAY_GCS_ACCESSOR_H

#include "ray/common/id.h"
#include "ray/gcs/callback.h"
#include "ray/protobuf/gcs.pb.h"

namespace ray {

namespace gcs {

/// \class ActorInfoAccessor
/// `ActorInfoAccessor` is a sub-interface of `GcsClient`.
/// This class includes all the methods that are related to accessing
/// actor information in the GCS.
class ActorInfoAccessor {
 public:
  virtual ~ActorInfoAccessor() = default;

  /// Get actor specification from GCS asynchronously.
  ///
  /// \param actor_id The ID of actor to look up in the GCS.
  /// \param callback Callback that will be called after lookup finishes.
  /// \return Status
  virtual Status AsyncGet(const ActorID &actor_id,
                          const OptionalItemCallback<rpc::ActorTableData> &callback) = 0;

  /// Register an actor to GCS asynchronously.
  ///
  /// \param data_ptr The actor that will be registered to the GCS.
  /// \param callback Callback that will be called after actor has been registered
  /// to the GCS.
  /// \return Status
  virtual Status AsyncRegister(const std::shared_ptr<rpc::ActorTableData> &data_ptr,
                               const StatusCallback &callback) = 0;

  /// Update dynamic states of actor in GCS asynchronously.
  ///
  /// \param actor_id ID of the actor to update.
  /// \param data_ptr Data of the actor to update.
  /// \param callback Callback that will be called after update finishes.
  /// \return Status
  /// TODO(micafan) Don't expose the whole `ActorTableData` and only allow
  /// updating dynamic states.
  virtual Status AsyncUpdate(const ActorID &actor_id,
                             const std::shared_ptr<rpc::ActorTableData> &data_ptr,
                             const StatusCallback &callback) = 0;

  /// Subscribe to any register or update operations of actors.
  ///
  /// \param subscribe Callback that will be called each time when an actor is registered
  /// or updated.
  /// \param done Callback that will be called when subscription is complete and we
  /// are ready to receive notification.
  /// \return Status
  virtual Status AsyncSubscribeAll(
      const SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
      const StatusCallback &done) = 0;

  /// Subscribe to any update operations of an actor.
  ///
  /// \param actor_id The ID of actor to be subscribed to.
  /// \param subscribe Callback that will be called each time when the actor is updated.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  virtual Status AsyncSubscribe(
      const ActorID &actor_id,
      const SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
      const StatusCallback &done) = 0;

  /// Cancel subscription to an actor.
  ///
  /// \param actor_id The ID of the actor to be unsubscribed to.
  /// \param done Callback that will be called when unsubscribe is complete.
  /// \return Status
  virtual Status AsyncUnsubscribe(const ActorID &actor_id,
                                  const StatusCallback &done) = 0;

 protected:
  ActorInfoAccessor() = default;
};

/// \class JobInfoAccessor
/// `JobInfoAccessor` is a sub-interface of `GcsClient`.
/// This class includes all the methods that are related to accessing
/// job information in the GCS.
class JobInfoAccessor {
 public:
  virtual ~JobInfoAccessor() = default;

  /// Add a job to GCS asynchronously.
  ///
  /// \param data_ptr The job that will be add to GCS.
  /// \param callback Callback that will be called after job has been added
  /// to GCS.
  /// \return Status
  virtual Status AsyncAdd(const std::shared_ptr<rpc::JobTableData> &data_ptr,
                          const StatusCallback &callback) = 0;

  /// Mark job as finished in GCS asynchronously.
  ///
  /// \param job_id ID of the job that will be make finished to GCS.
  /// \param callback Callback that will be called after update finished.
  /// \return Status
  virtual Status AsyncMarkFinished(const JobID &job_id,
                                   const StatusCallback &callback) = 0;

  /// Subscribe to finished jobs.
  ///
  /// \param subscribe Callback that will be called each time when a job finishes.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  virtual Status AsyncSubscribeToFinishedJobs(
      const SubscribeCallback<JobID, rpc::JobTableData> &subscribe,
      const StatusCallback &done) = 0;

 protected:
  JobInfoAccessor() = default;
};

/// \class TaskInfoAccessor
/// `TaskInfoAccessor` is a sub-interface of `GcsClient`.
/// This class includes all the methods that are related to accessing
/// task information in the GCS.
class TaskInfoAccessor {
 public:
  virtual ~TaskInfoAccessor() {}

  /// Add a task to GCS asynchronously.
  ///
  /// \param data_ptr The task that will be added to GCS.
  /// \param callback Callback that will be called after task has been added
  /// to GCS.
  /// \return Status
  virtual Status AsyncAdd(const std::shared_ptr<rpc::TaskTableData> &data_ptr,
                          const StatusCallback &callback) = 0;

  /// Get task information from GCS asynchronously.
  ///
  /// \param task_id The ID of the task to look up in GCS.
  /// \param callback Callback that is called after lookup finished.
  /// \return Status
  virtual Status AsyncGet(const TaskID &task_id,
                          const OptionalItemCallback<rpc::TaskTableData> &callback) = 0;

  /// Delete tasks from GCS asynchronously.
  ///
  /// \param task_ids The vector of IDs to delete from GCS.
  /// \param callback Callback that is called after delete finished.
  /// \return Status
  // TODO(micafan) Will support callback of batch deletion in the future.
  // Currently this callback will never be called.
  virtual Status AsyncDelete(const std::vector<TaskID> &task_ids,
                             const StatusCallback &callback) = 0;

  /// Subscribe asynchronously to the event that the given task is added in GCS.
  ///
  /// \param task_id The ID of the task to be subscribed to.
  /// \param subscribe Callback that will be called each time when the task is updated.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  virtual Status AsyncSubscribe(
      const TaskID &task_id,
      const SubscribeCallback<TaskID, rpc::TaskTableData> &subscribe,
      const StatusCallback &done) = 0;

  /// Cancel subscription to a task asynchronously.
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

#endif  // RAY_GCS_ACCESSOR_H